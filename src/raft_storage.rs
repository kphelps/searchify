// TODO: This module should take advantage of the rocksdb abstractions

use crate::proto::{ApplyState, Peer, RaftGroupMetaState, RaftGroupType, RaftLocalState};
use crate::storage_engine::{MessageWriteBatch, StorageEngine};
use byteorder::{BigEndian, ByteOrder};
use failure::Error;
use log::*;
use protobuf::Message;
use raft::{
    eraftpb::{ConfState, Entry, HardState, Snapshot},
    storage::{RaftState, Storage},
    Error as RaftError, Result as RaftResult, StorageError,
};

pub struct RaftStorage {
    pub raft_group: RaftGroupMetaState,
    engine: StorageEngine,
    state: RaftLocalState,
    apply_state: ApplyState,
    last_term: u64,
}

pub const LOCAL_PREFIX: u8 = 0x01;

const LOCAL_RAFT_GROUP_PREFIX: u8 = 0x01;
const LOCAL_RAFT_GROUP_PREFIX_KEY: &[u8] = &[LOCAL_PREFIX, LOCAL_RAFT_GROUP_PREFIX];
const LOCAL_STATE_SUFFIX: u8 = 0x01;
const RAFT_LOG_SUFFIX: u8 = 0x02;
const APPLY_STATE_SUFFIX: u8 = 0x03;

pub const RAFT_GROUP_META_PREFIX: u8 = 0x03;
pub const RAFT_GROUP_META_PREFIX_KEY: &[u8] = &[LOCAL_PREFIX, RAFT_GROUP_META_PREFIX];
const RAFT_GROUP_META_STATE_SUFFIX: u8 = 0x01;

fn new_peer(id: u64) -> Peer {
    let mut peer = Peer::new();
    peer.id = id;
    peer
}

pub fn init_raft_group(
    engine: &StorageEngine,
    id: u64,
    peer_id: u64,
    master_ids: &[u64],
    group_type: RaftGroupType,
) -> Result<(), Error> {
    let key = RaftStorage::raft_group_meta_state_key(id);
    let opt: Option<RaftGroupMetaState> = engine.get_message(key.as_ref())?;

    if opt.is_none() {
        let mut state = RaftGroupMetaState::new();
        state.id = id;
        state.group_type = group_type;
        master_ids.iter().for_each(|n| {
            state.mut_peers().push(new_peer(*n));
        });
        let is_master = master_ids.contains(&peer_id);
        if !is_master {
            state.mut_learners().push(new_peer(peer_id));
        }
        engine.put_message(key.as_ref(), &state)?;
        info!("Initialized raft group '{}'", state.id);
    }

    Ok(())
}

impl RaftStorage {
    pub fn new(raft_group: RaftGroupMetaState, engine: StorageEngine) -> Result<Self, Error> {
        let mut storage = Self {
            raft_group,
            engine,
            state: RaftLocalState::default(),
            apply_state: ApplyState::default(),
            last_term: 0,
        };
        storage.init_local_state()?;
        storage.init_apply_state()?;
        storage.init_last_term()?;
        Ok(storage)
    }

    pub fn raft_group_id(&self) -> u64 {
        self.raft_group.id
    }

    pub fn last_applied_index(&self) -> u64 {
        self.apply_state.applied_index
    }

    pub fn batch(&self) -> MessageWriteBatch {
        self.engine.batch()
    }

    pub fn apply_snapshot(&self, _snapshot: Snapshot) -> Result<(), Error> {
        // TODO
        Ok(())
    }

    pub fn append(
        &mut self,
        entries: &[Entry],
        batch: &mut MessageWriteBatch,
    ) -> Result<(), Error> {
        for entry in entries {
            let key = self.raft_log_key(entry.index);
            batch.put(key.as_ref(), entry)?;
        }
        if !entries.is_empty() {
            let last_entry = entries.last().unwrap();
            self.state.last_index = last_entry.index;
            self.last_term = last_entry.term;
        }
        Ok(())
    }

    pub fn set_hardstate(
        &mut self,
        hardstate: &HardState,
        batch: &mut MessageWriteBatch,
    ) -> Result<(), Error> {
        self.state.set_term(hardstate.term);
        self.state.set_vote(hardstate.vote);
        self.state.set_commit(hardstate.commit);
        self.persist_local_state(batch)
    }

    pub fn update_apply_index(&mut self, last_apply_index: u64) {
        self.apply_state.applied_index = last_apply_index;
    }

    pub fn compact(&mut self, _last_applied: u64) -> Result<(), Error> {
        // TODO
        Ok(())
    }

    pub fn create_snapshot(
        &mut self,
        _last_apply_index: u64,
        _conf_state: Option<ConfState>,
        _data: Vec<u8>,
    ) -> Result<Snapshot, Error> {
        // TODO
        Ok(Snapshot::default())
    }

    pub fn add_node(&mut self, id: u64) -> Result<(), Error> {
        if self
            .raft_group
            .get_peers()
            .iter()
            .find(|p| p.id == id)
            .is_some()
        {
            return Ok(());
        }
        self.raft_group.mut_peers().push(new_peer(id));
        self.persist_raft_group()
    }

    pub fn remove_node(&mut self, id: u64) -> Result<(), Error> {
        let new_peers = self
            .raft_group
            .take_peers()
            .into_iter()
            .filter(|p| p.id != id);
        self.raft_group
            .set_peers(new_peers.collect::<Vec<Peer>>().into());
        self.persist_raft_group()
    }

    pub fn add_learner(&mut self, id: u64) -> Result<(), Error> {
        if self
            .raft_group
            .get_learners()
            .iter()
            .find(|p| p.id == id)
            .is_some()
        {
            return Ok(());
        }
        self.raft_group.mut_learners().push(new_peer(id));
        self.persist_raft_group()
    }

    pub fn membership_change(&mut self, nodes: &[u64], learners: &[u64]) -> Result<(), Error> {
        self.raft_group.set_peers(
            nodes
                .iter()
                .map(|n| new_peer(*n))
                .collect::<Vec<Peer>>()
                .into(),
        );
        self.raft_group.set_learners(
            learners
                .iter()
                .map(|n| new_peer(*n))
                .collect::<Vec<Peer>>()
                .into(),
        );
        self.persist_raft_group()
    }

    fn persist_raft_group(&self) -> Result<(), Error> {
        let key = RaftStorage::raft_group_meta_state_key(self.raft_group_id());
        self.engine.put_message(key.as_ref(), &self.raft_group)
    }

    fn init_local_state(&mut self) -> Result<(), Error> {
        let key = self.local_state_key();
        if let Some(state) = self.engine.get_message(key.as_ref())? {
            self.state = state;
        } else {
            self.state.set_term(5);
            self.state.set_last_index(5);
            self.state.set_commit(5);
        }
        Ok(())
    }

    fn persist_local_state(&self, batch: &mut MessageWriteBatch) -> Result<(), Error> {
        let key = self.local_state_key();
        batch.put(key.as_ref(), &self.state)
    }

    fn init_apply_state(&mut self) -> Result<(), Error> {
        let key = self.apply_state_key();
        if let Some(state) = self.engine.get_message(key.as_ref())? {
            self.apply_state = state;
        } else {
            self.apply_state.set_applied_index(5);
            self.apply_state.set_truncated_index(5);
            self.apply_state.set_truncated_term(5);
        }
        Ok(())
    }

    pub fn persist_apply_state(&self, batch: &mut MessageWriteBatch) -> Result<(), Error> {
        let key = self.apply_state_key();
        batch.put(key.as_ref(), &self.apply_state)
    }

    fn init_last_term(&mut self) -> Result<(), Error> {
        let last_index = self.state.last_index;
        if last_index == 0 || last_index == 5 {
            self.last_term = last_index;
            return Ok(());
        }
        // TODO check apply state
        self.last_term = self.entries(last_index, last_index + 1, None)?[0].term;
        Ok(())
    }

    fn local_state_key(&self) -> [u8; 11] {
        self.raft_group_prefix(LOCAL_STATE_SUFFIX)
    }

    fn apply_state_key(&self) -> [u8; 11] {
        self.raft_group_prefix(APPLY_STATE_SUFFIX)
    }

    fn raft_log_key(&self, id: u64) -> [u8; 19] {
        self.raft_group_key(RAFT_LOG_SUFFIX, id)
    }

    fn raft_group_prefix(&self, suffix: u8) -> [u8; 11] {
        let mut key = [0; 11];
        key[..2].copy_from_slice(LOCAL_RAFT_GROUP_PREFIX_KEY);
        BigEndian::write_u64(&mut key[2..10], self.raft_group.id);
        key[10] = suffix;
        key
    }

    fn raft_group_key(&self, suffix: u8, sub_id: u64) -> [u8; 19] {
        let mut key = [0; 19];
        key[..11].copy_from_slice(&self.raft_group_prefix(suffix));
        BigEndian::write_u64(&mut key[11..19], sub_id);
        key
    }

    fn raft_group_meta_key(group_id: u64, suffix: u8) -> [u8; 11] {
        let mut key = [0; 11];
        key[..2].copy_from_slice(RAFT_GROUP_META_PREFIX_KEY);
        BigEndian::write_u64(&mut key[2..10], group_id);
        key[10] = suffix;
        key
    }

    pub fn raft_group_meta_state_key(group_id: u64) -> [u8; 11] {
        Self::raft_group_meta_key(group_id, RAFT_GROUP_META_STATE_SUFFIX)
    }

    fn hard_state(&self) -> HardState {
        let mut hs = HardState::new();
        hs.set_term(self.state.term);
        hs.set_vote(self.state.vote);
        hs.set_commit(self.state.commit);
        hs
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> Result<Vec<Entry>, Error> {
        let mut out = Vec::with_capacity((high - low) as usize);
        let start_key = self.raft_log_key(low);
        let end_key = self.raft_log_key(high);
        let mut buf_size = 0;
        let max_size = max_size.into();
        self.engine
            .scan(start_key.as_ref(), end_key.as_ref(), |_, value| {
                let mut entry = Entry::new();
                entry.merge_from_bytes(value)?;
                buf_size += value.len() as u64;
                let exceeded_max_size = max_size.map(|n| buf_size > n).unwrap_or(false);
                if !exceeded_max_size || out.is_empty() {
                    out.push(entry);
                }
                Ok(!exceeded_max_size)
            })?;
        Ok(out)
    }
}

impl Storage for RaftStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let mut conf_state = ConfState::default();
        self.raft_group
            .peers
            .iter()
            .for_each(|p| conf_state.mut_nodes().push(p.id));
        self.raft_group
            .learners
            .iter()
            .for_each(|p| conf_state.mut_learners().push(p.id));
        Ok(RaftState::new(self.hard_state(), conf_state))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> RaftResult<Vec<Entry>> {
        self.entries(low, high, max_size)
            .map_err(|_| RaftError::Store(StorageError::Unavailable))
    }

    fn term(&self, index: u64) -> RaftResult<u64> {
        if index == self.apply_state.truncated_index {
            return Ok(self.apply_state.truncated_term);
        }
        if index == self.last_index()? {
            return Ok(self.last_term);
        }
        let result = self.entries(index, index + 1, None);
        if result.is_err() {
            return Err(RaftError::Store(StorageError::Unavailable));
        }

        let term = result.unwrap()[0].term;
        Ok(term)
    }

    fn first_index(&self) -> RaftResult<u64> {
        Ok(self.apply_state.truncated_index + 1)
    }

    fn last_index(&self) -> RaftResult<u64> {
        Ok(self.state.last_index)
    }

    fn snapshot(&self) -> RaftResult<Snapshot> {
        Ok(Snapshot::default())
    }
}
