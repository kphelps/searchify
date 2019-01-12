use crate::cached_persistent_map::CachedPersistentMap;
use crate::index_tracker::IndexTracker;
use crate::keys::KeySpace;
use crate::proto::*;
use crate::raft::{FutureStateMachineObserver, RaftPropose, RaftStateMachine};
use crate::shard_tracker::ShardTracker;
use crate::storage_engine::StorageEngine;
use failure::Error;
use futures::sync::oneshot::Sender;
use log::info;

pub struct KeyValueStateMachine {
    _engine: StorageEngine,
    nodes: CachedPersistentMap<u64, PeerState>,
    indices: IndexTracker,
    shards: ShardTracker,
}

impl KeyValueStateMachine {
    pub fn new(engine: StorageEngine) -> Result<Self, Error> {
        Ok(Self {
            _engine: engine.clone(),
            nodes: CachedPersistentMap::new(&engine, KeySpace::Peer.as_key())?,
            indices: IndexTracker::new(&engine)?,
            shards: ShardTracker::new(&engine)?,
        })
    }
}

impl RaftStateMachine for KeyValueStateMachine {
    type EntryType = KeyValueEntry;

    fn apply(&mut self, entry: KeyValueEntry) -> Result<(), Error> {
        if entry.entry.is_none() {
            return Ok(());
        }

        match entry.entry.unwrap() {
            KeyValueEntry_oneof_entry::create_index(req) => self.create_index(req),
            KeyValueEntry_oneof_entry::delete_index(req) => self.delete_index(req),
            KeyValueEntry_oneof_entry::heartbeat(heartbeat) => self.liveness_heartbeat(heartbeat),
        }
    }
}

type SimpleObserver<T, F> = FutureStateMachineObserver<T, F>;
type SimplePropose = RaftPropose<KeyValueStateMachine>;

impl KeyValueStateMachine {
    fn create_index(&mut self, request: CreateIndexRequest) -> Result<(), Error> {
        let mut index_state = IndexState::new();
        index_state.shard_count = if request.shard_count == 0 {
            1
        } else {
            request.shard_count
        };
        index_state.replica_count = if request.replica_count == 0 {
            3
        } else {
            request.replica_count
        };
        index_state.name = request.name;
        index_state.mappings = request.mappings;
        self.indices.create(&mut index_state)?;
        let mut shards = self.allocate_shards(&index_state);
        for shard in shards.iter_mut() {
            self.shards.create_shard(shard)?;
        }
        Ok(())
    }

    fn delete_index(&mut self, request: DeleteIndexRequest) -> Result<(), Error> {
        let index_state = self.indices.delete(&request.get_name().to_string())?;
        self.shards.delete_shards_for_index(index_state.id)
    }

    fn liveness_heartbeat(&mut self, heartbeat: LivenessHeartbeat) -> Result<(), Error> {
        let mut peer_state = PeerState::new();
        peer_state.peer = heartbeat.peer;
        peer_state.last_heartbeat_tick = heartbeat.tick;
        self.nodes.insert(&peer_state.get_peer().id, &peer_state)
    }

    fn allocate_shards(&self, index_state: &IndexState) -> Vec<ShardState> {
        let shard_count = index_state.shard_count;
        let nodes = self.live_nodes();
        let mut peer_i = 0;
        let size = std::u64::MAX;
        let interval = size / shard_count;

        (0..shard_count)
            .map(|i| {
                let mut shard = ShardState::new();
                shard.index_id = index_state.id;
                let mut range = IdRange::new();
                range.set_low(interval * i);
                let high = if i == shard_count - 1 {
                    size
                } else {
                    (interval * (i + 1)) - 1
                };
                range.set_high(high);
                shard.set_range(range);
                shard.set_mappings(index_state.mappings.clone());
                let max_replicas = std::cmp::min(index_state.replica_count, nodes.len() as u64);
                // TODO: Need to ahndle not being fully replicated
                (0..max_replicas).for_each(|_| {
                    let peer = &nodes[peer_i];
                    shard.replicas.push(peer.get_peer().clone());
                    peer_i += 1;
                    peer_i %= nodes.len();
                });
                shard
            })
            .collect()
    }

    pub fn live_nodes(&self) -> Vec<PeerState> {
        self.nodes.cache().values().cloned().collect()
    }

    pub fn index(&self, name: &str) -> Result<Option<IndexState>, Error> {
        let index = self.indices.find_by_name(name).map(|mut index| {
            let shards = self.shards.get_shards_for_index(index.id);
            index.set_shards(shards.into());
            index
        });
        Ok(index)
    }

    pub fn list_indices(&self) -> Vec<IndexState> {
        self.indices
            .all()
            .into_iter()
            .map(|mut index_state| {
                let shards = self.shards.get_shards_for_index(index_state.id);
                info!("Shahrds: {:?}", shards);
                index_state.set_shards(shards.into());
                index_state
            })
            .collect()
    }

    pub fn shards_for_node(&self, node: u64) -> Result<Vec<ShardState>, Error> {
        Ok(self.shards.get_shards_assigned_to_node(node))
    }

    pub fn read_operation<F, R>(sender: Sender<R>, f: F) -> SimplePropose
    where
        F: FnOnce(&Self) -> R + Send + Sync + 'static,
        R: Send + Sync + 'static,
    {
        let entry = KeyValueEntry::new();
        let observer = SimpleObserver::new(sender, f);
        SimplePropose::new(entry, observer)
    }

    pub fn propose_create_index(
        request: CreateIndexRequest,
        sender: Sender<CreateIndexResponse>,
    ) -> SimplePropose {
        let mut entry = KeyValueEntry::new();
        entry.set_create_index(request);
        let observer = SimpleObserver::new(sender, move |_: &KeyValueStateMachine| {
            let mut response = CreateIndexResponse::new();
            response.success = true;
            response
        });
        SimplePropose::new(entry, observer)
    }

    pub fn propose_delete_index(
        request: DeleteIndexRequest,
        sender: Sender<EmptyResponse>,
    ) -> SimplePropose {
        let mut entry = KeyValueEntry::new();
        entry.set_delete_index(request);
        let observer = SimpleObserver::new(sender, |_: &KeyValueStateMachine| EmptyResponse::new());
        SimplePropose::new(entry, observer)
    }

    pub fn propose_heartbeat(
        mut request: HeartbeatRequest,
        sender: Sender<EmptyResponse>,
    ) -> SimplePropose {
        let mut entry = KeyValueEntry::new();
        let mut heartbeat = LivenessHeartbeat::new();
        heartbeat.set_peer(request.take_peer());
        entry.set_heartbeat(heartbeat);
        let observer = SimpleObserver::new(sender, |_: &Self| EmptyResponse::new());
        SimplePropose::new(entry, observer)
    }
}
