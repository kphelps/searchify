use crate::keys::{self, KeySpace};
use crate::cached_persistent_map::CachedPersistentMap;
use crate::index_tracker::IndexTracker;
use crate::proto::*;
use crate::raft::{
    FutureStateMachineObserver,
    RaftPropose,
    RaftStateMachine,
};
use crate::shard_tracker::ShardTracker;
use crate::storage_engine::StorageEngine;
use failure::{Error, err_msg};
use futures::sync::oneshot::Sender;
use log::info;
use rocksdb::Writable;

pub struct KeyValueStateMachine {
    engine: StorageEngine,
    nodes: CachedPersistentMap<u64, PeerState>,
    indices: IndexTracker,
    shards: ShardTracker,
}

impl KeyValueStateMachine {
    pub fn new(engine: StorageEngine) -> Result<Self, Error> {
        Ok(Self{
            engine: engine.clone(),
            nodes: CachedPersistentMap::new(&engine, KeySpace::Peer.as_key())?,
            indices: IndexTracker::new(&engine)?,
            shards: ShardTracker::new(&engine)?,
        })
    }
}

impl RaftStateMachine for KeyValueStateMachine {
    type EntryType = KeyValueEntry;

    fn apply(&mut self, entry: KeyValueEntry) {
        if let None = entry.entry {
            return;
        }

        let _ = match entry.entry.unwrap() {
            KeyValueEntry_oneof_entry::set(kv) => self.set(kv),
            KeyValueEntry_oneof_entry::create_index(req) => self.create_index(req),
            KeyValueEntry_oneof_entry::delete_index(req) => self.delete_index(req),
            KeyValueEntry_oneof_entry::heartbeat(heartbeat) => self.liveness_heartbeat(heartbeat),
        };
    }
}

type SimpleObserver<T, F> = FutureStateMachineObserver<T, F>;
type SimplePropose<T, F> = RaftPropose<SimpleObserver<T, F>, KeyValueStateMachine>;

impl KeyValueStateMachine {
    fn set(&mut self, key_value: KeyValue) -> Result<(), Error> {
        self.engine.db.put(&key_value.key, &key_value.value).map_err(err_msg)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.engine.db.get(key)
            .map_err(err_msg)
            .map(|opt| opt.map(|db_vec| db_vec.to_vec()))
    }

    fn create_index(&mut self, request: CreateIndexRequest) -> Result<(), Error> {
        let mut index_state = IndexState::new();
        index_state.shard_count = if request.shard_count == 0 { 1 } else { request.shard_count };
        index_state.replica_count = if request.replica_count == 0 { 3 } else { request.replica_count };
        index_state.name = request.name;
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

    fn liveness_heartbeat(&mut self, heartbeat: LivenessHeartbeat)-> Result<(), Error> {
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

        (0..shard_count).map(|i| {
            let mut shard = ShardState::new();
            shard.index_id = index_state.id;
            let mut range = IdRange::new();
            range.set_low(interval * i);
            let high = if i == shard_count - 1 { size } else { (interval * (i + 1)) - 1 };
            range.set_high(high);
            shard.set_range(range);
            let max_replicas = std::cmp::min(index_state.replica_count, nodes.len() as u64);
            // TODO: Need to ahndle not being fully replicated
            (0..max_replicas).for_each(|_| {
                let peer = &nodes[peer_i];
                shard.replicas.push(peer.get_peer().clone());
                peer_i += 1;
                peer_i %= nodes.len();
            });
            shard
        }).collect()
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
        self.indices.all().into_iter().map(|mut index_state| {
            let shards = self.shards.get_shards_for_index(index_state.id);
            info!("Shahrds: {:?}", shards);
            index_state.set_shards(shards.into());
            index_state
        }).collect()
    }

    pub fn shards_for_node(&self, node: u64) -> Result<Vec<ShardState>, Error> {
        Ok(self.shards.get_shards_assigned_to_node(node))
    }

    pub fn propose_set(key_value: KeyValue, sender: Sender<EmptyResponse>)
        -> SimplePropose<EmptyResponse, impl FnOnce(&Self) -> EmptyResponse>
    {
        let mut entry = KeyValueEntry::new();
        entry.set_set(key_value);
        let observer = SimpleObserver::new(
            sender,
            |_: &KeyValueStateMachine| EmptyResponse::new(),
        );
        SimplePropose::new(entry, observer)
    }

    pub fn read_operation<F, R>(sender: Sender<R>, f: F)
        -> SimplePropose<R, impl FnOnce(&Self) -> R>
        where F: FnOnce(&Self) -> R
    {
        let entry = KeyValueEntry::new();
        let observer = SimpleObserver::new(sender, f);
        SimplePropose::new(entry, observer)
    }

    pub fn propose_get(key: Key, sender: Sender<KeyValue>)
        -> SimplePropose<KeyValue, impl FnOnce(&Self) -> KeyValue>
    {
        Self::read_operation(
            sender,
            move |sm| {
                let value = sm.get(&key.key);
                let mut kv = KeyValue::new();
                kv.set_key(key.key);
                // TODO: need to differ between actual errors and not found
                if let Ok(Some(inner)) = value {
                    kv.set_value(inner);
                }
                kv
            },
        )
    }

    pub fn propose_create_index(request: CreateIndexRequest, sender: Sender<CreateIndexResponse>)
        -> SimplePropose<CreateIndexResponse, impl FnOnce(&Self) -> CreateIndexResponse>
    {
        let mut entry = KeyValueEntry::new();
        entry.set_create_index(request);
        let observer = SimpleObserver::new(
            sender,
            move |_: &KeyValueStateMachine| {
                let mut response = CreateIndexResponse::new();
                response.success = true;
                response
            }
        );
        SimplePropose::new(entry, observer)
    }

    pub fn propose_delete_index(request: DeleteIndexRequest, sender: Sender<EmptyResponse>)
        -> SimplePropose<EmptyResponse, impl FnOnce(&Self) -> EmptyResponse>
    {
        let mut entry = KeyValueEntry::new();
        entry.set_delete_index(request);
        let observer = SimpleObserver::new(
            sender,
            |_: &KeyValueStateMachine| EmptyResponse::new(),
        );
        SimplePropose::new(entry, observer)
    }

    pub fn propose_heartbeat(mut request: HeartbeatRequest, sender: Sender<EmptyResponse>)
        -> SimplePropose<EmptyResponse, impl FnOnce(&Self) -> EmptyResponse>
    {
        let mut entry = KeyValueEntry::new();
        let mut heartbeat = LivenessHeartbeat::new();
        heartbeat.set_peer(request.take_peer());
        entry.set_heartbeat(heartbeat);
        let observer = SimpleObserver::new(
            sender,
            |_: &Self| EmptyResponse::new(),
        );
        SimplePropose::new(entry, observer)
    }
}
