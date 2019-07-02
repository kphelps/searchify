use crate::cached_persistent_map::CachedPersistentMap;
use crate::clock::{Clock, HybridTimestamp};
use crate::event_emitter::EventEmitter;
use crate::index_tracker::IndexTracker;
use crate::keys::KeySpace;
use crate::proto::*;
use crate::raft::{FutureStateMachineObserver, RaftPropose, RaftStateMachine};
use crate::shard_tracker::ShardTracker;
use crate::storage_engine::StorageEngine;
use failure::Error;
use futures::sync::mpsc;
use futures::sync::oneshot::Sender;
use log::*;

#[derive(Clone, Debug)]
pub enum MetaStateEvent {
    PeerUpdate(PeerState),
    IndexUpdate(String, Option<IndexState>),
    ShardUpdate(u64, Option<ShardState>),
}

pub struct KeyValueStateMachine {
    _engine: StorageEngine,
    clock: Clock,
    nodes: CachedPersistentMap<u64, PeerState>,
    indices: IndexTracker,
    shards: ShardTracker,
    event_emitter: EventEmitter<MetaStateEvent>,
    last_applied: u64,
}

impl RaftStateMachine for KeyValueStateMachine {
    type EntryType = KeyValueEntry;

    fn apply(&mut self, id: u64, entry: KeyValueEntry) -> Result<(), Error> {
        if entry.entry.is_none() {
            return Ok(());
        }

        match entry.entry.unwrap() {
            KeyValueEntry_oneof_entry::create_index(req) => self.create_index(req)?,
            KeyValueEntry_oneof_entry::delete_index(req) => self.delete_index(req)?,
            KeyValueEntry_oneof_entry::heartbeat(heartbeat) => {
                self.liveness_heartbeat(heartbeat)?
            }
        }
        self.last_applied = id;
        Ok(())
    }

    fn peers(&self) -> Result<Vec<u64>, Error> {
        Ok(self.nodes.keys())
    }

    fn last_applied(&self) -> u64 {
        self.last_applied
    }
}

type SimpleObserver<T, F> = FutureStateMachineObserver<T, F>;
type SimplePropose = RaftPropose<KeyValueStateMachine>;

impl KeyValueStateMachine {
    pub fn new(engine: StorageEngine, clock: Clock) -> Result<Self, Error> {
        Ok(Self {
            clock,
            _engine: engine.clone(),
            nodes: CachedPersistentMap::new(&engine, KeySpace::Peer.as_key())?,
            indices: IndexTracker::new(&engine)?,
            shards: ShardTracker::new(&engine)?,
            event_emitter: EventEmitter::new(16),
            last_applied: 0,
        })
    }

    pub fn subscribe(&mut self) -> mpsc::Receiver<MetaStateEvent> {
        self.event_emitter.subscribe()
    }

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
            self.event_emitter
                .emit(MetaStateEvent::ShardUpdate(shard.id, Some(shard.clone())));
        }
        index_state.set_shards(shards.into());
        self.event_emitter.emit(MetaStateEvent::IndexUpdate(
            index_state.name.clone(),
            Some(index_state),
        ));
        Ok(())
    }

    fn delete_index(&mut self, request: DeleteIndexRequest) -> Result<(), Error> {
        let index_state = self.indices.delete(&request.get_name().to_string())?;
        let shard_ids = self.shards.delete_shards_for_index(index_state.id)?;
        shard_ids.into_iter().for_each(|id| {
            self.event_emitter
                .emit(MetaStateEvent::ShardUpdate(id, None));
        });
        self.event_emitter
            .emit(MetaStateEvent::IndexUpdate(index_state.name, None));
        Ok(())
    }

    fn liveness_heartbeat(&mut self, heartbeat: LivenessHeartbeat) -> Result<(), Error> {
        trace!("Heartbeat from: {}", heartbeat.get_peer().id);
        let mut peer_state = PeerState::new();
        peer_state.peer = heartbeat.peer;
        // TODO: check liveness?
        peer_state.liveness = heartbeat.update;
        self.nodes.insert(&peer_state.get_peer().id, &peer_state)?;
        self.event_emitter
            .emit(MetaStateEvent::PeerUpdate(peer_state));
        Ok(())
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
        let now = self.clock.now();
        let mut out = self
            .nodes
            .cache()
            .values()
            .filter(|peer| now < peer.get_liveness().get_expires_at().into())
            .cloned()
            .collect::<Vec<PeerState>>();
        out.sort_by_key(|peer| peer.get_peer().id);
        out
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
                index_state.set_shards(shards.into());
                index_state
            })
            .collect()
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
        expires_at: HybridTimestamp,
        sender: Sender<EmptyResponse>,
    ) -> SimplePropose {
        let mut entry = KeyValueEntry::new();
        let mut heartbeat = LivenessHeartbeat::new();
        heartbeat.set_peer(request.take_peer());
        heartbeat.set_liveness(request.take_liveness());
        let mut update = Liveness::new();
        update.set_expires_at(expires_at.into());
        heartbeat.set_update(update);
        entry.set_heartbeat(heartbeat);
        let observer = SimpleObserver::new(sender, |_: &Self| EmptyResponse::new());
        SimplePropose::new(entry, observer)
    }
}
