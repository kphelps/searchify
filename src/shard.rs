use crate::cached_persistent_cell::CachedPersistentCell;
use crate::keys::{KeySpace, MetaKey};
use crate::mappings::Mappings;
use crate::network::NetworkActor;
use crate::node_router::NodeRouterHandle;
use crate::proto::*;
use crate::raft::RaftClient;
use crate::raft_router::RaftRouter;
use crate::raft_storage::{init_raft_group, RaftStorage};
use crate::search_state_machine::SearchStateMachine;
use crate::storage_engine::StorageEngine;
use actix::prelude::*;
use failure::{format_err, Error};
use std::path::Path;

type RaftStateCell = CachedPersistentCell<RaftGroupMetaState>;
type StateCell = CachedPersistentCell<ShardState>;

pub struct Shard {
    raft_state: RaftStateCell,
    state: StateCell,
    raft: Addr<RaftClient<SearchStateMachine>>,
}

fn new_raft_state_cell(engine: &StorageEngine, shard_id: u64) -> Result<RaftStateCell, Error> {
    let key_raw = RaftStorage::raft_group_meta_state_key(shard_id);
    let key = MetaKey::new().add(key_raw.to_vec());
    RaftStateCell::new(engine, key)
}

fn new_state_cell(engine: &StorageEngine, shard_id: u64) -> Result<StateCell, Error> {
    let key = KeySpace::ShardState.as_key().add(shard_id);
    StateCell::new(engine, key)
}

impl Shard {
    pub fn load(
        id: u64,
        node_id: u64,
        node_router: NodeRouterHandle,
        raft_storage_engine: &StorageEngine,
        network: &Addr<NetworkActor>,
        storage_root: &str,
        raft_router: &RaftRouter<SearchStateMachine>,
    ) -> Result<Self, Error> {
        let raft_state = new_raft_state_cell(raft_storage_engine, id)?;
        let group_state = raft_state
            .get()
            .ok_or(format_err!("Shard does not exist: {}", id))?;

        let state = new_state_cell(raft_storage_engine, id)?;
        let shard_state = state
            .get()
            .ok_or(format_err!("Shard does not exist: {}", id))?;

        let storage_path = Path::new(storage_root).join(id.to_string());
        let mappings = serde_json::from_str(shard_state.get_mappings())?;
        let state_machine = SearchStateMachine::new(id, storage_path, mappings)?;

        let raft_storage = RaftStorage::new(group_state.clone(), raft_storage_engine.clone())?;
        let raft = RaftClient::new(
            node_id,
            raft_storage,
            state_machine,
            node_router,
            network,
            raft_router,
        )?
        .start();

        let shard = Self {
            raft_state,
            state,
            raft,
        };

        Ok(shard)
    }

    pub fn create(
        shard: &ShardState,
        node_id: u64,
        node_router: NodeRouterHandle,
        raft_storage_engine: &StorageEngine,
        network: &Addr<NetworkActor>,
        storage_root: &str,
        raft_router: &RaftRouter<SearchStateMachine>,
    ) -> Result<Self, Error> {
        init_raft_group(
            raft_storage_engine,
            shard.get_id(),
            &shard
                .get_replicas()
                .iter()
                .map(Peer::get_id)
                .collect::<Vec<u64>>(),
            RaftGroupType::RAFT_GROUP_SEARCH,
        )?;
        let mut cell = new_state_cell(raft_storage_engine, shard.get_id())?;
        if cell.get().is_some() {
            return Err(format_err!("Shard already exists: {}", shard.get_id()));
        }
        cell.set(shard)?;

        Self::load(
            shard.id,
            node_id,
            node_router,
            raft_storage_engine,
            network,
            storage_root,
            raft_router,
        )
    }
}
