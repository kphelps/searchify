use crate::cluster_state::{ClusterState, ClusterStateEvent};
use crate::config::Config;
use crate::node_router::NodeRouterHandle;
use crate::proto::*;
use crate::raft_router::RaftRouter;
use crate::search_state_machine::SearchStateMachine;
use crate::shard::Shard;
use crate::storage_engine::StorageEngine;
use failure::Error;
use futures::prelude::*;
use log::*;
use std::collections::HashMap;

#[derive(Clone)]
pub struct IndexCoordinator {}

pub struct Inner {
    config: Config,
    node_router: NodeRouterHandle,
    raft_storage_engine: StorageEngine,
    shards: HashMap<u64, Shard>,
    raft_router: RaftRouter<SearchStateMachine>,
}

impl IndexCoordinator {
    pub fn start(
        config: &Config,
        cluster_state: ClusterState,
        node_router: NodeRouterHandle,
        raft_storage_engine: StorageEngine,
        raft_group_states: &[RaftGroupMetaState],
        raft_router: &RaftRouter<SearchStateMachine>,
    ) -> Result<Self, Error> {
        let mut coordinator = Inner {
            config: config.clone(),
            node_router,
            raft_storage_engine,
            shards: HashMap::new(),
            raft_router: raft_router.clone(),
        };

        coordinator.initialize(&cluster_state, raft_group_states)?;
        Self::run_event_loop(coordinator, cluster_state);

        Ok(Self {})
    }

    fn run_event_loop(mut inner: Inner, cluster_state: ClusterState) {
        let task = cluster_state.subscribe().for_each(move |event| {
            match event {
                ClusterStateEvent::ShardAllocated(shard) => {
                    // TODO: this should just update in memory and process in the
                    // background to avoid holding the lock too long. We should
                    // also retry allocating the shard indefinitely since the
                    // cluster allocated it here.
                    if let Err(err) = inner.on_new_shard(shard) {
                        error!("Failed to allocate shard: {}", err);
                    }
                }
            };
            Ok(())
        });
        tokio::spawn(task);
    }
}

impl Inner {
    fn initialize(
        &mut self,
        cluster_state: &ClusterState,
        raft_group_states: &[RaftGroupMetaState],
    ) -> Result<(), Error> {
        raft_group_states
            .iter()
            .map(|state| {
                if state.group_type == RaftGroupType::RAFT_GROUP_SEARCH {
                    self.initialize_shard_from_disk(state)
                } else {
                    Ok(())
                }
            })
            .collect::<Result<(), Error>>()?;
        cluster_state
            .shards_for_node()
            .into_iter()
            .map(|shard| self.on_new_shard(shard))
            .collect::<Result<(), Error>>()?;
        // TODO: delete shards that don't exist in the cluster state
        Ok(())
    }

    fn on_new_shard(&mut self, shard: ShardState) -> Result<(), Error> {
        if self.shards.get(&shard.id).is_some() {
            return Ok(());
        }

        self.allocate_shard(&shard)
    }

    fn initialize_shard_from_disk(&mut self, state: &RaftGroupMetaState) -> Result<(), Error> {
        info!("Loading shard from disk: {:?}", state.get_id());
        let shard = Shard::load(
            state.get_id(),
            self.config.node_id,
            self.node_router.clone(),
            &self.raft_storage_engine,
            &self.config.search_storage_root(),
            &self.raft_router,
        )?;
        self.shards.insert(state.get_id(), shard);
        Ok(())
    }

    fn allocate_shard(&mut self, shard_state: &ShardState) -> Result<(), Error> {
        info!("Allocating shard: {:?}", shard_state.get_id());
        let shard = Shard::create(
            shard_state,
            self.config.node_id,
            self.node_router.clone(),
            &self.raft_storage_engine,
            &self.config.search_storage_root(),
            &self.raft_router,
        )?;
        self.shards.insert(shard_state.get_id(), shard);
        Ok(())
    }
}
