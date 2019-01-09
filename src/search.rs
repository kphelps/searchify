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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct IndexCoordinator {
    config: Config,
    node_router: NodeRouterHandle,
    raft_storage_engine: StorageEngine,
    shards: HashMap<u64, Shard>,
    raft_router: RaftRouter<SearchStateMachine>,
}

impl IndexCoordinator {
    pub fn start(
        config: &Config,
        node_router: NodeRouterHandle,
        raft_storage_engine: StorageEngine,
        raft_group_states: &Vec<RaftGroupMetaState>,
        raft_router: &RaftRouter<SearchStateMachine>,
    ) -> Result<(), Error> {
        let mut coordinator = Self {
            config: config.clone(),
            node_router: node_router,
            raft_storage_engine,
            shards: HashMap::new(),
            raft_router: raft_router.clone(),
        };

        raft_group_states
            .iter()
            .map(|state| {
                if state.group_type == RaftGroupType::RAFT_GROUP_SEARCH {
                    coordinator.initialize_shard_from_disk(state)
                } else {
                    Ok(())
                }
            })
            .collect::<Result<(), Error>>()?;

        Ok(coordinator.run())
    }

    fn run(self) {
        let this = Arc::new(Mutex::new(self));
        let task = tokio::timer::Interval::new(Instant::now(), Duration::from_secs(5))
            .from_err()
            .for_each(move |_| Self::poll_node_info(this.clone()))
            .map_err(|err| error!("Error in node polling loop: {:?}", err));
        tokio::run(task);
    }

    fn poll_node_info(this: Arc<Mutex<Self>>) -> impl Future<Item = (), Error = Error> {
        let f = {
            let inner = this.lock().unwrap();
            inner.node_router.list_shards(inner.config.node_id)
        };
        f.map(move |shards| {
            let mut inner = this.lock().unwrap();
            shards.iter().for_each(|shard| {
                if inner.shards.get(&shard.id).is_none() {
                    if let Err(err) = inner.allocate_shard(&shard) {
                        error!("Failed to allocate shard: {:?}", err);
                    }
                }
            })
        })
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
