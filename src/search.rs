use actix::prelude::*;
use crate::config::Config;
use crate::proto::*;
use crate::node_router::NodeRouterHandle;
use crate::raft_storage::init_raft_group;
use crate::shard::Shard;
use crate::storage_engine::StorageEngine;
use failure::Error;
use log::*;
use futures::prelude::*;
use std::collections::HashMap;
use std::time::Duration;

pub struct IndexCoordinator {
    config: Config,
    node_router: NodeRouterHandle,
    raft_storage_engine: StorageEngine,
    shards: HashMap<u64, Shard>,
}

impl Actor for IndexCoordinator {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(5), Self::poll_node_info);
    }
}

impl IndexCoordinator {
    pub fn new(
        config: &Config,
        node_router: NodeRouterHandle,
        raft_storage_engine: StorageEngine,
        raft_group_states: &Vec<RaftGroupMetaState>,
    ) -> Result<Self, Error> {
        let mut coordinator = Self {
            config: config.clone(),
            node_router: node_router,
            raft_storage_engine,
            shards: HashMap::new(),
        };

        raft_group_states.iter()
            .map(|state| if state.group_type == RaftGroupType::RAFT_GROUP_SEARCH {
                coordinator.initialize_shard_from_disk(state)
            } else {
                Ok(())
            })
            .collect::<Result<(), Error>>()?;

        Ok(coordinator)
    }

    fn poll_node_info(&mut self, ctx: &mut Context<Self>) {
        let f = self.node_router.list_shards(self.config.node_id)
            .into_actor(self)
            .map(|shards, this, ctx| {
                shards.iter().for_each(|shard| {
                    if this.shards.get(&shard.id).is_none() {
                        if let Err(err) = this.allocate_shard(&shard) {
                            error!("Failed to allocate shard: {:?}", err);
                        }
                    }
                })
            })
            .map_err(|_, _, _| ());

        ctx.spawn(f);
    }

    fn initialize_shard_from_disk(&mut self, state: &RaftGroupMetaState) -> Result<(), Error> {
        info!("Loading shard from disk: {:?}", state);
        let shard = Shard::load(
            state.get_id(),
            self.config.node_id,
            self.node_router.clone(),
            &self.raft_storage_engine,
        )?;
        self.shards.insert(state.get_id(), shard);
        Ok(())
    }

    fn allocate_shard(&mut self, shard_state: &ShardState) -> Result<(), Error> {
        info!("Allocating shard: {:?}", shard_state);
        let shard = Shard::create(
            shard_state,
            self.config.node_id,
            self.node_router.clone(),
            &self.raft_storage_engine,
        )?;
        self.shards.insert(shard_state.get_id(), shard);
        Ok(())
    }
}
