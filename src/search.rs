use crate::config::Config;
use crate::node_router::NodeRouterHandle;
use crate::proto::*;
use crate::raft_router::RaftRouter;
use crate::search_state_machine::SearchStateMachine;
use crate::shard::Shard;
use crate::storage_engine::StorageEngine;
use failure::Error;
use futures::{
    prelude::*,
    future,
    stream,
    sync::mpsc,
};
use log::*;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct IndexCoordinator {
    sender: mpsc::Sender<()>,
}

pub struct Inner {
    config: Config,
    node_router: NodeRouterHandle,
    raft_storage_engine: StorageEngine,
    shards: HashMap<u64, Shard>,
    raft_router: RaftRouter<SearchStateMachine>,
}

#[derive(Debug, Eq, PartialEq)]
enum SearchEvent {
    Event,
    Poll,
    Done,
}

impl IndexCoordinator {
    pub fn start(
        config: &Config,
        node_router: NodeRouterHandle,
        raft_storage_engine: StorageEngine,
        raft_group_states: &Vec<RaftGroupMetaState>,
        raft_router: &RaftRouter<SearchStateMachine>,
    ) -> Result<Self, Error> {
        let mut coordinator = Inner {
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
        let (sender, receiver) = mpsc::channel(256);
        coordinator.run(receiver);
        Ok(Self{ sender })
    }
}

impl Inner {
    fn run(self, receiver: mpsc::Receiver<()>) {
        let this = Arc::new(Mutex::new(self));
        let ticker = tokio::timer::Interval::new(Instant::now(), Duration::from_secs(5))
            .map(|_| SearchEvent::Poll)
            .map_err(|_| ());
        let events = receiver
            .map(|_| SearchEvent::Event)
            .chain(stream::once(Ok(SearchEvent::Done)))
            .select(ticker)
            .take_while(|item| future::ok(*item != SearchEvent::Done));
        let f = events.for_each(move |_| {
            Self::poll_node_info(this.clone())
                .map_err(|err| warn!("Error in node polling loop: {:?}", err))
                .then(|_| Ok(()))
        });
        tokio::spawn(f.then(|_| Ok(info!("Shard polling stopped"))));
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
        info!("loaded");
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
