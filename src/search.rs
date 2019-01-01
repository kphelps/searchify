use actix::prelude::*;
use crate::config::Config;
use crate::node_router::NodeRouterHandle;
use log::*;
use futures::prelude::*;
use std::time::Duration;

pub struct IndexCoordinator {
    config: Config,
    node_router: NodeRouterHandle,
}

impl Actor for IndexCoordinator {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(5), Self::poll_node_info);
    }
}

impl IndexCoordinator {
    pub fn new(config: &Config, node_router: NodeRouterHandle) -> Self {
        Self{
            config: config.clone(),
            node_router: node_router,
        }
    }

    fn poll_node_info(&mut self, ctx: &mut Context<Self>) {
        let f = self.node_router.list_shards(self.config.node_id)
            .map(|v| info!("Shards: {:?}", v))
            .map_err(|_| ())
            .into_actor(self);

        ctx.spawn(f);
    }
}
