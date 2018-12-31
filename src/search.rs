use actix::prelude::*;
use crate::network::NetworkActor;
use futures::prelude::*;
use std::time::Duration;

struct IndexCoordinator {
    config: Config,
    network: Addr<NetworkActor>,
    cluster: ClusterProxy,
}

impl Actor for IndexCoordinator {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(5), Self::poll_node_info)
    }
}

impl IndexCoordinator {
    pub fn new(network: Addr<NetworkActor>) -> Self {
        Self{
            network,
        }
    }

    pub fn poll_node_info(&mut self) {
        self.cluster.node_info(self.config.node_id)
    }
}
