use crate::action::{Action, ActionContext};
use crate::node_router::NodeRouterHandle;
use failure::Error;
use futures::prelude::*;

#[derive(Clone)]
pub struct ActionExecutor {
    node_router: NodeRouterHandle,
}

impl ActionExecutor {

    pub fn new(node_router: NodeRouterHandle) -> Self {
        Self {
            node_router,
        }
    }

    pub fn execute<A, Req, Resp>(&self, action: A, request: Req) -> impl Future<Item=Resp, Error=Error>
    where A: Action<Req, Response=Resp>
    {
        let context = ActionContext::new(self.node_router.clone());
        action.execute(request, context)
    }
}
