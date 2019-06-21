use crate::node_router::NodeRouterHandle;

pub struct ActionContext {
    pub node_router: NodeRouterHandle,
}

impl ActionContext {
    pub fn new(node_router: NodeRouterHandle) -> Self {
        Self {
            node_router,
        }
    }
}
