use crate::actions::{Action, ActionContext};
use crate::node_router::NodeRouterHandle;
use actix_web::*;
use failure::Error;
use futures::prelude::*;

#[derive(Clone)]
pub struct ActionExecutor {
    node_router: NodeRouterHandle,
}

impl ActionExecutor {
    pub fn new(node_router: NodeRouterHandle) -> Self {
        Self { node_router }
    }

    pub fn execute_http<A, P>(
        &self,
        action: A,
        path: web::Path<P>,
        request: &HttpRequest,
    ) -> impl Future<Item = HttpResponse, Error = Error> + 'static
    where
        A: Action<Path = P> + 'static,
        P: FromRequest,
    {
        let executor = self.clone();
        action
            .parse_http(path.into_inner(), request)
            .into_future()
            .and_then(move |action_request| executor.execute(&action, action_request))
            .map(move |action_response| action.to_http_response(action_response))
    }

    pub fn execute<A, Req, Resp>(
        &self,
        action: &A,
        request: Req,
    ) -> impl Future<Item = Resp, Error = Error>
    where
        A: Action<Request = Req, Response = Resp>,
    {
        let context = ActionContext::new(self.node_router.clone());
        action.execute(request, context)
    }
}
