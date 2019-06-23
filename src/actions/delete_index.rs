use super::{Action, ActionContext};
use actix_web::{web::Payload, HttpRequest, HttpResponse};
use failure::Error;
use futures::prelude::*;

#[derive(Clone, Copy)]
pub struct DeleteIndexAction;

pub struct DeleteIndexRequest {
    name: String,
}

impl Action for DeleteIndexAction {
    type Path = String;
    type Payload = Payload;
    type ParseFuture = Result<Self::Request, Error>;
    type Request = DeleteIndexRequest;
    type Response = ();

    fn method(&self) -> actix_web::http::Method {
        actix_web::http::Method::DELETE
    }

    fn path(&self) -> String {
        "/{name}".to_string()
    }

    fn parse_http(
        &self,
        index: String,
        _request: &HttpRequest,
        _payload: Self::Payload,
    ) -> Result<DeleteIndexRequest, Error> {
        Ok(DeleteIndexRequest { name: index })
    }

    fn to_http_response(&self, _response: ()) -> HttpResponse {
        HttpResponse::NoContent().finish()
    }

    fn execute(
        &self,
        request: DeleteIndexRequest,
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>> {
        Box::new(ctx.node_router.delete_index(request.name))
    }
}
