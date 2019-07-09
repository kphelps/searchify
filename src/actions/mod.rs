use actix_web::{HttpRequest, HttpResponse};
use failure::Error;
use futures::prelude::*;

mod action_context;
mod bulk;
mod cluster_health;
mod common;
mod create_index;
mod delete_document;
mod delete_index;
mod get_document;
mod get_index;
mod index_document;
mod index_exists;
mod list_indices;
mod refresh;
mod search;

pub use self::common::*;

pub use self::action_context::ActionContext;
pub use self::bulk::*;
pub use self::cluster_health::*;
pub use self::create_index::*;
pub use self::delete_document::*;
pub use self::delete_index::*;
pub use self::get_document::*;
pub use self::get_index::*;
pub use self::index_document::*;
pub use self::index_exists::*;
pub use self::list_indices::*;
pub use self::refresh::*;
pub use self::search::*;

pub trait Action: Copy {
    type Path;
    type Payload;
    type ParseFuture: IntoFuture<Item = Self::Request, Error = Error>;
    type Request;
    type Response;

    fn method(&self) -> actix_web::http::Method;
    fn path(&self) -> String;
    fn parse_http(
        &self,
        path: Self::Path,
        request: &HttpRequest,
        _payload: Self::Payload,
    ) -> Self::ParseFuture;
    fn to_http_response(&self, response: Self::Response) -> HttpResponse;
    fn execute(
        &self,
        request: Self::Request,
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>>;
}
