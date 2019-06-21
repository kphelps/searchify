use actix_web::{HttpRequest, HttpResponse};
use failure::Error;
use futures::prelude::*;

mod action_context;
mod common;
mod delete_document;
mod delete_index;
mod get_document;
mod index_document;
mod list_indices;
mod refresh;

pub use self::common::*;

pub use self::action_context::ActionContext;
pub use self::delete_index::DeleteIndexAction;
pub use self::delete_document::DeleteDocumentAction;
pub use self::get_document::GetDocumentAction;
pub use self::index_document::IndexDocumentAction;
pub use self::list_indices::ListIndicesAction;
pub use self::refresh::RefreshAction;

pub trait Action: Copy {
    type Path;
    type ParseFuture: IntoFuture<Item = Self::Request, Error = Error>;
    type Request;
    type Response;

    fn method(&self) -> actix_web::http::Method;
    fn path(&self) -> String;
    fn parse_http(&self, path: Self::Path, request: &HttpRequest) -> Self::ParseFuture;
    fn to_http_response(&self, response: Self::Response) -> HttpResponse;
    fn execute(&self, request: Self::Request, ctx: ActionContext) -> Box<Future<Item=Self::Response, Error=Error>>;
}
