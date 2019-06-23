use super::{Action, ActionContext};
use actix_web::{error::PayloadError, web::Payload, HttpRequest, HttpResponse};
use bytes::Bytes;
use failure::Error;
use futures::prelude::*;
use futures::sync::mpsc;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;

struct BulkRequest {
    index: String,
    payload: Payload,
}

#[derive(Serialize)]
struct BulkResponse {}

#[derive(Deserialize)]
enum BulkOperation {
    Create(CreateOperation),
    Update(UpdateOperation),
    Delete(DeleteOperation),
}

impl BulkOperation {
    fn document_id(&self) -> &str {
        match self {
            BulkOperation::Create(inner) => &inner.id,
            BulkOperation::Update(inner) => &inner.id,
            BulkOperation::Delete(inner) => &inner.id,
        }
    }
}

#[derive(Deserialize)]
struct CreateOperation {
    id: String,
    document: JsonValue,
}

#[derive(Deserialize)]
struct UpdateOperation {
    id: String,
    document: JsonValue,
}

#[derive(Deserialize)]
struct DeleteOperation {
    id: String,
}

#[derive(Clone, Copy)]
struct BulkAction;

impl Action for BulkAction {
    type Path = String;
    type Payload = Payload;
    type ParseFuture = Result<Self::Request, Error>;
    type Request = BulkRequest;
    type Response = BulkResponse;

    fn method(&self) -> actix_web::http::Method {
        actix_web::http::Method::POST
    }

    fn path(&self) -> String {
        "/{name}/_bulk".to_string()
    }

    fn parse_http(
        &self,
        index: String,
        request: &HttpRequest,
        payload: Self::Payload,
    ) -> Result<BulkRequest, Error> {
        Ok(BulkRequest { index, payload })
    }

    fn to_http_response(&self, response: BulkResponse) -> HttpResponse {
        HttpResponse::NoContent().finish()
    }

    fn execute(
        &self,
        request: BulkRequest,
        ctx: ActionContext,
    ) -> Box<Future<Item = Self::Response, Error = Error>> {
        let index = request.index;
        // let mut by_shard = HashMap::new();
        let f = request
            .payload
            .map_err(|_| failure::err_msg("Failed to parse bulk request"))
            .fold(
                (Vec::new(), Bytes::new()),
                move |(mut operations, mut buf),
                      line|
                      -> Result<(Vec<BulkOperation>, Bytes), Error> {
                    buf.extend(line);

                    let mut split = buf.split(|b| *b == b'\n').peekable();
                    while let Some(l) = split.next() {
                        if split.peek().is_none() {
                            return Ok((operations, Bytes::from(l)));
                        }
                        let operation: BulkOperation =
                            serde_json::from_str(std::str::from_utf8(l).unwrap()).unwrap();
                        operations.push(operation);
                    }
                    Ok((operations, buf))
                },
            )
            .and_then(move |(mut operations, left)| {
                if !left.is_empty() {
                    let operation: BulkOperation =
                        serde_json::from_str(std::str::from_utf8(&left).unwrap()).unwrap();
                    operations.push(operation);
                }

                // let doc_id = operation.document_id();
                // let shard_id = ctx.node_router.get_shard_for_document(index, doc_id.into());
                Ok(())
            })
            .and_then(|op_responses| Ok(BulkResponse {}))
            .map_err(|_| failure::err_msg("shits fucked :shrug:"));
        Box::new(f)
    }
}
