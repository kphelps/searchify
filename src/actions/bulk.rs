use super::{Action, ActionContext};
use crate::proto;
use crate::rpc_client::{futurize, RpcClient};
use actix_web::{error::PayloadError, web::Payload, HttpRequest, HttpResponse};
use bytes::Bytes;
use failure::Error;
use futures::prelude::*;
use futures::sync::mpsc;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

pub struct BulkRequest {
    index: String,
    payload: Payload,
}

#[derive(Serialize)]
pub struct BulkResponse {}

#[derive(Debug, Deserialize)]
pub enum BulkOperation {
    #[serde(rename = "index")]
    Index(BulkMeta),
    #[serde(rename = "create")]
    Create(BulkMeta),
    #[serde(rename = "update")]
    Update(BulkMeta),
    #[serde(rename = "delete")]
    Delete(BulkMeta),
}

#[derive(Debug)]
pub struct BulkPayload {
    operation: BulkOperation,
    payload: Option<Bytes>,
}

impl BulkOperation {
    fn meta(&self) -> &BulkMeta {
        match self {
            BulkOperation::Index(inner) => &inner,
            BulkOperation::Create(inner) => &inner,
            BulkOperation::Update(inner) => &inner,
            BulkOperation::Delete(inner) => &inner,
        }
    }

    fn index(&self) -> &str {
        &self.meta().index
    }

    fn document_id(&self) -> &str {
        &self.meta().id
    }

    fn proto_op_type(&self) -> proto::BulkOpType {
        match self {
            BulkOperation::Index(inner) => proto::BulkOpType::INDEX,
            BulkOperation::Create(inner) => proto::BulkOpType::CREATE,
            BulkOperation::Update(inner) => proto::BulkOpType::UPDATE,
            BulkOperation::Delete(inner) => proto::BulkOpType::DELETE,
        }
    }

    fn has_payload(&self) -> bool {
        match self {
            BulkOperation::Delete(_) => false,
            _ => true,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct BulkMeta {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_index")]
    index: String,
}

#[derive(Clone, Copy)]
pub struct BulkAction;

struct OperationParser {
    operation: Option<BulkOperation>,
    payloads: Vec<BulkPayload>,
}

impl OperationParser {
    fn new() -> Self {
        Self {
            operation: None,
            payloads: Vec::new(),
        }
    }

    fn push(&mut self, line: &[u8]) -> Result<(), Error> {
        if let Some(operation) = self.operation.take() {
            self.payloads.push(BulkPayload {
                operation,
                payload: Some(Bytes::from(line)),
            })
        } else {
            let string = std::str::from_utf8(&line)?;
            let operation: BulkOperation = serde_json::from_str(string)?;
            if operation.has_payload() {
                self.operation = Some(operation);
            } else {
                self.payloads.push(BulkPayload {
                    operation,
                    payload: None,
                })
            }
        }
        Ok(())
    }
}

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
        _request: &HttpRequest,
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
        let f = self
            .parse_operations(request)
            .into_future()
            .and_then(|operations| execute_operations(operations, ctx));
        Box::new(f)
    }
}

impl BulkAction {
    fn parse_operations(
        &self,
        request: BulkRequest,
    ) -> impl Future<Item = Vec<BulkPayload>, Error = Error> {
        let f = request
            .payload
            .map_err(|_| failure::err_msg("Failed to parse bulk request"))
            .fold(
                (OperationParser::new(), Bytes::new()),
                move |(mut operations, mut buf), line| -> Result<(OperationParser, Bytes), Error> {
                    buf.extend(line);

                    let mut split = buf.split(|b| *b == b'\n').peekable();
                    while let Some(l) = split.next() {
                        if split.peek().is_none() {
                            return Ok((operations, Bytes::from(l)));
                        }
                        operations.push(l)?;
                    }
                    Ok((operations, buf))
                },
            )
            .and_then(move |(mut operations, left)| {
                if !left.is_empty() {
                    operations.push(&left)?;
                }
                Ok(operations.payloads)
            });
        Box::new(f)
    }
}

fn execute_operations(
    operations: Vec<BulkPayload>,
    ctx: ActionContext,
) -> impl Future<Item = BulkResponse, Error = Error> {
    let mut by_index = HashMap::new();
    for op in operations.into_iter() {
        let ops = by_index
            .entry(op.operation.index().to_string())
            .or_insert_with(Vec::new);
        ops.push(op);
    }

    let fs = by_index
        .into_iter()
        .map(move |(index_name, ops)| send_for_index(&ctx, index_name.to_string(), ops))
        .collect::<Vec<Box<Future<Item = BulkResponse, Error = Error>>>>();
    futures::future::join_all(fs).and_then(|_| Ok(BulkResponse {}))
}

fn send_for_index(
    ctx: &ActionContext,
    index_name: String,
    ops: Vec<BulkPayload>,
) -> Box<Future<Item = BulkResponse, Error = Error>> {
    let f = partition_by_shard(ctx, ops, &index_name)
        .into_future()
        .and_then(|by_shard| {
            let fs = by_shard
                .into_iter()
                .map(|(shard_id, batch)| rpc_send(batch.client, shard_id, batch.operations));
            futures::future::join_all(fs)
        })
        .and_then(|responses| Ok(BulkResponse {}));
    Box::new(f)
}

struct PendingShardBulk {
    client: RpcClient,
    operations: Vec<BulkPayload>,
}

fn partition_by_shard(
    ctx: &ActionContext,
    ops: Vec<BulkPayload>,
    index_name: &str,
) -> Result<HashMap<u64, PendingShardBulk>, Error> {
    let mut shards = HashMap::new();
    for op in ops.into_iter() {
        let shard = ctx
            .node_router
            .get_shard_client_for_document(index_name, &op.operation.document_id().into())?;
        let shards = shards
            .entry(shard.shard.id)
            .or_insert_with(|| PendingShardBulk {
                client: shard.client,
                operations: Vec::new(),
            });
        shards.operations.push(op);
    }
    Ok(shards)
}

fn rpc_send(
    client: RpcClient,
    shard_id: u64,
    operations: Vec<BulkPayload>,
) -> impl Future<Item = BulkResponse, Error = Error> {
    let mut request = proto::BulkRequest::new();
    request.set_shard_id(shard_id);
    let proto_ops = operations
        .into_iter()
        .map(|operation| {
            let mut op = proto::BulkOperation::new();
            op.set_op_type(operation.operation.proto_op_type());
            op.set_document_id(operation.operation.document_id().to_string());
            if operation.payload.is_some() {
                op.set_payload(operation.payload.unwrap().to_vec());
            }
            op
        })
        .collect::<Vec<proto::BulkOperation>>();
    request.set_operations(proto_ops.into());
    futurize("bulk", client.client.bulk_async_opt(&request, client.options())).map(|_| BulkResponse {})
}
