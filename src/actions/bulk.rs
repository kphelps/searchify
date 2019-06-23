use actix_web::{HttpRequest, HttpResponse, web::{Path, Payload}};

struct BulkRequest {
    index: String,
    operations: Vec<BulkOperation>,
}

struct BulkResponse {
}

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

struct CreateOperation {
    id: String,
    document: JsonValue,
}

struct UpdateOperation {
    id: String,
    document: JsonValue,
}

struct DeleteOperation {
    id: String,
}

#[derive(Clone, Copy)]
struct BulkAction;

impl Action for BulkAction {
    type Path = String;
    type ParseFuture = Result<Self::Request, Error>;
    type Request = BulkRequest;
    type Response = BulkResponse;

    fn method(&self) -> actix_web::http::Method {
        actix_web::http::Method::POST
    }

    fn path(&self) -> String {
        "/{name}/_bulk".to_string()
    }

    fn parse_http(&self, index: String, request: &HttpRequest, _payload: Payload) -> Result<BulkRequest, Error> {
        Ok(BulkRequest {
            index,
            operations: Vec::new()
        })
    }

    fn to_http_response(&self, response: BulkResponse) -> HttpResponse {
        HttpResponse::NoContent().finish()
    }

    fn execute(&self, request: BulkRequest, ctx: ActionContext) -> Box<Future<Item=Self::Response, Error=Error>> {
        let index = request.index;
        // let mut by_shard = HashMap::new();
        request.operations.into_iter().for_each(|operation| {
            let doc_id = operation.document_id();
            // let shard_id = ctx.node_router.get_shard_for_document(index, doc_id.into());
        });
        Box::new(futures::future::ok(BulkResponse{}))
    }
}
