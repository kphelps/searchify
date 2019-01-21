use crate::document::DocumentId;
use crate::mappings::Mappings;
use crate::proto::{
    GetDocumentResponse,
    SearchHit,
    SearchRequest,
    SearchResponse,
};
use crate::query_api::{QueryValue, SearchQuery, ToQuery, TermQuery};
use failure::Error;
use log::*;
use std::fs::DirBuilder;
use std::path::Path;
use tantivy::{
    collector::{Count, TopDocs},
    directory::MmapDirectory,
    schema::*,
    DocAddress,
    Index, IndexWriter,
    Searcher,
};

pub struct SearchStorage {
    shard_id: u64,
    index: Index,
    writer: IndexWriter,
    mappings: Mappings,
    schema: Schema,
}

impl SearchStorage {
    pub fn new<P>(
        shard_id: u64,
        path: P,
        mappings: Mappings,
    ) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let schema = mappings.schema()?;
        DirBuilder::new().recursive(true).create(&path)?;
        let directory = MmapDirectory::open(path)?;
        info!("Schema: {}", serde_json::to_string(&schema)?);
        let index = Index::open_or_create(directory, schema.clone())?;
        let writer = index.writer(200_000_000)?;

        Ok(SearchStorage {
            shard_id,
            index,
            writer,
            mappings,
            schema: schema,
        })
    }

    pub fn index(&mut self, id: DocumentId, source: &serde_json::Value) -> Result<(), Error> {
        let mapped_document = self.mappings.map_to_document(id, source)?;
        mapped_document
            .to_documents(&self.schema)
            .into_iter()
            .for_each(|doc| {
                info!("[shard-{}] Indexing doc: {:?}", self.shard_id, doc);
                self.writer.add_document(doc);
            });
        Ok(())
    }

    pub fn search(&self, request: SearchRequest) -> Result<SearchResponse, Error> {
        let query: SearchQuery = serde_json::from_slice(&request.query)?;
        let limit = if request.limit == 0 {
            10
        } else {
            request.limit
        };
        let docs_collector = TopDocs::with_limit(limit as usize);
        let collector = (Count, docs_collector);
        info!("[shard-{}] Request: {:?}", self.shard_id, query);
        let searcher = self.index.searcher();
        let result = searcher.search(&query.to_query(&self.schema, &searcher)?, &collector)?;
        let mut response = SearchResponse::new();
        response.set_total(result.0 as u64);
        let sources = result.1.into_iter()
            .map(|(_, addr)| self.get_source(&searcher, addr))
            .collect::<Result<Vec<Vec<u8>>, Error>>()?;
        let hits = sources.into_iter().map(|source| {
            let mut hit = SearchHit::new();
            hit.set_source(source);
            hit
        }).collect::<Vec<SearchHit>>();
        response.set_hits(hits.into());
        info!("[shard-{}] Response: {:?}", self.shard_id, response);
        Ok(response)
    }

    pub fn get(&self, document_id: &DocumentId) -> Result<GetDocumentResponse, Error> {
        let query = TermQuery::new("_id", QueryValue::String(document_id.id().to_string()));
        let collector = TopDocs::with_limit(1);
        info!("[shard-{}] Request: {:?}", self.shard_id, query);
        let searcher = self.index.searcher();
        let result = searcher.search(&query.to_query(&self.schema, &searcher)?, &collector)?;
        let found = !result.is_empty();
        let mut response = GetDocumentResponse::new();
        response.set_found(found);
        if found {
            response.set_source(self.get_source(&searcher, result[0].1)?);
        }
        info!("[shard-{}] Response: {:?}", self.shard_id, response);
        Ok(response)
    }

    pub fn refresh(&mut self) -> Result<(), Error> {
        info!("[shard-{}] Refresh", self.shard_id);
        self.writer.commit()?;
        Ok(())
    }

    fn get_source(&self, searcher: &Searcher, addr: DocAddress) -> Result<Vec<u8>, Error> {
        let doc = searcher.doc(addr)?;
        info!("[shard-{}] Hit: {:?}", self.shard_id, doc);
        let field = self.schema.get_field("_source").expect("document source field");
        let serialized = match doc.get_first(field).expect("document source") {
            Value::Bytes(bytes) => bytes.clone(),
            _ => unreachable!(),
        };
        Ok(serialized)
    }
}
