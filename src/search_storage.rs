use crate::mappings::{MappedDocument, Mappings};
use crate::proto::{SearchRequest, SearchResponse};
use crate::query_api::{SearchQuery, ToQuery};
use failure::Error;
use log::*;
use std::fs::DirBuilder;
use std::path::Path;
use tantivy::{
    collector::{Count, TopDocs},
    directory::MmapDirectory,
    schema::*,
    Index, IndexWriter,
};

pub struct SearchStorage {
    shard_id: u64,
    index: Index,
    writer: IndexWriter,
    schema: Schema,
}

impl SearchStorage {
    pub fn new<P>(shard_id: u64, path: P, schema: Schema) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        DirBuilder::new().recursive(true).create(&path)?;
        let directory = MmapDirectory::open(path)?;
        let index = Index::open_or_create(directory, schema.clone())?;
        let writer = index.writer(200_000_000)?;

        Ok(SearchStorage {
            shard_id,
            index,
            writer,
            schema,
        })
    }

    pub fn index(&mut self, mapped_document: MappedDocument) -> Result<(), Error> {
        mapped_document
            .to_documents(&self.schema)
            .into_iter()
            .for_each(|doc| {
                info!("[shard-{}] Indexing doc: {:?}", self.shard_id, doc);
                self.writer.add_document(doc);
            });
        self.writer.commit()?;
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
        info!("[shard-{}] Response: {:?}", self.shard_id, response);
        Ok(response)
    }
}
