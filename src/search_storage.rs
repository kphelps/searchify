use crate::document::DocumentId;
use crate::mappings::Mappings;
use crate::proto::{GetDocumentResponse, SearchHit, SearchRequest, SearchResponse};
use crate::query_api::{QueryValue, SearchQuery, TermQuery, ToQuery};
use failure::Error;
use log::*;
use std::fs::DirBuilder;
use std::path::Path;
use tantivy::{
    collector::{Count, TopDocs},
    directory::MmapDirectory,
    schema::*,
    DocAddress, Index, IndexWriter, Searcher,
};

pub struct SearchStorage {
    shard_id: u64,
    index: Index,
    writer: IndexWriter,
    mappings: Mappings,
    schema: Schema,
}

impl SearchStorage {
    pub fn new<P>(shard_id: u64, path: P, mappings: Mappings) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let schema = mappings.schema()?;
        DirBuilder::new().recursive(true).create(&path)?;
        let directory = MmapDirectory::open(path)?;
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

    pub fn index(&mut self, id: &DocumentId, source: &serde_json::Value) -> Result<(), Error> {
        let mapped_document = self.mappings.map_to_document(id, source)?;
        mapped_document
            .to_documents(&self.schema)
            .into_iter()
            .for_each(|doc| {
                self.writer.add_document(doc);
            });
        Ok(())
    }

    pub fn delete(&mut self, id: DocumentId) -> Result<(), Error> {
        let query_value = QueryValue::String(id.into());
        let term = query_value.to_term("_id", &self.schema)?;
        self.writer.delete_term(term);
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
        let searcher = self.index.searcher();
        let raw_query = query.to_query(&self.schema, &searcher)?;
        let result = searcher.search(&raw_query, &collector)?;
        let mut response = SearchResponse::new();
        response.set_total(result.0 as u64);
        let hits = result
            .1
            .into_iter()
            .map(|(score, addr)| {
                self.get_hit(&searcher, addr).map(|mut hit| {
                    hit.set_score(score);
                    hit
                })
            })
            .collect::<Result<Vec<SearchHit>, Error>>()?;
        response.set_hits(hits.into());
        Ok(response)
    }

    pub fn get(&self, document_id: DocumentId) -> Result<GetDocumentResponse, Error> {
        let query = TermQuery::new("_id", QueryValue::String(document_id.into()));
        let collector = TopDocs::with_limit(1);
        let searcher = self.index.searcher();
        let result = searcher.search(&query.to_query(&self.schema, &searcher)?, &collector)?;
        let found = !result.is_empty();
        let mut response = GetDocumentResponse::new();
        response.set_found(found);
        if found {
            response.set_source(self.get_source(&searcher, result[0].1)?);
        }
        Ok(response)
    }

    pub fn refresh(&mut self) -> Result<(), Error> {
        self.writer.commit()?;
        self.index.load_searchers()?;
        Ok(())
    }

    fn get_source(&self, searcher: &Searcher, addr: DocAddress) -> Result<Vec<u8>, Error> {
        Ok(self.get_hit(searcher, addr)?.take_source())
    }

    fn get_hit(&self, searcher: &Searcher, addr: DocAddress) -> Result<SearchHit, Error> {
        let mut hit = SearchHit::new();
        let doc = searcher.doc(addr)?;
        let source_field = self
            .schema
            .get_field("_source")
            .expect("document source field");
        let id_field = self.schema.get_field("_id").expect("document id field");
        let source = match doc.get_first(source_field).expect("document source") {
            Value::Bytes(bytes) => bytes.clone(),
            _ => unreachable!(),
        };
        let id = match doc.get_first(id_field).expect("document id") {
            Value::Str(string) => string.clone(),
            _ => unreachable!(),
        };
        hit.set_source(source);
        hit.set_id(id);
        Ok(hit)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::tempdir;

    fn new_mappings() -> Mappings {
        let data = r#"
          {
            "properties": {
              "hello": { "type": "keyword" }
            }
          }"#;
        serde_json::from_str(data).unwrap()
    }

    fn new_storage(dir: &tempfile::TempDir) -> SearchStorage {
        SearchStorage::new(1, dir.path(), new_mappings()).unwrap()
    }

    #[test]
    fn test_get_missing() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world";
        let result = storage.get(&doc_id.into()).unwrap();
        assert!(!result.get_found());
    }

    // TODO: This asserts we do _not_ do a realtime get. Elasticsearch
    // would pass this test.
    #[test]
    fn test_get_exists_uncommitted() {
        let dir = tempdir().unwrap();
        let mut storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        storage.index(&doc_id, &document).unwrap();
        let result = storage.get(&doc_id).unwrap();
        assert!(!result.get_found());
    }

    #[test]
    fn test_get_exists() {
        let dir = tempdir().unwrap();
        let mut storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        storage.index(&doc_id, &document).unwrap();
        storage.refresh().unwrap();
        let result = storage.get(&doc_id).unwrap();
        assert!(result.get_found());
        assert_eq!(
            result.get_source().to_vec(),
            serde_json::to_vec(&document).unwrap()
        );
    }

    #[test]
    fn test_term_query() {
        let dir = tempdir().unwrap();
        let mut storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        storage.index(&doc_id, &document).unwrap();
        storage.refresh().unwrap();

        let q = SearchQuery::TermQuery(TermQuery::new(
            "hello",
            QueryValue::String("world".to_string()),
        ));
        let mut request = SearchRequest::new();
        request.query = serde_json::to_vec(&q).unwrap();
        let result = storage.search(request).unwrap();
        assert_eq!(result.total, 1);
        let hit = result.get_hits().first().unwrap();
        assert_eq!(hit.get_id(), doc_id.id());
        assert_eq!(
            hit.get_source().to_vec(),
            serde_json::to_vec(&document).unwrap(),
        );
    }
}
