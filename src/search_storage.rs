use crate::document::{DocumentId, ExpectedVersion};
use crate::mappings::Mappings;
use crate::metrics::SEARCH_TIME_HISTOGRAM;
use crate::proto::{GetDocumentResponse, SearchHit, SearchRequest, SearchResponse};
use crate::query_api::{QueryValue, SearchQuery, TermQuery, ToQuery};
use crate::version_tracker::{TrackedVersion, VersionTracker};
use failure::{Error, Fail};
use log::*;
use std::fs::DirBuilder;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tantivy::{
    collector::{Count, TopDocs},
    directory::MmapDirectory,
    schema::*,
    DocAddress, Index, IndexReader, IndexWriter, Searcher,
};

pub struct SearchStorage {
    _index: Index,
    reader: IndexReader,
    writer: SearchStorageWriter,
    schema: Arc<Schema>,
}

#[derive(Clone)]
pub struct SearchStorageWriter {
    inner: Arc<Mutex<WriterInner>>,
}

struct WriterInner {
    reader: IndexReader,
    writer: IndexWriter,
    versions: VersionTracker,
    mappings: Mappings,
    schema: Arc<Schema>,
}

#[derive(Clone)]
pub struct SearchStorageReader {
    inner: IndexReader,
    schema: Arc<Schema>,
}

#[derive(Debug, Fail)]
pub enum SearchStorageError {
    #[fail(display = "document already exists")]
    DocumentAlreadyExists,
}

impl SearchStorage {
    pub fn new<P>(path: P, mappings: Mappings) -> Result<Self, Error>
    where
        P: AsRef<Path>,
    {
        let schema = mappings.schema()?;
        DirBuilder::new().recursive(true).create(&path)?;
        let directory = MmapDirectory::open(path)?;
        let index = Index::open_or_create(directory, schema.clone())?;
        let reader = index.reader()?;
        let index_writer = index.writer(200_000_000)?;
        let p_schema = Arc::new(schema);

        let writer_inner = WriterInner {
            reader: reader.clone(),
            writer: index_writer,
            versions: VersionTracker::new(),
            mappings,
            schema: p_schema.clone(),
        };

        let writer = SearchStorageWriter {
            inner: Arc::new(Mutex::new(writer_inner)),
        };

        let storage = SearchStorage {
            _index: index,
            reader,
            writer,
            schema: p_schema,
        };

        Ok(storage)
    }

    pub fn writer(&self) -> SearchStorageWriter {
        self.writer.clone()
    }

    pub fn reader(&self) -> SearchStorageReader {
        SearchStorageReader {
            inner: self.reader.clone(),
            schema: self.schema.clone(),
        }
    }
}

impl SearchStorageWriter {
    pub fn index(
        &self,
        id: &DocumentId,
        source: &serde_json::Value,
        expected_version: ExpectedVersion,
    ) -> Result<(), Error> {
        with_timer("index", || {
            let mut locked = self.inner.lock().unwrap();
            locked.index(id, source, expected_version)
        })
    }

    pub fn delete(&self, id: DocumentId) -> Result<(), Error> {
        with_timer("delete", || {
            let mut locked = self.inner.lock().unwrap();
            locked.delete(id)
        })
    }

    pub fn refresh(&self) -> Result<(), Error> {
        with_timer("refresh", || {
            let mut locked = self.inner.lock().unwrap();
            locked.refresh()
        })
    }
}

impl WriterInner {
    pub fn index(
        &mut self,
        id: &DocumentId,
        source: &serde_json::Value,
        expected_version: ExpectedVersion,
    ) -> Result<(), Error> {
        let document_exists = self.document_exists(id.clone())?;
        match expected_version {
            ExpectedVersion::Any => (),
            ExpectedVersion::Version(_) => (),
            ExpectedVersion::Deleted => {
                if document_exists {
                    return Err(SearchStorageError::DocumentAlreadyExists.into());
                }
            }
        }
        self.versions.insert(id, 0.into());
        if document_exists {
            self.raw_delete(id.clone())?;
        }
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
        self.versions.delete(&id);
        self.raw_delete(id)
    }

    pub fn refresh(&mut self) -> Result<(), Error> {
        with_timer("pre_commit", || self.versions.pre_commmit());
        with_timer("commit", || self.writer.commit())?;
        with_timer("post_commit", || self.versions.post_commit());
        let _ = with_timer("reader_reload", || self.reader.reload());
        Ok(())
    }

    fn raw_delete(&mut self, id: DocumentId) -> Result<(), Error> {
        let query_value = QueryValue::String(id.into());
        let term = query_value.to_term("_id", &self.schema)?;
        self.writer.delete_term(term);
        Ok(())
    }

    fn document_exists(&self, id: DocumentId) -> Result<bool, Error> {
        match self.versions.get(&id) {
            Some(TrackedVersion::Live(_)) => Ok(true),
            Some(TrackedVersion::Deleted) => Ok(false),
            None => {
                let searcher = self.reader.searcher();
                Ok(resolve_document_id(&self.schema, &searcher, id)?.is_some())
            }
        }
    }
}

impl SearchStorageReader {
    pub fn search(&self, request: SearchRequest) -> Result<SearchResponse, Error> {
        with_timer("search", || {
            let query: SearchQuery = serde_json::from_slice(&request.query)?;
            let limit = if request.limit == 0 {
                10
            } else {
                request.limit
            };
            let docs_collector = TopDocs::with_limit(limit as usize);
            let collector = (Count, docs_collector);
            let searcher = self.inner.searcher();
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
        })
    }

    pub fn get(&self, document_id: DocumentId) -> Result<GetDocumentResponse, Error> {
        with_timer("get", || {
            let searcher = self.inner.searcher();
            let maybe_addr = resolve_document_id(&self.schema, &searcher, document_id)?;
            let mut response = GetDocumentResponse::new();
            response.set_found(maybe_addr.is_some());
            if let Some(addr) = maybe_addr {
                response.set_source(self.get_source(&searcher, addr)?);
            }
            Ok(response)
        })
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

fn with_timer<F, R>(name: &'static str, f: F) -> R
where
    F: FnOnce() -> R,
{
    let timer = SEARCH_TIME_HISTOGRAM
        .with_label_values(&[name])
        .start_timer();
    let out = f();
    timer.observe_duration();
    out
}

fn resolve_document_id(
    schema: &Schema,
    searcher: &Searcher,
    id: DocumentId,
) -> Result<Option<DocAddress>, Error> {
    let query = TermQuery::new("_id", QueryValue::String(id.into()));
    let collector = TopDocs::with_limit(1);
    let result = searcher.search(&query.to_query(schema, &searcher)?, &collector)?;
    Ok(if result.is_empty() {
        None
    } else {
        Some(result[0].1)
    })
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
        SearchStorage::new(dir.path(), new_mappings()).unwrap()
    }

    #[test]
    fn test_get_missing() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world";
        let result = storage.reader().get(doc_id.into()).unwrap();
        assert!(!result.get_found());
    }

    // TODO: This asserts we do _not_ do a realtime get. Elasticsearch
    // would pass this test.
    #[test]
    fn test_get_exists_uncommitted() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        storage
            .writer()
            .index(&doc_id, &document, ExpectedVersion::Any)
            .unwrap();
        let result = storage.reader().get(doc_id).unwrap();
        assert!(!result.get_found());
    }

    #[test]
    fn test_get_exists() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        let writer = storage.writer();
        writer
            .index(&doc_id, &document, ExpectedVersion::Any)
            .unwrap();
        writer.refresh().unwrap();
        let result = storage.reader().get(doc_id).unwrap();
        assert!(result.get_found());
        assert_eq!(
            result.get_source().to_vec(),
            serde_json::to_vec(&document).unwrap()
        );
    }

    #[test]
    fn test_term_query() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        let writer = storage.writer();
        writer
            .index(&doc_id, &document, ExpectedVersion::Any)
            .unwrap();
        writer.refresh().unwrap();

        let q = SearchQuery::TermQuery(TermQuery::new(
            "hello",
            QueryValue::String("world".to_string()),
        ));
        let mut request = SearchRequest::new();
        request.query = serde_json::to_vec(&q).unwrap();
        let result = storage.reader().search(request).unwrap();
        assert_eq!(result.total, 1);
        let hit = result.get_hits().first().unwrap();
        assert_eq!(hit.get_id(), doc_id.id());
        assert_eq!(
            hit.get_source().to_vec(),
            serde_json::to_vec(&document).unwrap(),
        );
    }

    fn search_term(storage: &SearchStorage, key: &str, value: &str) -> SearchResponse {
        let qv = QueryValue::String(value.to_string());
        let q = SearchQuery::TermQuery(TermQuery::new(key, qv));
        let mut request = SearchRequest::new();
        request.query = serde_json::to_vec(&q).unwrap();
        storage.reader().search(request).unwrap()
    }

    #[test]
    fn test_index_multi_doc_same_id_pre_commit() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        let doc_str2 = r#"{"hello": "world2"}"#;
        let document2 = serde_json::from_str(doc_str2).unwrap();
        let writer = storage.writer();
        writer
            .index(&doc_id, &document, ExpectedVersion::Any)
            .unwrap();
        writer
            .index(&doc_id, &document2, ExpectedVersion::Any)
            .unwrap();
        writer.refresh().unwrap();
        assert_eq!(search_term(&storage, "hello", "world").total, 0);
        assert_eq!(search_term(&storage, "hello", "world2").total, 1);
        assert_eq!(search_term(&storage, "_id", doc_id.id()).total, 1);
    }

    #[test]
    fn test_index_multi_doc_same_id_post_commit() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        let doc_str2 = r#"{"hello": "world2"}"#;
        let document2 = serde_json::from_str(doc_str2).unwrap();
        let writer = storage.writer();
        writer
            .index(&doc_id, &document, ExpectedVersion::Any)
            .unwrap();
        writer.refresh().unwrap();
        writer
            .index(&doc_id, &document2, ExpectedVersion::Any)
            .unwrap();
        writer.refresh().unwrap();
        assert_eq!(search_term(&storage, "hello", "world").total, 0);
        assert_eq!(search_term(&storage, "hello", "world2").total, 1);
        assert_eq!(search_term(&storage, "_id", doc_id.id()).total, 1);
    }

    #[test]
    fn test_index_create_post_commit() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        let writer = storage.writer();
        writer
            .index(&doc_id, &document, ExpectedVersion::Deleted)
            .unwrap();
        writer.refresh().unwrap();
        let result = writer.index(&doc_id, &document, ExpectedVersion::Deleted);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_create_pre_commit() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        let writer = storage.writer();
        writer
            .index(&doc_id, &document, ExpectedVersion::Deleted)
            .unwrap();
        let result = writer.index(&doc_id, &document, ExpectedVersion::Deleted);
        assert!(result.is_err());
    }

    #[test]
    fn test_index_create_delete_create_pre_commit() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        let writer = storage.writer();
        writer
            .index(&doc_id, &document, ExpectedVersion::Deleted)
            .unwrap();
        writer.delete(doc_id.clone()).unwrap();
        let result = writer.index(&doc_id, &document, ExpectedVersion::Deleted);
        assert!(result.is_ok());
    }

    #[test]
    fn test_index_create_delete_commit_create() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        let writer = storage.writer();
        writer
            .index(&doc_id, &document, ExpectedVersion::Deleted)
            .unwrap();
        writer.delete(doc_id.clone()).unwrap();
        writer.refresh().unwrap();
        let result = writer.index(&doc_id, &document, ExpectedVersion::Deleted);
        assert!(result.is_ok());
    }

    #[test]
    fn test_index_create_commit_delete_create() {
        let dir = tempdir().unwrap();
        let storage = new_storage(&dir);
        let doc_id = "hello world".into();
        let doc_str = r#"{"hello": "world"}"#;
        let document = serde_json::from_str(doc_str).unwrap();
        let writer = storage.writer();
        writer
            .index(&doc_id, &document, ExpectedVersion::Deleted)
            .unwrap();
        writer.refresh().unwrap();
        writer.delete(doc_id.clone()).unwrap();
        let result = writer.index(&doc_id, &document, ExpectedVersion::Deleted);
        assert!(result.is_ok());
    }
}
