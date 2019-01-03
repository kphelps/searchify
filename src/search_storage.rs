use failure::Error;
use std::fs::DirBuilder;
use std::path::Path;
use tantivy::{Document, Index, IndexWriter};
use tantivy::directory::MmapDirectory;
use tantivy::schema::*;

pub struct SearchStorage {
    index: Index,
    writer: IndexWriter,
}

impl SearchStorage {

    pub fn new<P>(path: P) -> Result<Self, Error>
        where P: AsRef<Path>
    {
        DirBuilder::new().recursive(true).create(&path)?;
        let directory = MmapDirectory::open(path)?;
        // TODO
        let schema = SchemaBuilder::default().build();
        let index = Index::open_or_create(directory, schema)?;
        let writer = index.writer(200_000_000)?;

        Ok(SearchStorage{
            index,
            writer,
        })
    }

    pub fn index<D>(&mut self, doc: D) -> Result<(), Error>
        where D: Into<Document>
    {
        self.writer.add_document(doc.into());
        self.writer.commit()?;
        Ok(())
    }
}
