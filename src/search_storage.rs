use crate::mappings::{MappedDocument, Mappings};
use failure::Error;
use log::*;
use std::fs::DirBuilder;
use std::path::Path;
use tantivy::{Index, IndexWriter};
use tantivy::directory::MmapDirectory;
use tantivy::schema::*;

pub struct SearchStorage {
    index: Index,
    writer: IndexWriter,
    schema: Schema,
}

impl SearchStorage {

    pub fn new<P>(
        path: P,
        schema: Schema,
    ) -> Result<Self, Error>
        where P: AsRef<Path>
    {
        DirBuilder::new().recursive(true).create(&path)?;
        let directory = MmapDirectory::open(path)?;
        let index = Index::open_or_create(directory, schema.clone())?;
        let writer = index.writer(200_000_000)?;

        Ok(SearchStorage{
            index,
            writer,
            schema,
        })
    }

    pub fn index(&mut self, mapped_document: MappedDocument) -> Result<(), Error> {
        mapped_document.to_documents(&self.schema).into_iter().for_each(|doc| {
            info!("Indexing doc: {:?}", doc);
            self.writer.add_document(doc);
        });
        self.writer.commit()?;
        Ok(())
    }
}
