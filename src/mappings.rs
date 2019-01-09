use failure::{format_err, Error};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tantivy::{
    schema::{Schema, SchemaBuilder, FAST, STRING},
    Document,
};

// How do we represent raw documents (from the API)
//   - serde_json::Value?
//
// How do we do the mapping?
//   - custom code
//   - serde
//
// How should different field types be represented?
//   - traits?
//   - enum? easier to serde...

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct MappedDocument {
    fields: HashMap<String, MappedField>,
}

impl MappedDocument {
    pub fn to_documents(&self, schema: &Schema) -> Vec<Document> {
        let mut document = Document::new();

        self.fields.iter().for_each(|(name, mapped_field)| {
            let field = schema.get_field(name).unwrap();
            match mapped_field {
                MappedField::Keyword(ref s) => document.add_text(field, s),
                MappedField::Long(n) => document.add_i64(field, *n),
            }
        });

        vec![document]
    }
}

// TODO might be able to make this be zero-copy from input?
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MappedField {
    Keyword(String),
    Long(i64),
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Mappings {
    properties: HashMap<String, MappingField>,
}

impl Mappings {
    pub fn map_to_document(&self, doc: &Value) -> Result<MappedDocument, Error> {
        let mut visitor = DocumentMappingVisitor::new(doc);
        self.accept(&mut visitor)?;

        Ok(MappedDocument {
            fields: visitor.output,
        })
    }

    pub fn accept<V>(&self, visitor: &mut V) -> Result<(), Error>
    where
        V: MappingVisitor,
    {
        self.properties
            .iter()
            .map(|(field_name, field_value)| {
                if visitor.enter_scope(field_name)? {
                    field_value.accept(visitor)?;
                    visitor.leave_scope();
                }
                Ok(())
            })
            .collect::<Result<(), Error>>()
    }

    pub fn schema(&self) -> Result<Schema, Error> {
        let mut visitor = SchemaBuilderVisitor::new();
        self.accept(&mut visitor)?;
        Ok(visitor.builder.build())
    }
}

impl_persistable!(Mappings);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum MappingField {
    Keyword,
    Long,
    Object {
        #[serde(default)]
        properties: HashMap<String, MappingField>,
    },
}

pub trait MappingVisitor {
    fn enter_scope(&mut self, name: &str) -> Result<bool, Error>;
    fn leave_scope(&mut self);

    fn visit_long(&mut self) -> Result<(), Error>;
    fn visit_keyword(&mut self) -> Result<(), Error>;
    fn visit_object(&mut self, properties: &HashMap<String, MappingField>) -> Result<(), Error>;
}

impl MappingField {
    fn accept<V>(&self, visitor: &mut V) -> Result<(), Error>
    where
        V: MappingVisitor,
    {
        match self {
            MappingField::Keyword => visitor.visit_keyword(),
            MappingField::Long => visitor.visit_long(),
            MappingField::Object { properties } => {
                visitor.visit_object(properties)?;
                properties
                    .iter()
                    .map(|(key, value)| {
                        if visitor.enter_scope(key)? {
                            value.accept(visitor)?;
                            visitor.leave_scope();
                        }
                        Ok(())
                    })
                    .collect::<Result<(), Error>>()
            }
        }
    }
}

struct DocumentMappingVisitor {
    value: Value,
    scope: Vec<String>,
    output: HashMap<String, MappedField>,
    value_stack: Vec<Value>,
}

impl DocumentMappingVisitor {
    fn new(value: &Value) -> Self {
        Self {
            value: value.clone(),
            scope: Vec::new(),
            output: HashMap::new(),
            value_stack: Vec::new(),
        }
    }

    fn finalize_field(&mut self, field: MappedField) {
        self.output.insert(self.current_path(), field);
    }

    fn current_value(&self) -> &Value {
        self.value_stack.last().unwrap_or(&self.value)
    }

    fn current_path(&self) -> String {
        self.scope.join(".")
    }
}

impl MappingVisitor for DocumentMappingVisitor {
    fn enter_scope(&mut self, name: &str) -> Result<bool, Error> {
        let properties = self
            .current_value()
            .as_object()
            .ok_or(format_err!("Invalid object: {}", self.value))?;
        let maybe_value = properties.get(name);
        if maybe_value.is_none() {
            return Ok(false);
        }
        // TODO: clone is gonna be a perf issue here
        self.value_stack.push(maybe_value.unwrap().clone());
        self.scope.push(name.to_string());
        Ok(true)
    }

    fn leave_scope(&mut self) {
        self.scope.pop();
        self.value_stack.pop();
    }

    fn visit_keyword(&mut self) -> Result<(), Error> {
        let keyword = self
            .current_value()
            .as_str()
            .map(str::to_string)
            .map(MappedField::Keyword)
            .ok_or(format_err!("Invalid keyword: '{}'", self.value))?;
        self.finalize_field(keyword);
        Ok(())
    }

    fn visit_long(&mut self) -> Result<(), Error> {
        let long = self
            .current_value()
            .as_i64()
            .map(MappedField::Long)
            .ok_or(format_err!("Invalid long: {}", self.value))?;
        self.finalize_field(long);
        Ok(())
    }

    fn visit_object(&mut self, _: &HashMap<String, MappingField>) -> Result<(), Error> {
        Ok(())
    }
}

struct SchemaBuilderVisitor {
    scope: Vec<String>,
    builder: SchemaBuilder,
}

impl SchemaBuilderVisitor {
    fn new() -> Self {
        Self {
            scope: Vec::new(),
            builder: SchemaBuilder::new(),
        }
    }

    fn current_path(&self) -> String {
        self.scope.join(".")
    }
}

impl MappingVisitor for SchemaBuilderVisitor {
    fn enter_scope(&mut self, name: &str) -> Result<bool, Error> {
        self.scope.push(name.to_string());
        Ok(true)
    }

    fn leave_scope(&mut self) {
        self.scope.pop();
    }

    fn visit_keyword(&mut self) -> Result<(), Error> {
        self.builder.add_text_field(&self.current_path(), STRING);
        Ok(())
    }

    fn visit_long(&mut self) -> Result<(), Error> {
        self.builder.add_i64_field(&self.current_path(), FAST);
        Ok(())
    }

    fn visit_object(&mut self, _: &HashMap<String, MappingField>) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tantivy::schema::Type;

    fn new_mappings() -> Mappings {
        let data = r#"
          {
            "properties": {
              "hello": { "type": "keyword" },
              "world": { "type": "long" },
              "object": {
                "type": "object",
                "properties": {
                  "field": { "type": "keyword" }
                }
              }
            }
          }"#;
        serde_json::from_str(data).unwrap()
    }

    #[test]
    fn test_mapping() {
        let mappings = new_mappings();
        assert_eq!(mappings.properties.len(), 3);
        assert_eq!(
            *mappings.properties.get("hello").unwrap(),
            MappingField::Keyword {}
        );
        assert_eq!(
            *mappings.properties.get("world").unwrap(),
            MappingField::Long {}
        );
        let mut obj = HashMap::new();
        let field = MappingField::Keyword {};
        obj.insert("field".to_string(), field);
        assert_eq!(
            *mappings.properties.get("object").unwrap(),
            MappingField::Object { properties: obj },
        );

        let raw_doc_data = r#"
          {
            "hello": "world",
            "world": 1,
            "object": {
              "field": "works"
            }
          }"#;
        let raw_doc: Value = serde_json::from_str(raw_doc_data).unwrap();
        let mapped_doc = mappings.map_to_document(&raw_doc).unwrap();
        let mut fields = HashMap::new();
        fields.insert(
            "hello".to_string(),
            MappedField::Keyword("world".to_string()),
        );
        fields.insert("world".to_string(), MappedField::Long(1));
        fields.insert(
            "object.field".to_string(),
            MappedField::Keyword("works".to_string()),
        );
        let expected = MappedDocument { fields: fields };
        assert_eq!(mapped_doc, expected);
    }

    #[test]
    fn test_build_schema() {
        let mappings = new_mappings();
        let schema = mappings.schema().unwrap();

        let hello = schema.get_field_entry(schema.get_field("hello").unwrap());
        assert!(hello.is_indexed());
        assert_eq!(hello.field_type().value_type(), Type::Str);

        let world = schema.get_field_entry(schema.get_field("world").unwrap());
        assert!(world.is_int_fast());
        assert_eq!(world.field_type().value_type(), Type::I64);

        let field = schema.get_field_entry(schema.get_field("object.field").unwrap());
        assert!(field.is_indexed());
        assert_eq!(field.field_type().value_type(), Type::Str);
    }
}
