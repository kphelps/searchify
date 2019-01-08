use failure::{Error, Fail};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use tantivy::{
    query::{self, Occur, Query},
    schema::{IndexRecordOption, Schema},
    Searcher, Term,
};

#[derive(Debug, Fail)]
pub enum QueryError {
    #[fail(display = "invalid query: {}", _0)]
    InvalidQueryError(String),

    #[fail(display = "field '{}' not found", _0)]
    FieldNotFound(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum QueryValue {
    String(String),
    Long(i64),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum SearchQuery {
    #[serde(rename = "bool")]
    BoolQuery(BoolQuery),
    #[serde(rename = "term")]
    TermQuery(TermQuery),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct BoolQuery {
    must: Vec<SearchQuery>,
    should: Vec<SearchQuery>,
    must_not: Vec<SearchQuery>,
    filter: Vec<SearchQuery>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct TermQuery {
    #[serde(flatten)]
    query: HashMap<String, QueryValue>,
}

type QueryResult = Result<Box<dyn Query + 'static>, Error>;

pub trait ToQuery {
    fn to_query(&self, schema: &Schema, searcher: &Searcher) -> QueryResult;
}

impl ToQuery for SearchQuery {
    fn to_query(&self, schema: &Schema, searcher: &Searcher) -> QueryResult {
        match self {
            SearchQuery::BoolQuery(query) => query.to_query(schema, searcher),
            SearchQuery::TermQuery(query) => query.to_query(schema, searcher),
        }
    }
}

impl ToQuery for BoolQuery {
    fn to_query(&self, schema: &Schema, searcher: &Searcher) -> QueryResult {
        let mut queries = vec![];
        queries.extend(
            self.must
                .iter()
                .map(|query| Ok((Occur::Must, query.to_query(schema, searcher)?)))
                .collect::<Result<Vec<(Occur, Box<dyn Query>)>, Error>>()?,
        );
        queries.extend(
            self.must_not
                .iter()
                .map(|query| Ok((Occur::MustNot, query.to_query(schema, searcher)?)))
                .collect::<Result<Vec<(Occur, Box<dyn Query>)>, Error>>()?,
        );
        queries.extend(
            self.should
                .iter()
                .map(|query| Ok((Occur::Should, query.to_query(schema, searcher)?)))
                .collect::<Result<Vec<(Occur, Box<dyn Query>)>, Error>>()?,
        );
        queries.extend(
            self.filter
                .iter()
                .map(|query| Ok((Occur::Must, query.to_query(schema, searcher)?)))
                .collect::<Result<Vec<(Occur, Box<dyn Query>)>, Error>>()?,
        );
        let q: Box<query::BooleanQuery> = Box::new(queries.into());
        Ok(q)
    }
}

impl ToQuery for TermQuery {
    fn to_query(&self, schema: &Schema, _: &Searcher) -> QueryResult {
        if let Some((field_name, query_value)) = self.query.iter().take(1).next() {
            let term = query_value.to_term(field_name, schema)?;
            Ok(Box::new(query::TermQuery::new(
                term,
                IndexRecordOption::Basic,
            )))
        } else {
            Err(
                QueryError::InvalidQueryError("Term expects a single field and value".to_string())
                    .into(),
            )
        }
    }
}

impl QueryValue {
    fn to_term(&self, field_name: &str, schema: &Schema) -> Result<Term, Error> {
        let field = schema
            .get_field(field_name)
            .ok_or_else(|| QueryError::FieldNotFound(field_name.to_string()))?;
        let term = match self {
            QueryValue::String(value) => Term::from_field_text(field, value),
            QueryValue::Long(value) => Term::from_field_i64(field, *value),
        };
        Ok(term)
    }
}
