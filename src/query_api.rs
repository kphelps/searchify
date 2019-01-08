use failure::{Error, Fail};
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use tantivy::{
    query::{self, Occur, Query},
    Searcher,
};

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
    TermQuery(TermQuery)
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
    query: HashMap<String, QueryValue>
}

pub trait ToQuery {
    fn to_query(&self, searcher: &Searcher) -> Result<Query, Error>;
}

impl ToQuery for SearchQuery {
    fn to_query(&self, searcher: &Searcher) -> Result<Query, Error> {
        match self {
            SearchQuery::BoolQuery(query) => query.to_query(searcher),
            SearchQuery::TermQuery(query) => query.to_query(searcher),
        }
    }
}

impl ToQuery for BoolQuery {
    fn to_query(&self, searcher: &Searcher) -> Result<Query, Error> {
        let mut queries = vec![];
        queries.extend(
            self.must.iter()
                .map(|query| (Occur::Must, query.to_query(searcher)?)).collect()?
        );
        queries.extend(
            self.must_not.iter()
                .map(|query| (Occur::MustNot, query.to_query(searcher)?)).collect()?
        );
        queries.extend(
            self.should.iter()
                .map(|query| (Occur::Should, query.to_query(searcher)?)).collect()?
        );
        queries.extend(
            self.filter.iter()
                .map(|query| (Occur::Must, query.to_query(searcher)?)).collect()?
        );
    }
}
