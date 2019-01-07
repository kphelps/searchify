use crate::proto;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum QueryValue {
    String(String),
    Long(i64),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub enum SearchQuery {
    #[serde(rename = "bool")]
    BoolQuery{
        must: Vec<SearchQuery>,
        should: Vec<SearchQuery>,
        must_not: Vec<SearchQuery>,
        filter: Vec<SearchQuery>,
    },
    #[serde(rename = "term")]
    TermQuery{
        #[serde(flatten)]
        query: HashMap<String, QueryValue>
    }
}

impl From<SearchQuery> for proto::SearchQuery {
    fn from(_: SearchQuery) -> proto::SearchQuery {
        proto::SearchQuery::new()
    }
}
