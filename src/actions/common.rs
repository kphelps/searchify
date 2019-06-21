use serde::Serialize;

#[derive(Serialize)]
pub struct ShardResultResponse {
    pub total: u64,
    pub successful: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped: Option<u64>,
    pub failed: u64,
}
