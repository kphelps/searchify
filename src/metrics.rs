use lazy_static::*;
use log::*;
use prometheus::*;

pub fn dump() -> String {
    let mut buffer = vec![];
    let encoder = TextEncoder::new();
    let metric_familys = gather();
    for mf in metric_familys {
        if let Err(e) = encoder.encode(&[mf], &mut buffer) {
            warn!("prometheus encoding error: {:?}", e);
        }
    }
    String::from_utf8(buffer).unwrap()
}

lazy_static! {
    pub static ref GRPC_CLIENT_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "searchify_grpc_client_duration",
        "Bucketed histogram of grpc client requests",
        &["type"]
    )
    .unwrap();
    pub static ref GRPC_CLIENT_ERROR_COUNTER: IntCounterVec = register_int_counter_vec!(
        "searchify_grpc_client_errors",
        "Count of grpc client errors",
        &["type"]
    )
    .unwrap();
    pub static ref GRPC_SERVER_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "searchify_grpc_server_duration",
        "Bucketed histogram of grpc server messages",
        &["type"]
    )
    .unwrap();
    pub static ref GRPC_SERVER_ERROR_COUNTER: IntCounterVec = register_int_counter_vec!(
        "searchify_grpc_server_errors",
        "Count of grpc server errors",
        &["type"]
    )
    .unwrap();
    pub static ref WEB_REQUEST_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "searchify_web_request_duration_seconds",
        "Bucketed histogram of http requests",
        &["type"]
    )
    .unwrap();
    pub static ref RAFT_TICK_HISTOGRAM: Histogram = register_histogram!(
        "searchify_raft_tick_seconds",
        "Bucketed histogram of raft ticks"
    )
    .unwrap();
}
