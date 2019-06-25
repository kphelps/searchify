use log::*;
use lazy_static::*;
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
    pub static ref GRPC_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "searchify_grpc_msg_duration_seconds",
        "Bucketed histogram of grpc server messages",
        &["type"]
    ).unwrap();

    pub static ref WEB_REQUEST_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "searchify_web_request_duration_seconds",
        "Bucketed histogram of http requests",
        &["type"]
    ).unwrap();
}
