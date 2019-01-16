mod node;
mod server;
mod state;

mod proto {
    use prost_derive::{Enumeration, Message};
    include!(concat!(env!("OUT_DIR"), "/gossip.rs"));

    impl GossipData {
        pub fn new(node_id: u64) -> Self {
            Self {
                node_id,
                sequence_id: 0,
                peer_addresses: std::collections::HashMap::new(),
            }
        }
    }
}

pub use self::node::GossipNode;
