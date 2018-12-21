

pub struct RaftGroup {
    pub id: u64,
    pub peers: Vec<RaftPeer>,
}

pub struct RaftPeer {
    pub id: u64,
}
