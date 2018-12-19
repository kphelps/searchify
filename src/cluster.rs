use actix::prelude::*;
use crate::raft::RaftClient;
use failure::Error;

pub struct Cluster {
    raft: Addr<RaftClient>,
}

impl Cluster {
    pub fn new(raft: Addr<RaftClient>) -> Result<Self, Error> {
        Ok(Self {
            raft: raft,
        })
    }
}
