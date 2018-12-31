use actix::prelude::*;
use actix::SystemRunner;
use crate::config::Config;
use crate::web::start_web;
use crate::network::NetworkActor;
use crate::key_value_state_machine::KeyValueStateMachine;
use crate::proto::{Peer, RaftGroupMetaState};
use crate::raft::{InitNetwork, RaftClient};
use crate::raft_storage::{
    LOCAL_PREFIX,
    RAFT_GROUP_META_PREFIX,
    RAFT_GROUP_META_PREFIX_KEY,
    RaftStorage,
};
use crate::storage_engine::StorageEngine;
use failure::Error;
use protobuf::parse_from_bytes;
use std::path::Path;

pub fn run(config: &Config) -> Result<(), Error> {
    let _ = build_system(config)?.run();
    Ok(())
}

fn build_system(config: &Config) -> Result<SystemRunner, Error> {
    let sys = System::new("searchify");
    let storage_root = Path::new(&config.storage_root);
    let storage_engine = StorageEngine::new(&storage_root.join("cluster"))?;
    init_node(&config.master_ids, &storage_engine)?;

    let group_states = get_raft_groups(&storage_engine)?;
    let group_state = group_states[0].clone();
    let storage = RaftStorage::new(group_state, storage_engine)?;
    let kv_engine = StorageEngine::new(&storage_root.join("kv"))?;
    let kv_state_machine = KeyValueStateMachine::new(kv_engine)?;
    let raft = RaftClient::new(config.node_id, storage, kv_state_machine)?.start();
    let network = NetworkActor::start(config.node_id, config.port, &config.seeds, raft.clone())?;
    raft.try_send(InitNetwork(network.clone()))?;
    start_web(config, network);

    Ok(sys)
}

fn init_node(master_ids: &[u64], engine: &StorageEngine) -> Result<(), Error> {
    let key = RaftStorage::raft_group_meta_state_key(0);
    let opt: Option<RaftGroupMetaState> = engine.get_message(key.as_ref())?;

    if opt.is_none() {
        let mut state = RaftGroupMetaState::new();
        state.id = 0;
        master_ids.iter().for_each(|n| {
            let mut peer = Peer::new();
            peer.id = *n;
            state.mut_peers().push(peer);
        });
        engine.put_message(key.as_ref(), &state)?;
    }

    Ok(())
}

fn get_raft_groups(engine: &StorageEngine) -> Result<Vec<RaftGroupMetaState>, Error> {
    let end_key: Vec<u8> = vec![LOCAL_PREFIX, RAFT_GROUP_META_PREFIX + 1];
    let mut out = Vec::new();
    engine.scan(RAFT_GROUP_META_PREFIX_KEY, end_key, |_, value| {
        out.push(parse_from_bytes(value)?);
        Ok(true)
    })?;

    Ok(out)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::rpc_client::RpcClient;
    use futures::prelude::*;
    use log::error;
    use rand::{thread_rng, Rng};
    use tempfile;

    fn config() -> Config {
        std::env::set_var("RUST_LOG", "searchify=info,actix_web=info,raft=debug");
        let _ = env_logger::try_init();
        let mut config = Config::default().unwrap();
        let dir = tempfile::tempdir().unwrap();
        config.storage_root = dir.path().to_str().unwrap().to_string();
        config.master_ids = vec![config.node_id];
        config.port = thread_rng().gen_range(10000, 65534);
        config.web.port = config.port + 1;
        config
    }

    fn run_in_system<F, I, E>(system: &mut SystemRunner, f: F) -> Result<I, Error>
    where F: IntoFuture<Item=I, Error=E>,
          E: Into<Error>
    {
        system.block_on(f.into_future())
            .map_err(|e| {
                let err = e.into();
                error!("Error in execution: {:?}", err);
                err
            })
    }

    fn run_node<F, I, E>(config: &Config, f: F) -> Result<I, Error>
    where F: IntoFuture<Item=I, Error=E>,
          E: Into<Error>
    {
        let mut system = build_system(config)?;
        run_in_system(&mut system, f)
    }

    fn run_node_fn<F, N, I, E>(config: &Config, f: F) -> Result<I, Error>
        where F: FnOnce() -> N,
              N: IntoFuture<Item=I, Error=E>,
              E: Into<Error>
    {
        run_node(config, f())
    }

    fn rpc_client(config: &Config) -> RpcClient {
        let address = format!("127.0.0.1:{}", config.port);
        RpcClient::new(config.node_id, &address)
    }

    #[test]
    fn test_set_then_get() {
        let config = config();
        let mut system  = build_system(&config).unwrap();
        let client = rpc_client(&config);
        let key = vec![0];
        let value = vec![0];
        let f = client.set(&key, &value);
        let _ = run_in_system(&mut system, f).unwrap();

        let f = client.get(&key);
        let got = run_in_system(&mut system, f).unwrap();
        assert_eq!(got.key, key);
        assert_eq!(got.value, value);
    }

    #[test]
    fn test_set_restart_get() {
        let mut config = config();
        let client = rpc_client(&config);
        let key = vec![0];
        let value = vec![0];
        let f = client.set(&key, &value);
        let _ = run_node(&config, f).unwrap();
        config.web.port += 1;  // TODO :sigh:

        let f = client.get(&key);
        let got = run_node(&config, f).unwrap();
        assert_eq!(got.key, key);
        assert_eq!(got.value, value);
    }

    #[test]
    fn test_create_index_with_live_nodes() {
        let config = config();
        let mut system = build_system(&config).unwrap();
        let client = rpc_client(&config);
        let _ = run_in_system(&mut system, client.heartbeat());
        let index_name = "hello-world";
        let f = client.create_index(index_name);
        let _ = run_in_system(&mut system, f).unwrap();

        let f = client.show_index(index_name);
        let response = run_in_system(&mut system, f).unwrap();
        assert_eq!(response.shard_count, 2);
        assert_eq!(response.replica_count, 3);

        let f = client.list_shards(config.node_id);
        let shards = run_in_system(&mut system, f).unwrap();
        // No live nodes
        assert_eq!(shards.len(), 2);
    }

    #[test]
    fn test_create_index_without_live_nodes() {
        let config = config();
        let mut system = build_system(&config).unwrap();
        let client = rpc_client(&config);
        let index_name = "hello-world";
        let f = client.create_index(index_name);
        let _ = run_in_system(&mut system, f).unwrap();

        let f = client.show_index(index_name);
        let response = run_in_system(&mut system, f).unwrap();
        assert_eq!(response.shard_count, 2);
        assert_eq!(response.replica_count, 3);

        let f = client.list_shards(config.node_id);
        let shards = run_in_system(&mut system, f).unwrap();
        // No live nodes
        assert_eq!(shards.len(), 0);
    }

    #[test]
    fn test_node_liveness() {
        let config = config();
        let mut system = build_system(&config).unwrap();
        let client = rpc_client(&config);
        let f = client.list_nodes();
        let nodes = run_in_system(&mut system, f).unwrap();
        assert_eq!(nodes.len(), 0);
        let f = client.heartbeat()
            .and_then(|_| client.list_nodes());
        let nodes = run_in_system(&mut system, f).unwrap();
        assert_eq!(nodes.len(), 1);
    }
}
