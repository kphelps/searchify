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

pub fn run(config: &Config) -> Result<(), Error> {
    let _ = build_system(config)?.run();
    Ok(())
}

fn build_system(config: &Config) -> Result<SystemRunner, Error> {
    let sys = System::new("searchify");
    let storage_engine = StorageEngine::new(&config.storage_root)?;
    init_node(&config.master_ids, &storage_engine)?;

    let group_states = get_raft_groups(&storage_engine)?;
    let group_state = group_states[0].clone();
    let storage = RaftStorage::new(group_state, storage_engine)?;
    let kv_state_machine = KeyValueStateMachine::new();
    let raft = RaftClient::new(config.node_id, storage, kv_state_machine)?.start();
    let network = NetworkActor::start(config.node_id, config.port, &config.seeds, raft.clone())?;
    raft.try_send(InitNetwork(network))?;
    start_web(config);

    Ok(sys)
}

fn init_node(master_ids: &[u64], engine: &StorageEngine) -> Result<(), Error> {
    let key = RaftStorage::raft_group_meta_state_key(0);
    let opt = engine.get_message::<RaftGroupMetaState>(&key)?;

    if opt.is_none() {
        let mut state = RaftGroupMetaState::new();
        state.id = 0;
        master_ids.iter().for_each(|n| {
            let mut peer = Peer::new();
            peer.id = *n;
            state.mut_peers().push(peer);
        });
        engine.put_message(&key, &state)?;
    }

    Ok(())
}

fn get_raft_groups(engine: &StorageEngine) -> Result<Vec<RaftGroupMetaState>, Error> {
    let end_key = vec![LOCAL_PREFIX, RAFT_GROUP_META_PREFIX + 1];
    let mut out = Vec::new();
    engine.scan(RAFT_GROUP_META_PREFIX_KEY, &end_key, |_, value| {
        out.push(parse_from_bytes(value)?);
        Ok(true)
    })?;

    Ok(out)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::proto::*;
    use failure::format_err;
    use futures::prelude::*;
    use futures::future;
    use grpcio::{CallOption, EnvBuilder, ChannelBuilder};
    use log::error;
    use rand::{thread_rng, Rng};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile;

    fn config() -> Config {
        let mut config = Config::default().unwrap();
        let dir = tempfile::tempdir().unwrap();
        config.storage_root = dir.path().to_str().unwrap().to_string();
        config.master_ids = vec![config.node_id];
        config.port = thread_rng().gen_range(10000, 65534);
        config.web.port = config.port + 1;
        config
    }

    fn run_node<F, N, I, E>(config: &Config, f: F) -> Result<I, Error>
    where F: FnOnce() -> N,
          N: IntoFuture<Item=I, Error=E>,
          E: Into<Error>
    {
        let mut system = build_system(config)?;
        let fut = f().into_future();
        system.block_on(fut)
            .map_err(|e| {
                let err = e.into();
                error!("Error in execution: {:?}", err);
                err
            })
    }

    fn run_node_fut<F, I, E>(config: &Config, f: F) -> Result<I, Error>
        where F: IntoFuture<Item=I, Error=E>,
              E: Into<Error>
    {
        run_node(config, move || f)
    }

    fn rpc_client(config: &Config) -> InternalClient {
        let env = Arc::new(EnvBuilder::new().build());
        let address = format!("127.0.0.1:{}", config.port);
        let channel = ChannelBuilder::new(env).connect(&address);
        InternalClient::new(channel)
    }

    fn rpc_options() -> CallOption {
        CallOption::default()
            .wait_for_ready(true)
            .timeout(Duration::from_secs(3))
    }

    fn eventually_ok<F, N, I>(
        f: F,
        back_off: Duration,
        max_time: Duration,
    ) -> impl Future<Item=I, Error=Error>
    where F: Fn() -> N + 'static,
          N: IntoFuture<Item=I, Error=Error> + 'static,
          I: 'static
    {
        eventually_ok_impl(f, back_off, max_time, Instant::now())
    }

    fn eventually_ok_impl<F, N, I>(
        f: F,
        back_off: Duration,
        max_time: Duration,
        start_time: Instant,
    ) -> impl Future<Item=I, Error=Error>
        where F: Fn() -> N + 'static,
              N: IntoFuture<Item=I, Error=Error> + 'static,
              I: 'static
    {
        f().into_future().or_else(move |err| -> Box<Future<Item=I, Error=Error>> {
            if Instant::now() - start_time > max_time {
                Box::new(future::err(format_err!("Retry limit exceeded: {}", err)))
            } else {
                Box::new(eventually_ok_impl(f, back_off, max_time, start_time))
            }
        })
    }

    #[test]
    fn test_something() {
        // std::env::set_var("RUST_LOG", "searchify=info,actix_web=info,raft=debug");
        // env_logger::init();
        let config = config();
        let client = rpc_client(&config);
        let mut kv = KeyValue::new();
        kv.key = vec![0];
        kv.value = vec![0];
        let f = move || client.set_async_opt(&kv, rpc_options()).unwrap().from_err();
        let with_retries = eventually_ok(f, Duration::from_millis(1), Duration::from_secs(3));
        assert!(run_node_fut(&config, with_retries).is_ok());
    }
}
