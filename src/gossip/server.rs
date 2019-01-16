use super::proto::{self, GossipData};
use super::state::GossipState;
use tower_h2::Server;
use failure::Error;
use futures::prelude::*;
use futures::sync::{oneshot, mpsc};
use log::*;
use std::sync::{Arc, RwLock};
use tokio::executor::DefaultExecutor;
use tokio::net::tcp::TcpListener;
use tower_grpc::{Request, Response};
use tokio::net::tcp::{ConnectFuture, TcpStream};
use tower_h2::{client::Connection, RecvBody};
use tower_http::AddOrigin;
use tower_util::MakeService;

pub type GossipRpcClient = proto::client::Gossip<AddOrigin<Connection<TcpStream, DefaultExecutor, RecvBody>>>;

#[derive(Clone)]
pub struct GossipServer {
    state: Arc<RwLock<GossipState>>,
}

pub struct GossipServerHandle {
    sender: oneshot::Sender<()>,
}

impl proto::server::Gossip for GossipServer {
    type ExchangeFuture = Box<Future<Item = Response<GossipData>, Error = tower_grpc::Error> + Send>;

    fn exchange(&mut self, request: Request<GossipData>) -> Self::ExchangeFuture {
        let response = Response::new(self.state.read().unwrap().current.clone());
        Box::new(futures::future::ok(response))
    }
}

impl GossipServer {
    pub fn run(
        address: &str,
        state: Arc<RwLock<GossipState>>,
    ) -> Result<GossipServerHandle, Error> {
        let handler = Self{ state };
        let new_service = proto::server::GossipServer::new(handler.clone());
        let h2_settings = Default::default();
        let mut h2 = Server::new(new_service, h2_settings, DefaultExecutor::current());
        let addr = address.parse()?;
        let bind = TcpListener::bind(&addr).expect("bind");
        let gossip_f = bind.incoming()
            .for_each(move |socket| {
                if let Err(e) = socket.set_nodelay(true) {
                    return Err(e);
                }

                let serve = h2.serve(socket);
                tokio::spawn(serve.map_err(|e| error!("h2 error: {:?}", e)));
                Ok(())
            })
            .map_err(|e| error!("accept error: {}", e));
        let (sender, receiver) = oneshot::channel();
        let serve = gossip_f.select(receiver.map_err(|_| ()));
        tokio::spawn(serve.map(|_| ()).map_err(|_| ()));

        let bootstrap_addresses = handler.state.read().unwrap().bootstrap.clone();
        bootstrap_addresses.iter().for_each(|address| {
            let _ = handler.spawn_client(address);
        });
        Ok(GossipServerHandle { sender })
    }

    fn spawn_client(&self, address: &str) -> Result<(), Error> {
        info!("Gossip?");
        let h2_settings = Default::default();
        let mut make_client = tower_h2::client::Connect::new(Dst, h2_settings, DefaultExecutor::current());
        let (sender, receiver) = mpsc::channel(64);
        self.state.write().unwrap().add_connection(address, sender);
        let client_f = Self::connect_to_peer(address)
            .and_then(move |client| Self::client_loop(client, receiver));
        let state = self.state.clone();
        let address = address.to_string();
        let f = client_f.then(move |result| {
            info!("Gossip done: {:?}", result);
            state.write().unwrap().remove_connection(&address);
            Ok(())
        });

        tokio::spawn(f);
        Ok(())
    }

    fn connect_to_peer(&self, address: &str) -> impl Future<Item = GossipRpcClient, Error = ()> {
        let uri: http::Uri = address.parse()?;
        make_client.make_service(())
            .map(move |conn| {
                let conn = tower_http::add_origin::Builder::new()
                    .uri(uri)
                    .build(conn)
                    .unwrap();

                proto::client::Gossip::new(conn)
            })
            .map_err(|err| debug!("Failed to connect to gossip peer: {:?}", err))
    }

    fn client_loop(mut client: GossipRpcClient, receiver: mpsc::Receiver<()>)
        -> impl Future<Item = (), Error = ()>
    {
        info!("Gossip connected");
        // receiver.map_err(|_| ())
        Ok(()).into_future()
    }
}

struct Dst;

impl tokio_connect::Connect for Dst {
    type Connected = TcpStream;
    type Error = ::std::io::Error;
    type Future = ConnectFuture;

    fn connect(&self) -> Self::Future {
        TcpStream::connect(&([127, 0, 0, 1], 50051).into())
    }
}
