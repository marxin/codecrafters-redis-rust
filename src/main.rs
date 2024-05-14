use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use clap::Parser;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tracing::{debug, error, info, info_span, warn, Instrument};

use crate::server::RedisServer;

mod command;
mod parser;
mod server;

use crate::command::RedisRequest;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Port to listed to
    #[arg(long, default_value_t = 6379)]
    port: u16,
    /// Replica of a master instance
    #[arg(long, num_args(2))]
    replicaof: Option<Vec<String>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));

    let listener = TcpListener::bind(addr).await?;
    info!("listening on: {}", addr);
    let server = Arc::new(Mutex::new(RedisServer::new(
        args.replicaof.map(|arg| arg.join(":")),
    )));

    loop {
        let (socket, addr) = listener.accept().await?;
        let server = server.clone();

        tokio::spawn(
            async move {
                let mut socket = BufReader::new(socket);

                loop {
                    let token_result = parser::parse_token(&mut socket).await.unwrap();
                    debug!("parsed command: {token_result:?}");
                    // TODO
                    let command = RedisRequest::try_from(token_result.0);
                    if command.is_err() {
                        error!("could not parse request: {:?}", command.err());
                        break;
                    }
                    let command = command.unwrap();
                    if matches!(command, RedisRequest::Null) {
                        break;
                    }
                    // TODO
                    let response = server.lock().unwrap().run(command);
                    if response.is_err() {
                        warn!("failed to make a response: {:?}", response.err());
                    } else {
                        debug!("sending reply: {response:?}");
                        // TODO
                        let written = socket
                            .write_all(response.unwrap().serialize().as_bytes())
                            .await;
                        if written.is_err() {
                            warn!("Failed to reply: {:?}", written.err());
                        }
                    }
                }
            }
            .instrument(info_span!("connection", addr = %addr)),
        );
    }
}
