use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use clap::Parser;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::parser::RedisValue;
use crate::server::RedisServer;

mod parser;
mod server;

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
    let args = Args::parse();
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));

    let listener = TcpListener::bind(addr).await?;
    println!("Listening on: {}", addr);
    let server = Arc::new(Mutex::new(RedisServer::new(
        args.replicaof.map(|arg| arg.join(":")),
    )));

    loop {
        let (socket, _) = listener.accept().await?;
        let server = server.clone();

        tokio::spawn(async move {
            let mut socket = std::pin::pin!(BufReader::new(socket));

            loop {
                let token_result = parser::parse_token(&mut socket).await;
                println!("parsed query: {token_result:?}");
                if matches!(token_result, Ok(RedisValue::None)) {
                    break;
                }
                // TODO
                let response = server.lock().unwrap().run(token_result.expect("todo"));
                if response.is_err() {
                    eprintln!("failed to make a response: {:?}", response.err());
                } else {
                    println!("Sending reply: {response:?}");
                    // TODO
                    let written = socket
                        .write_all(response.unwrap().serialize().as_bytes())
                        .await;
                    if written.is_err() {
                        eprintln!("Failed to reply: {:?}", written.err());
                    }
                }
            }
        });
    }
}
