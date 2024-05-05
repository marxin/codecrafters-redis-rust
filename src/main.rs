use std::sync::{Arc, Mutex};

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::parser::RedisValue;
use crate::server::RedisServer;

mod parser;
mod server;

const ADDR: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind(ADDR).await?;
    println!("Listening on: {}", ADDR);
    let server = Arc::new(Mutex::new(RedisServer::new()));

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
