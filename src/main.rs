use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::parser::{serialize, RedisValue};

mod parser;

const ADDR: &str = "127.0.0.1:6379";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind(ADDR).await?;
    println!("Listening on: {}", ADDR);

    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut socket = std::pin::pin!(BufReader::new(socket));
            let token_result = parser::parse_token(&mut socket).await;
            match token_result {
                Ok(value) => match value {
                    RedisValue::Array(items) => {
                        for item in items {
                            match item {
                                RedisValue::String(command) => match &command.as_str() {
                                    &"ping" => {
                                        let reply =
                                            serialize(RedisValue::String("PONG".to_string()));
                                        socket.write_all(reply.as_bytes()).await;
                                    }
                                    _ => todo!(),
                                },
                                _ => todo!("unsupported command"),
                            }
                        }
                    }
                    _ => todo!("only array replies supported now"),
                },
                Err(err) => eprintln!("Cannot parse token: {:?}", err),
            }
        });
    }
}
