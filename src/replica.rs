use std::{
    collections::HashMap,
    future::{self},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tracing::{debug, info, info_span, Instrument};

use crate::{
    command::{RedisRequest, RedisResponse},
    parser::{self, RedisValue},
};

type Storage = Arc<Mutex<HashMap<String, String>>>;

pub struct RedisReplica {
    storage: Storage,

    replicaof: SocketAddr,
}

impl RedisReplica {
    pub fn new(replicaof: SocketAddr) -> Self {
        Self {
            storage: Storage::default(),
            replicaof,
        }
    }

    pub async fn start_replication(&self, addr: SocketAddr) -> anyhow::Result<()> {
        info!("Replicating: {}, listening on: {}", self.replicaof, addr);
        let mut stream = BufReader::new(TcpStream::connect(self.replicaof).await?);

        stream
            .write_all(&RedisValue::Array(vec![RedisValue::String("ping".to_string())]).serialize())
            .await?;
        info!("PING sent");
        let reply = parser::parse_token(&mut stream).await.unwrap();
        anyhow::ensure!(reply.0 == RedisValue::String("PONG".to_string()));

        let request: RedisValue =
            ["replconf", "listening-port", &addr.port().to_string()][..].into();
        stream.write_all(&request.serialize()).await?;
        info!("REPLCONF 1 sent");
        let reply = parser::parse_token(&mut stream).await.unwrap();
        anyhow::ensure!(reply.0 == RedisValue::ok());

        let request: RedisValue = ["replconf", "capa", "psync2"][..].into();
        stream.write_all(&request.serialize()).await?;
        info!("REPLCONF 2 sent");
        let reply = parser::parse_token(&mut stream).await.unwrap();
        anyhow::ensure!(reply.0 == RedisValue::ok());

        let request: RedisValue = ["psync", "?", "-1"][..].into();
        stream.write_all(&request.serialize()).await?;
        info!("PSYNC 2 sent");
        let reply = parser::parse_token(&mut stream).await.unwrap();
        info!("got reply for PSYNC: {:?}", reply.0);
        // TODO: check arguments of FULLRESYNC

        let reply = parser::parse_file(&mut stream).await.unwrap();
        let RedisValue::File(content) = reply else {
            panic!();
        };
        info!(
            "got RDB file ({}), starting main loop in replica",
            content.len()
        );

        let storage = self.storage.clone();
        tokio::spawn(async move {
            Self::start_server(storage, addr).await.unwrap();
        });

        let mut replicated_bytes = 0;
        loop {
            let (token, token_size) = parser::parse_token(&mut stream).await.unwrap();
            let command = RedisRequest::try_from(token)?;
            info!("parsed command: {command:?}");
            match command {
                RedisRequest::Null => break,
                RedisRequest::Set { key, value, .. } => {
                    self.storage.lock().unwrap().insert(key, value);
                }
                RedisRequest::Del { key } => {
                    self.storage.lock().unwrap().remove(&key);
                }
                RedisRequest::ReplConf { arg, value } => {
                    anyhow::ensure!(arg == "GETACK" && value == "*");
                    let reply: RedisValue =
                        ["REPLCONF", "ACK", &replicated_bytes.to_string()][..].into();
                    info!("sending reply: {reply:?}");
                    stream.write_all(&reply.serialize()).await?;
                }
                RedisRequest::Ping => {}
                _ => todo!(),
            }

            replicated_bytes += token_size;
        }

        future::pending::<()>().await;

        Ok(())
    }

    async fn handle_connection(storage: Storage, stream: TcpStream) -> anyhow::Result<()> {
        let mut stream = BufReader::new(stream);

        loop {
            let query = parser::parse_token(&mut stream).await.unwrap();
            debug!("parsed command: {query:?}");
            let command = RedisRequest::try_from(query.0)?;

            let reply = match command {
                RedisRequest::Null => {
                    break;
                }
                RedisRequest::Ping => RedisResponse::String("PONG".to_string()),
                RedisRequest::Get { key } => storage.lock().unwrap().get(&key).map_or_else(
                    || RedisResponse::Null,
                    |value| RedisResponse::String(value.clone()),
                ),
                RedisRequest::Info => RedisResponse::String("role:slave\n".to_string()),
                _ => todo!("unsupported command: {command:?}"),
            };

            stream.write_all(&reply.serialize()).await?;
        }

        Ok(())
    }

    async fn start_server(storage: Storage, addr: SocketAddr) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("Listening on: {}", addr);

        loop {
            let (stream, addr) = listener.accept().await?;
            let storage = storage.clone();
            tokio::spawn(
                async move {
                    Self::handle_connection(storage, stream).await.unwrap();
                }
                .instrument(info_span!("connection", addr = %addr)),
            );
        }
    }
}
