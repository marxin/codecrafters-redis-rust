use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::Add,
    sync::{Arc, Mutex},
};

use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, watch},
    task::JoinSet,
    time::Instant,
};
use tracing::{debug, error, info, info_span, Instrument};

use crate::{
    command::{RedisRequest, RedisResponse},
    parser::{self, RedisValue},
};

#[derive(Debug)]
struct ReplicationMonitor {
    replication_id: [u8; 20],

    /// Receiver for operations that need to be replicated.
    broadcast_tx: broadcast::Sender<RedisRequest>,
    /// Replication monitor used for e.g. WAIT operation.
    latest_repl_id: Arc<Mutex<HashMap<SocketAddr, u64>>>,
    /// Total number of bytes of the writes operations.
    total_written_bytes: u64,
    /// Watch notifier about the confirmed command by replicas.
    replicated_update_tx: watch::Sender<Vec<u64>>,
    replicated_update_rx: watch::Receiver<Vec<u64>>,
}

impl ReplicationMonitor {
    fn new(mut repl_rx: mpsc::Receiver<RedisRequest>) -> Self {
        // TODO: factor out capacity
        let (broadcast_tx, _) = broadcast::channel(16);

        let btx = broadcast_tx.clone();
        tokio::spawn(
            async move {
                loop {
                    let rep = repl_rx.recv().await.unwrap();
                    debug!("replicating command: {rep:?}");
                    // TODO
                    btx.send(rep).unwrap();
                }
            }
            .instrument(info_span!("replication manager")),
        );

        let replicated_update_channel = watch::channel(Vec::new());

        Self {
            replication_id: rand::random(),
            broadcast_tx,
            latest_repl_id: Arc::default(),
            total_written_bytes: 0,
            replicated_update_tx: replicated_update_channel.0,
            replicated_update_rx: replicated_update_channel.1,
        }
    }

    async fn handle_replica(
        &self,
        mut stream: BufReader<TcpStream>,
        addr: SocketAddr,
    ) -> anyhow::Result<()> {
        stream.write_all(&RedisValue::ok().serialize()).await?;
        let request = parser::parse_token(&mut stream).await.unwrap();
        debug!("parsed request: {request:?}");
        stream.write_all(&RedisValue::ok().serialize()).await?;
        let request = parser::parse_token(&mut stream).await.unwrap();
        debug!("parsed request: {request:?}");
        stream
            .write_all(
                &RedisValue::String(format!("FULLRESYNC {} 0", hex::encode(self.replication_id)))
                    .serialize(),
            )
            .await?;

        // sending empty RDB file now
        stream
            .write_all(&RedisValue::File(hex::decode(EMPTY_RDB)?).serialize())
            .await?;

        self.latest_repl_id.lock().unwrap().insert(addr, 0);

        // process replication channel here
        while let Ok(command) = self.broadcast_tx.subscribe().recv().await {
            info!("replicating command: {command:?}");

            match command {
                RedisRequest::Set { .. } | RedisRequest::Del { .. } => {
                    stream.write_all(&command.to_value().serialize()).await?;
                }
                RedisRequest::ReplConf { .. } => {
                    stream.write_all(&command.to_value().serialize()).await?;
                    let reply = parser::parse_token(&mut stream).await.unwrap().0;
                    let command = RedisRequest::try_from(reply)?;
                    info!("replconf reply received: {command:?}");
                }
                _ => todo!("unexpected command to replicate: {command:?}"),
            }
        }
        Ok(())
    }
}

type Storage = Arc<Mutex<HashMap<String, String>>>;

#[derive(Debug)]
pub struct RedisServer {
    /// Key-value storage of the server.
    storage: Storage,

    /// Replication-related fields.
    repl_tx: mpsc::Sender<RedisRequest>,
    repl_monitor: ReplicationMonitor,

    /// Key expiration related channel.
    expiration_tx: mpsc::Sender<(String, Instant)>,
}

const EMPTY_RDB: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

impl RedisServer {
    pub fn new() -> Self {
        let (exp_tx, exp_rx) = mpsc::channel::<(String, Instant)>(16);
        let (repl_tx, repl_rx) = mpsc::channel(16);
        let storage: Storage = Arc::default();

        // Start the expiration thread
        RedisServer::start_expiration_thread(storage.clone(), exp_rx, repl_tx.clone());

        Self {
            storage,
            expiration_tx: exp_tx,
            repl_monitor: ReplicationMonitor::new(repl_rx),
            repl_tx: repl_tx.clone(),
        }
    }

    async fn handle_connection(&self, stream: TcpStream, addr: SocketAddr) -> anyhow::Result<()> {
        let mut stream = BufReader::new(stream);

        loop {
            let token_result = parser::parse_token(&mut stream).await.unwrap();
            debug!("parsed command: {token_result:?}");
            let command = RedisRequest::try_from(token_result.0)?;
            match command {
                RedisRequest::Null => break,
                RedisRequest::ReplConf { arg, value } => {
                    anyhow::ensure!(arg == "listening-port");
                    info!("moving replication client to monitor: {value}");
                    self.repl_monitor
                        .handle_replica(stream, addr)
                        .instrument(info_span!("replication"))
                        .await?;
                    return Ok(());
                }
                _ => {
                    let response = self.run(command).await?;
                    debug!("sending reply: {response:?}");
                    // TODO
                    stream.write_all(&response.serialize()).await?;
                }
            }
        }

        Ok(())
    }

    pub async fn start_server(server: Arc<RedisServer>, addr: SocketAddr) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("Listening on: {}", addr);

        loop {
            let (stream, addr) = listener.accept().await?;
            let server = server.clone();
            tokio::spawn(
                async move {
                    if let Err(err) = server.handle_connection(stream, addr).await {
                        error!("handle connection failed: {err}");
                    }
                }
                .instrument(info_span!("connection", addr = %addr)),
            );
        }
    }

    async fn run(&self, request: RedisRequest) -> anyhow::Result<RedisResponse> {
        match request {
            RedisRequest::Ping => Ok(RedisResponse::String("PONG".to_string())),
            RedisRequest::Echo { message } => Ok(RedisResponse::String(message)),
            RedisRequest::Get { key } => Ok(self.storage.lock().unwrap().get(&key).map_or_else(
                || RedisResponse::Null,
                |value| RedisResponse::String(value.clone()),
            )),
            RedisRequest::Set {
                key,
                value,
                expiration,
            } => {
                self.storage
                    .lock()
                    .unwrap()
                    .insert(key.clone(), value.clone());
                if let Some(expiration) = expiration {
                    self.expiration_tx
                        .send((key.clone(), Instant::now().add(expiration)))
                        .await?;
                }
                self.repl_tx
                    .send(RedisRequest::Set {
                        key,
                        value,
                        expiration: None,
                    })
                    .await?;
                Ok(RedisResponse::String("OK".to_string()))
            }
            RedisRequest::Del { key } => {
                self.repl_tx
                    .send(RedisRequest::Del { key: key.clone() })
                    .await?;
                Ok(RedisResponse::String(
                    self.storage
                        .lock()
                        .unwrap()
                        .remove(&key)
                        .map_or(0, |_| 1)
                        .to_string(),
                ))
            }
            RedisRequest::Info => Ok(RedisResponse::String(format!(
                "role:master\nmaster_replid:{}\nmaster_repl_offset:0\n",
                hex::encode(self.repl_monitor.replication_id)
            ))),
            RedisRequest::ReplConf { .. } => anyhow::bail!("REPLCONF should not be handled here"),
            RedisRequest::Null => panic!("unexpected NULL command here"),
            RedisRequest::Wait { .. } => {
                self.repl_monitor
                    .broadcast_tx
                    .send(RedisRequest::ReplConf {
                        arg: "GETACK".to_string(),
                        value: "*".to_string(),
                    })?;
                Ok(RedisResponse::Integer(
                    self.repl_monitor.latest_repl_id.lock().unwrap().len() as i64,
                ))
            }
        }
    }

    fn start_expiration_thread(
        storage: Storage,
        mut exp_rx: mpsc::Receiver<(String, Instant)>,
        repl_tx: mpsc::Sender<RedisRequest>,
    ) {
        tokio::spawn(
            async move {
                let mut set = JoinSet::new();

                loop {
                    let storage = storage.clone();
                    let plan_fn = |set: &mut JoinSet<String>, key, deadline| {
                        debug!("planning sleep: {key}, deadline: {deadline:?}");
                        set.spawn(async move {
                            tokio::time::sleep_until(deadline).await;
                            key
                        });
                    };

                    if set.is_empty() {
                        let (key, deadline) = exp_rx.recv().await.unwrap();
                        plan_fn(&mut set, key, deadline);
                    } else {
                        tokio::select! {
                            request = exp_rx.recv() => {
                                let (key, deadline) = request.unwrap();
                                plan_fn(& mut set, key, deadline);
                            },
                            key = set.join_next() => {
                                let key = key.unwrap().unwrap();
                                let _ = storage.lock().unwrap().remove(&key);
                                // TODO
                                repl_tx.send(RedisRequest::Del { key: key.clone() }).await.unwrap();
                                debug!("removed {key}");
                            }
                        }
                    }
                }
            }
            .instrument(info_span!("expiration handler")),
        );
    }
}
