use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex};

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::parser::RedisValue;
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

#[derive(Debug)]
struct ReplicationMonitor {
    broadcast_rx: broadcast::Receiver<RedisValue>,

    /// Replication monitor used for e.g. WAIT operation
    latest_repl_id: HashMap<SocketAddrV4, u64>,
}

impl ReplicationMonitor {
    fn new(mut repl_rx: mpsc::Receiver<RedisValue>) -> Self {
        // TODO: factor out capacity
        let (broadcast_tx, broadcast_rx) = broadcast::channel(16);

        tokio::spawn(async move {
            loop {
                let rep = repl_rx.recv().await.unwrap();
                println!("replicate: {rep:?}");
                // TODO
                broadcast_tx.send(rep).unwrap();
            }
        });

        Self {
            broadcast_rx,
            latest_repl_id: HashMap::default(),
        }
    }

    fn handle_replica(&self, mut stream: BufReader<TcpStream>) {
        tokio::spawn(async move {
            loop {
                println!("handling replication server part here!!!");
                let mut buf = [0u8; 1024];
                stream.read_exact(&mut buf).await.unwrap();
            }
        });
    }
}

type Storage = Arc<Mutex<HashMap<String, String>>>;

#[derive(Debug)]
struct RedisServer2 {
    /// Key-value storage of the server
    storage: Storage,

    /// Replication
    repl_tx: mpsc::Sender<RedisValue>,
    repl_monitor: ReplicationMonitor,

    /// Key expiration related channels
    expiration_tx: mpsc::Sender<(String, Instant)>,
}

impl RedisServer2 {
    fn new() -> Self {
        let (exp_tx, exp_rx) = mpsc::channel::<(String, Instant)>(16);
        let (repl_tx, repl_rx) = mpsc::channel(16);
        let storage: Storage = Arc::default();

        // Start the expiration thread
        RedisServer2::start_expiration_thread(storage.clone(), exp_rx, repl_tx.clone());

        Self {
            storage,
            expiration_tx: exp_tx,
            repl_monitor: ReplicationMonitor::new(repl_rx),
            repl_tx: repl_tx.clone(),
        }
    }

    async fn start_server(&self, addr: SocketAddr) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        println!("Listening on: {}", addr);

        loop {
            let (stream, _) = listener.accept().await?;
            let mut stream = BufReader::new(stream);

            let token_result = parser::parse_token(&mut stream).await.unwrap();
            println!("GOT: {token_result:?}");
            let (RedisValue::Array(token), _) = token_result else {
                todo!();
            };
            stream
                .write_all(RedisValue::None.serialize().as_bytes())
                .await
                .unwrap();

            self.repl_monitor.handle_replica(stream);
        }
    }

    fn start_expiration_thread(
        storage: Storage,
        mut exp_rx: mpsc::Receiver<(String, Instant)>,
        repl_tx: mpsc::Sender<RedisValue>,
    ) {
        tokio::spawn(async move {
            let mut set = JoinSet::new();

            loop {
                let storage = storage.clone();
                let plan_fn = |set: &mut JoinSet<String>, key, deadline| {
                    println!("planning sleep: {key}, deadline: {deadline:?}");
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
                            repl_tx.send(RedisValue::None).await.unwrap();
                            println!("removed {key}");
                        }
                    }
                }
            }
        });
    }
}

/*
impl Wrapper {
    fn new() -> Self {
        let dictionary = Arc::new(Mutex::new(HashMap::new()));
        let cloned_dictionary = dictionary.clone();

        tokio::spawn(async move {
            let mut counter = 0;
            loop {
                cloned_dictionary.lock().unwrap().insert(counter, counter);
                counter += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
                println!("hello from spawn");
            }
        });

        Self { dictionary }
    }
}
*/

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, args.port));

    /*
    RedisServer2::new().start_server(addr).await.unwrap();
    panic!();
    */

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
                // TODO
                let command = RedisRequest::try_from(token_result.unwrap().0).unwrap();
                println!("parsed command: {command:?}");
                if matches!(command, RedisRequest::Null) {
                    break;
                }
                // TODO
                let response = server.lock().unwrap().run(command);
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
