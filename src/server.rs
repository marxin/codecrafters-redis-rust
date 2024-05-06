use std::collections::HashMap;
use std::ops::Add;
use std::time::{Duration, Instant};

use crate::parser::RedisValue;

pub struct RedisServer {
    db: HashMap<String, (String, Option<Instant>)>,
    replicateof: Option<String>,
    replication_id: [u8; 20],
}

impl RedisServer {
    pub fn new(replicateof: Option<String>) -> Self {
        Self {
            db: HashMap::new(),
            replicateof,
            replication_id: rand::random(),
        }
    }

    pub fn run(&mut self, value: RedisValue) -> anyhow::Result<RedisValue> {
        let RedisValue::Array(array) = value else {
            anyhow::bail!("Unexpected redis value: {value:?}")
        };

        let command = array.first().ok_or(anyhow::anyhow!("An empty array"))?;
        let RedisValue::String(command) = command else {
            anyhow::bail!("Unexpected command: {command:?}")
        };

        let command = command.to_lowercase();
        match command.as_str() {
            "ping" => Ok(RedisValue::String("PONG".to_string())),
            "echo" => {
                anyhow::ensure!(
                    array.len() == 2,
                    "unexpected arguments for ECHO command: {array:?}"
                );
                let argument = &array[1];
                let RedisValue::String(argument) = argument else {
                    anyhow::bail!("echo argument needs a String value: {argument:?}");
                };
                Ok(RedisValue::String(argument.to_owned()))
            }
            "set" => {
                anyhow::ensure!(
                    array.len() == 3 || array.len() == 5,
                    "unexpected arguments for SET command: {array:?}"
                );
                let key = &array[1];
                let RedisValue::String(key) = key else {
                    anyhow::bail!("SET argument key must be string: {key:?}");
                };
                let value = &array[2];
                let RedisValue::String(value) = value else {
                    anyhow::bail!("SET argument value must be string: {value:?}");
                };
                let mut expiration = None;
                if array.len() == 5 {
                    // TODO: check 4th argument
                    let RedisValue::String(ref duration) = array[4] else {
                        anyhow::bail!("SET 4th argument key must be string: {:?}", array[4]);
                    };
                    expiration =
                        Some(Instant::now().add(Duration::from_millis(duration.parse::<u64>()?)));
                }
                self.db
                    .insert(key.to_owned(), (value.to_owned(), expiration));
                Ok(RedisValue::String("OK".to_string()))
            }
            "get" => {
                anyhow::ensure!(
                    array.len() == 2,
                    "unexpected arguments for GET command: {array:?}"
                );
                let key = &array[1];
                let RedisValue::String(key) = key else {
                    anyhow::bail!("GET argument key must be string: {key:?}");
                };
                let value = self.db.get(key);
                let Some(value) = value else {
                    return Ok(RedisValue::None);
                };
                let retval = match value.1 {
                    None => Some(value.0.to_owned()),
                    Some(expiration) if expiration < Instant::now() => {
                        self.db.remove(key);
                        None
                    }
                    Some(_) => Some(value.0.to_owned()),
                };
                Ok(retval.map_or_else(|| RedisValue::None, RedisValue::String))
            }
            "info" => {
                anyhow::ensure!(
                    array.len() == 2,
                    "unexpected arguments for INFO command: {array:?}"
                );
                let detail = &array[1];
                let RedisValue::String(detail) = detail else {
                    anyhow::bail!("INFO argument key must be string: {detail:?}");
                };
                anyhow::ensure!(detail == "replication");

                let output = if self.replicateof.is_some() {
                    "role:slave\n".to_string()
                } else {
                    format!(
                        "role:master\nmaster_replid:{}\nmaster_repl_offset:0\n",
                        hex::encode(self.replication_id)
                    )
                };

                Ok(RedisValue::String(output))
            }
            _ => todo!(),
        }
    }
}
