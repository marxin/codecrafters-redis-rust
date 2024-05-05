use std::collections::HashMap;

use crate::parser::RedisValue;

pub struct RedisServer {
    db: HashMap<String, String>,
}

impl RedisServer {
    pub fn new() -> Self {
        Self { db: HashMap::new() }
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
                    array.len() == 3,
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
                self.db.insert(key.to_owned(), value.to_owned());
                Ok(RedisValue::String("Ok".to_string()))
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
                Ok(self
                    .db
                    .get(key)
                    .map_or(RedisValue::None, |v| RedisValue::String(v.to_owned())))
            }
            _ => todo!(),
        }
    }
}
