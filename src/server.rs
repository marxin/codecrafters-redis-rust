use crate::parser::RedisValue;

pub struct RedisServer {}

impl RedisServer {
    pub fn run(&self, value: RedisValue) -> anyhow::Result<RedisValue> {
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
                    "unexpected arguments for echo command: {array:?}"
                );
                let argument = array.into_iter().nth(1).unwrap();
                let RedisValue::String(argument) = argument else {
                    anyhow::bail!("echo argument needs a String value: {argument:?}");
                };
                Ok(RedisValue::String(argument))
            }
            _ => todo!(),
        }
    }
}
