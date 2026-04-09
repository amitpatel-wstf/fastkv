use lz4_flex::{compress_prepend_size, decompress_size_prepended};
use hashbrown::HashMap;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::RwLock;
use std::hash::{Hash, Hasher};
use ahash::AHasher;

#[derive(Debug)]
pub enum CommandError {
    EmptyInput,
    InvalidCommand,
    MissingArguments,
}

pub enum Value {
    Inline([u8; 32], usize),
    Heap(Vec<u8>),
}

fn current_timestamp() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

pub struct Shard {
    pub data: HashMap<Vec<u8>, Value>,
    pub expiry: HashMap<Vec<u8>, u64>,
}

impl Shard {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expiry: HashMap::new(),
        }
    }

    fn store_value(data: Vec<u8>) -> Value {
        if data.len() <= 32 {
            let mut buf = [0u8; 32];
            buf[..data.len()].copy_from_slice(&data);
            Value::Inline(buf, data.len())
        } else {
            Value::Heap(data)
        }
    }

    pub fn get_slice(value: &Value) -> &[u8] {
        match value {
            Value::Inline(buf, len) => &buf[..*len],
            Value::Heap(vec) => vec.as_slice(),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, ttl: Option<u64>) {
        let val = Self::store_value(value);
        self.data.insert(key.clone(), val);

        if let Some(ttl_secs) = ttl {
            let expire_at = current_timestamp() + ttl_secs;
            self.expiry.insert(key, expire_at);
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&[u8]> {
        if let Some(&expire_at) = self.expiry.get(key) {
            if current_timestamp() > expire_at {
                self.data.remove(key);
                self.expiry.remove(key);
                return None;
            }
        }
        self.data.get(key).map(|v| Self::get_slice(v))
    }

    pub fn del(&mut self, key: &[u8]) {
        self.data.remove(key);
        self.expiry.remove(key);
    }
}

pub struct Store {
    shards: Vec<RwLock<Shard>>,
    shard_count: usize,
}

impl Store {
    pub fn new(shard_count: usize) -> Self {
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(RwLock::new(Shard::new()));
        }
        Self { shards, shard_count }
    }

    fn get_shard_index(&self, key: &[u8]) -> usize {
        let mut hasher = AHasher::default();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shard_count
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>, ttl: Option<u64>) {
        let idx = self.get_shard_index(&key);
        let mut shard = self.shards[idx].write().unwrap();
        shard.set(key, value, ttl);
    }

    pub fn with_get<F, R>(&self, key: &[u8], f: F) -> R 
    where 
        F: FnOnce(Option<&[u8]>) -> R,
    {
        let idx = self.get_shard_index(key);
        let mut shard = self.shards[idx].write().unwrap();
        let expire_at = shard.expiry.get(key).copied();
        if let Some(exp) = expire_at {
            if current_timestamp() > exp {
                shard.data.remove(key);
                shard.expiry.remove(key);
                return f(None);
            }
        }
        let val_opt = shard.data.get(key).map(|v| Shard::get_slice(v));
        f(val_opt)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.with_get(key, |opt| opt.map(|s| s.to_vec()))
    }

    pub fn del(&self, key: &[u8]) {
        let idx = self.get_shard_index(key);
        let mut shard = self.shards[idx].write().unwrap();
        shard.del(key);
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        let idx = self.get_shard_index(key);
        let shard = self.shards[idx].read().unwrap();
        shard.data.contains_key(key)
    }

    pub fn save_binary(&self, path: &str) {
        let mut buffer: Vec<u8> = Vec::new();

        for shard_lock in &self.shards {
            let shard = shard_lock.read().unwrap();
            for (key, value) in &shard.data {
                let val = Shard::get_slice(value);
                let key_len = key.len() as u32;
                let val_len = val.len() as u32;

                buffer.extend_from_slice(&key_len.to_le_bytes());
                buffer.extend_from_slice(key);
                buffer.extend_from_slice(&val_len.to_le_bytes());
                buffer.extend_from_slice(val);
            }
        }

        let compressed = compress_prepend_size(&buffer);
        let mut file = File::create(path).unwrap();
        file.write_all(&compressed).unwrap();
    }

    pub fn load_binary(&self, path: &str) {
        if let Ok(compressed) = fs::read(path) {
            if let Ok(buffer) = decompress_size_prepended(&compressed) {
                let mut i = 0;
                while i < buffer.len() {
                    let key_len = u32::from_le_bytes(buffer[i..i + 4].try_into().unwrap()) as usize;
                    i += 4;
                    let key = buffer[i..i + key_len].to_vec();
                    i += key_len;

                    let val_len = u32::from_le_bytes(buffer[i..i + 4].try_into().unwrap()) as usize;
                    i += 4;
                    let value = buffer[i..i + val_len].to_vec();
                    i += val_len;

                    self.set(key, value, None);
                }
            }
        }
    }

    pub fn cleanup_expired(&self) {
        let now = current_timestamp();
        for shard_lock in &self.shards {
            if let Ok(mut shard) = shard_lock.write() {
                let expired_keys: Vec<Vec<u8>> = shard
                    .expiry
                    .iter()
                    .filter(|(_, &expire_at)| now > expire_at)
                    .map(|(key, _)| key.clone())
                    .collect();

                for key in expired_keys {
                    shard.data.remove(&key);
                    shard.expiry.remove(&key);
                }
            }
        }
    }

    pub fn execute(&self, cmd: Command) -> String {
        match cmd {
            Command::Set(key, value, ttl) => {
                let ttl = ttl.or(Some(86400));
                self.set(key, value, ttl);
                "OK".to_string()
            }
            Command::Get(key) => {
                self.with_get(&key, |val_opt| {
                    match val_opt {
                        Some(val) => String::from_utf8_lossy(val).to_string(),
                        None => "(nil)".to_string(),
                    }
                })
            }
            Command::Del(key) => {
                self.del(&key);
                "OK".to_string()
            }
            Command::Exists(key) => {
                if self.exists(&key) {
                    "1".to_string()
                } else {
                    "0".to_string()
                }
            }
            Command::Save(path) => {
                self.save_binary(&path);
                "Saved".to_string()
            }
            Command::Load(path) => {
                self.load_binary(&path);
                "Loaded".to_string()
            }
        }
    }
}

pub enum Command {
    Set(Vec<u8>, Vec<u8>, Option<u64>),
    Get(Vec<u8>),
    Del(Vec<u8>),
    Save(String),
    Load(String),
    Exists(Vec<u8>),
}

pub fn parse_command(input: &str) -> Result<Command, CommandError> {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();

    if parts.is_empty() {
        return Err(CommandError::EmptyInput);
    }

    match parts[0].to_uppercase().as_str() {
        "SET" => {
            if parts.len() < 3 {
                return Err(CommandError::MissingArguments);
            }

            let mut ttl: Option<u64> = None;

            if parts.len() > 4 && parts[3] == "--expiry" {
                ttl = parts[4].parse::<u64>().ok();
            }

            Ok(Command::Set(
                parts[1].as_bytes().to_vec(),
                parts[2].as_bytes().to_vec(),
                ttl,
            ))
        }

        "GET" => {
            if parts.len() < 2 {
                return Err(CommandError::MissingArguments);
            }
            Ok(Command::Get(parts[1].as_bytes().to_vec()))
        }

        "DEL" => {
            if parts.len() < 2 {
                return Err(CommandError::MissingArguments);
            }
            Ok(Command::Del(parts[1].as_bytes().to_vec()))
        }

        "EXISTS" => {
            if parts.len() < 2 {
                return Err(CommandError::MissingArguments);
            }
            Ok(Command::Exists(parts[1].as_bytes().to_vec()))
        }

        "SAVE" => {
            if parts.len() < 2 {
                return Err(CommandError::MissingArguments);
            }
            Ok(Command::Save(parts[1].to_string()))
        }

        "LOAD" => {
            if parts.len() < 2 {
                return Err(CommandError::MissingArguments);
            }
            Ok(Command::Load(parts[1].to_string()))
        }

        _ => Err(CommandError::InvalidCommand),
    }
}