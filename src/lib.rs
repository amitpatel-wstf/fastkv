use lz4_flex::compress_prepend_size;
use lz4_flex::decompress_size_prepended;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

// =========================
// Error Handling
// =========================

#[derive(Debug)]
pub enum CommandError {
    EmptyInput,
    InvalidCommand,
    MissingArguments,
}

// =========================
// Time Helper
// =========================

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

// =========================
// Store (Core Engine)
// =========================

pub struct Store {
    data: HashMap<Vec<u8>, Vec<u8>>,
    expiry: HashMap<Vec<u8>, u64>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            expiry: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, ttl: Option<u64>) {
        self.data.insert(key.clone(), value);

        if let Some(ttl_secs) = ttl {
            let expire_at = current_timestamp() + ttl_secs;
            self.expiry.insert(key, expire_at);
        }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&Vec<u8>> {
        if let Some(&expire_at) = self.expiry.get(key) {
            if current_timestamp() > expire_at {
                self.data.remove(key);
                self.expiry.remove(key);
                return None;
            }
        }

        self.data.get(key)
    }

    pub fn del(&mut self, key: &[u8]) {
        self.data.remove(key);
        self.expiry.remove(key);
    }

    pub fn save_binary(&self, path: &str) {
        let mut buffer: Vec<u8> = Vec::new();

        for (key, value) in &self.data {
            let key_len = key.len() as u32;
            let val_len = value.len() as u32;

            buffer.extend_from_slice(&key_len.to_le_bytes());
            buffer.extend_from_slice(key);
            buffer.extend_from_slice(&val_len.to_le_bytes());
            buffer.extend_from_slice(value);
        }

        // 🔥 Compress entire buffer
        let compressed = compress_prepend_size(&buffer);

        let mut file = File::create(path).unwrap();
        file.write_all(&compressed).unwrap();
    }

    pub fn load_binary(&mut self, path: &str) {
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

    pub fn cleanup_expired(&mut self) {
        let now = current_timestamp();

        let expired_keys: Vec<Vec<u8>> = self
            .expiry
            .iter()
            .filter(|(_, &expire_at)| now > expire_at)
            .map(|(key, _)| key.clone())
            .collect();

        for key in expired_keys {
            self.data.remove(&key);
            self.expiry.remove(&key);
        }
    }

    pub fn execute(&mut self, cmd: Command) -> String {
        match cmd {
            Command::Set(key, value, ttl) => {
                let ttl = ttl.or(Some(86400)); // default 24h
                self.set(key, value, ttl);
                "OK".to_string()
            }

            Command::Get(key) => match self.get(&key) {
                Some(val) => String::from_utf8_lossy(val).to_string(),
                None => "(nil)".to_string(),
            },

            Command::Del(key) => {
                self.del(&key);
                "OK".to_string()
            }

            Command::Exists(key) => {
                if self.data.contains_key(&key) {
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

// =========================
// Command Enum
// =========================

pub enum Command {
    Set(Vec<u8>, Vec<u8>, Option<u64>),
    Get(Vec<u8>),
    Del(Vec<u8>),
    Save(String),
    Load(String),
    Exists(Vec<u8>),
}

// =========================
// Command Parser
// =========================

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

            if parts.len() > 3 {
                if parts[3] == "--expiry" && parts.len() > 4 {
                    ttl = parts[4].parse::<u64>().ok();
                }
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
