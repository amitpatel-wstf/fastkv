use std::collections::HashMap;
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
        self.expiry.remove(key); // ✅ also remove expiry
    }

    pub fn execute(&mut self, cmd: Command) -> String {
        match cmd {
            // ✅ FIXED: now includes ttl
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

        _ => Err(CommandError::InvalidCommand),
    }
}