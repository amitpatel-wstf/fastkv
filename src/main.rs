use std::env;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use fastkv::Store;
use threadpool::ThreadPool;

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut port = "6379".to_string();
    let mut max_threads: Option<usize> = None;

    if args.len() > 1 {
        port = args[1].clone();
    }

    if args.len() > 2 {
        max_threads = args[2].parse::<usize>().ok();
    }

    if let Ok(env_port) = env::var("RCLI_PORT") {
        port = env_port;
    }

    if let Ok(env_threads) = env::var("RCLI_THREADS") {
        max_threads = env_threads.parse::<usize>().ok();
    }

    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let thread_count = match max_threads {
        Some(user_val) => std::cmp::min(user_val, cpu_count),
        None => cpu_count,
    };

    let addr = format!("0.0.0.0:{}", port);

    let store = Arc::new(RwLock::new(Store::new()));

    let store_clone = Arc::clone(&store);
    thread::spawn(move || loop {
        if let Ok(mut store) = store_clone.write() {
            store.cleanup_expired();
        }
        thread::sleep(Duration::from_secs(5));
    });

    let pool = ThreadPool::new(thread_count);

    println!("Server running on {}", addr);
    println!("Thread pool size: {}", thread_count);

    let listener = TcpListener::bind(&addr).unwrap();

    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let store = Arc::clone(&store);

            pool.execute(move || {
                handle_client(stream, store);
            });
        }
    }
}

fn handle_client(stream: TcpStream, store: Arc<RwLock<Store>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = stream;

    loop {
        let buffer = match reader.fill_buf() {
            Ok(buf) => buf,
            Err(_) => break,
        };

        if buffer.is_empty() {
            break;
        }

        let response = if buffer[0] == b'*' {
            match read_resp_command(&mut reader) {
                Ok(Some(parts)) => handle_resp(parts, &store),
                _ => "-ERR invalid request\r\n".to_string(),
            }
        } else {
            let mut line = String::new();
            if reader.read_line(&mut line).is_err() {
                break;
            }
            handle_plain(line, &store)
        };

        if writer.write_all(response.as_bytes()).is_err() {
            break;
        }
    }
}

fn read_resp_command(
    reader: &mut BufReader<TcpStream>,
) -> std::io::Result<Option<Vec<String>>> {
    let mut first_line = String::new();
    if reader.read_line(&mut first_line)? == 0 {
        return Ok(None);
    }

    let count: usize = first_line[1..].trim().parse().unwrap_or(0);
    let mut parts = Vec::with_capacity(count);

    for _ in 0..count {
        let mut len_line = String::new();
        reader.read_line(&mut len_line)?;

        let len: usize = len_line[1..].trim().parse().unwrap_or(0);

        let mut buf = vec![0u8; len + 2];
        reader.read_exact(&mut buf)?;

        let value = String::from_utf8_lossy(&buf[..len]).to_string();
        parts.push(value);
    }

    Ok(Some(parts))
}

fn handle_resp(parts: Vec<String>, store: &Arc<RwLock<Store>>) -> String {
    if parts.is_empty() {
        return "-ERR empty command\r\n".to_string();
    }

    match parts[0].to_uppercase().as_str() {
        "SET" => {
            if parts.len() < 3 {
                return "-ERR wrong args\r\n".to_string();
            }
            let key = parts[1].as_bytes().to_vec();
            let value = parts[2].as_bytes().to_vec();
            let mut store = store.write().unwrap();
            store.set(key, value, Some(86400));
            "+OK\r\n".to_string()
        }
        "GET" => {
            if parts.len() < 2 {
                return "-ERR wrong args\r\n".to_string();
            }
            let key = parts[1].as_bytes();
            let mut store = store.write().unwrap();
            match store.get(key) {
                Some(val) => {
                    format!("${}\r\n{}\r\n", val.len(), String::from_utf8_lossy(val))
                }
                None => "$-1\r\n".to_string(),
            }
        }
        "DEL" => {
            let key = parts[1].as_bytes();
            let mut store = store.write().unwrap();
            store.del(key);
            ":1\r\n".to_string()
        }
        "EXISTS" => {
            let key = parts[1].as_bytes();
            let store = store.read().unwrap();
            if store.data.contains_key(key) {
                ":1\r\n".to_string()
            } else {
                ":0\r\n".to_string()
            }
        }
        _ => "-ERR unknown command\r\n".to_string(),
    }
}

fn handle_plain(input: String, store: &Arc<RwLock<Store>>) -> String {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();

    if parts.is_empty() {
        return "ERR\n".to_string();
    }

    match parts[0].to_uppercase().as_str() {
        "SET" => {
            let key = parts[1].as_bytes().to_vec();
            let value = parts[2].as_bytes().to_vec();
            let mut store = store.write().unwrap();
            store.set(key, value, Some(86400));
            "OK\n".to_string()
        }
        "GET" => {
            let key = parts[1].as_bytes();
            let mut store = store.write().unwrap();
            match store.get(key) {
                Some(val) => format!("{}\n", String::from_utf8_lossy(val)),
                None => "(nil)\n".to_string(),
            }
        }
        "DEL" => {
            let key = parts[1].as_bytes();
            let mut store = store.write().unwrap();
            store.del(key);
            "OK\n".to_string()
        }
        "EXISTS" => {
            let key = parts[1].as_bytes();
            let store = store.read().unwrap();
            if store.data.contains_key(key) {
                "1\n".to_string()
            } else {
                "0\n".to_string()
            }
        }
        _ => "ERR\n".to_string(),
    }
}