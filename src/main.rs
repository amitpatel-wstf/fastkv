use std::env;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
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

    // Initialize store with 64 shards for good concurrent performance
    let store = Arc::new(Store::new(64));

    let store_clone = Arc::clone(&store);
    thread::spawn(move || loop {
        store_clone.cleanup_expired();
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

fn handle_client(stream: TcpStream, store: Arc<Store>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut writer = BufWriter::new(stream);

    loop {
        let buffer = match reader.fill_buf() {
            Ok(buf) => buf,
            Err(_) => break,
        };

        if buffer.is_empty() {
            break;
        }

        let is_resp = buffer[0] == b'*';

        let res = if is_resp {
            match read_resp_command(&mut reader) {
                Ok(Some(parts)) => handle_resp(parts, &store, &mut writer),
                _ => writer.write_all(b"-ERR invalid request\r\n"),
            }
        } else {
            let mut line = String::new();
            if reader.read_line(&mut line).is_err() {
                break;
            }
            handle_plain(line, &store, &mut writer)
        };

        if res.is_err() {
            break;
        }

        if writer.flush().is_err() {
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

fn handle_resp<W: Write>(parts: Vec<String>, store: &Arc<Store>, writer: &mut W) -> std::io::Result<()> {
    if parts.is_empty() {
        return writer.write_all(b"-ERR empty command\r\n");
    }

    match parts[0].to_uppercase().as_str() {
        "SET" => {
            if parts.len() < 3 {
                return writer.write_all(b"-ERR wrong args\r\n");
            }
            let key = parts[1].as_bytes().to_vec();
            let value = parts[2].as_bytes().to_vec();
            store.set(key, value, Some(86400));
            writer.write_all(b"+OK\r\n")
        }
        "GET" => {
            if parts.len() < 2 {
                return writer.write_all(b"-ERR wrong args\r\n");
            }
            let key = parts[1].as_bytes();
            store.with_get(key, |val_opt| {
                match val_opt {
                    Some(val) => {
                        writer.write_all(b"$")?;
                        writer.write_all(val.len().to_string().as_bytes())?;
                        writer.write_all(b"\r\n")?;
                        writer.write_all(val)?;
                        writer.write_all(b"\r\n")
                    }
                    None => writer.write_all(b"$-1\r\n"),
                }
            })
        }
        "DEL" => {
            if parts.len() < 2 {
                return writer.write_all(b"-ERR wrong args\r\n");
            }
            let key = parts[1].as_bytes();
            store.del(key);
            writer.write_all(b":1\r\n")
        }
        "EXISTS" => {
            if parts.len() < 2 {
                return writer.write_all(b"-ERR wrong args\r\n");
            }
            let key = parts[1].as_bytes();
            if store.exists(key) {
                writer.write_all(b":1\r\n")
            } else {
                writer.write_all(b":0\r\n")
            }
        }
        _ => writer.write_all(b"-ERR unknown command\r\n"),
    }
}

fn handle_plain<W: Write>(input: String, store: &Arc<Store>, writer: &mut W) -> std::io::Result<()> {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();

    if parts.is_empty() {
        return writer.write_all(b"ERR\n");
    }

    match parts[0].to_uppercase().as_str() {
        "SET" => {
            if parts.len() < 3 {
                return writer.write_all(b"ERR\n");
            }
            let key = parts[1].as_bytes().to_vec();
            let value = parts[2].as_bytes().to_vec();
            
            let mut ttl = Some(86400);
            if parts.len() > 4 && parts[3] == "--expiry" {
                ttl = parts[4].parse::<u64>().ok();
            }
            
            store.set(key, value, ttl);
            writer.write_all(b"OK\n")
        }
        "GET" => {
            if parts.len() < 2 {
                return writer.write_all(b"ERR\n");
            }
            let key = parts[1].as_bytes();
            store.with_get(key, |val_opt| {
                match val_opt {
                    Some(val) => {
                        writer.write_all(val)?;
                        writer.write_all(b"\n")
                    }
                    None => writer.write_all(b"(nil)\n"),
                }
            })
        }
        "DEL" => {
            if parts.len() < 2 {
                return writer.write_all(b"ERR\n");
            }
            let key = parts[1].as_bytes();
            store.del(key);
            writer.write_all(b"OK\n")
        }
        "EXISTS" => {
            if parts.len() < 2 {
                return writer.write_all(b"ERR\n");
            }
            let key = parts[1].as_bytes();
            if store.exists(key) {
                writer.write_all(b"1\n")
            } else {
                writer.write_all(b"0\n")
            }
        }
        "SAVE" => {
            if parts.len() < 2 {
                return writer.write_all(b"ERR\n");
            }
            store.save_binary(parts[1]);
            writer.write_all(b"Saved\n")
        }
        "LOAD" => {
            if parts.len() < 2 {
                return writer.write_all(b"ERR\n");
            }
            store.load_binary(parts[1]);
            writer.write_all(b"Loaded\n")
        }
        _ => writer.write_all(b"ERR\n"),
    }
}