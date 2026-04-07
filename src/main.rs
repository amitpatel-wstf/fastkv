use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use fastkv::{parse_command, Store};

// =========================
// Entry Point
// =========================

fn main() {
    let store = Arc::new(RwLock::new(Store::new()));

    // =========================
    // Background Expiry Thread
    // =========================

    let store_clone = Arc::clone(&store);

    thread::spawn(move || loop {
        {
            if let Ok(mut store) = store_clone.write() {
                store.cleanup_expired();
            }
        }
        thread::sleep(Duration::from_secs(5));
    });

    // =========================
    // TCP Server
    // =========================

    let listener = TcpListener::bind("127.0.0.1:6379").expect("Failed to bind to port 6379");

    println!("🚀 Server running on 127.0.0.1:6379");

    for stream in listener.incoming() {
        let store = Arc::clone(&store);

        match stream {
            Ok(stream) => {
                println!("📡 New client connected");

                thread::spawn(move || {
                    handle_client(stream, store);
                });
            }
            Err(e) => {
                eprintln!("❌ Connection failed: {}", e);
            }
        }
    }
}

// =========================
// Client Handler
// =========================

fn handle_client(stream: TcpStream, store: Arc<RwLock<Store>>) {
    let reader = BufReader::new(&stream);
    let mut writer = &stream;

    for line in reader.lines() {
        let input = match line {
            Ok(input) => input,
            Err(_) => break,
        };

        let response = match parse_command(&input) {
            Ok(cmd) => match store.write() {
                Ok(mut store) => store.execute(cmd),
                Err(_) => "ERR internal lock error".to_string(),
            },
            Err(_) => "ERR invalid command".to_string(),
        };
        if writer.write_all(response.as_bytes()).is_err() {
            break;
        }

        if writer.write_all(b"\n").is_err() {
            break;
        }
    }

    println!("🔌 Client disconnected");
}
