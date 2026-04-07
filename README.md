# fastkv

A Redis-like in-memory key-value store and TCP server implemented in Rust. This project provides a simple, fast, and thread-safe database server that mimics basic Redis functionality.

## Features

- **In-Memory Storage**: Fast key-value operations using a thread-safe HashMap
- **TTL Support**: Automatic expiration of keys with configurable time-to-live
- **TCP Server**: Multi-threaded server listening on port 6379 (Redis default)
- **Persistence**: Save and load data to/from compressed binary files using LZ4
- **Background Cleanup**: Automatic removal of expired keys every 5 seconds
- **Simple Commands**: Supports SET, GET, DEL, EXISTS, SAVE, LOAD operations

## Installation

### Prerequisites

- Rust 1.70 or later
- Cargo package manager

### Building from Source

1. Clone the repository:    
   ```bash
   git clone https://github.com/amitpatel-wstf/fastkv.git
   cd fastkv
   ```

2. Build the project:
   ```bash
   cargo build --release
   ```

3. Run the server:
   ```bash
   cargo run
   ```

The server will start on `127.0.0.1:6379`.

## Usage

### Connecting to the Server

You can connect using any TCP client, such as `telnet` or `nc` (netcat):

```bash
telnet 127.0.0.1 6379
```

Or using netcat:
```bash
nc 127.0.0.1 6379
```

### Supported Commands

- **SET key value [ttl]**: Store a key-value pair. Optional TTL in seconds (default: 86400 = 24 hours)
  ```
  SET mykey myvalue
  SET mykey myvalue 3600  # expires in 1 hour
  ```

- **GET key**: Retrieve the value for a key
  ```
  GET mykey
  ```

- **DEL key**: Delete a key
  ```
  DEL mykey
  ```

- **EXISTS key**: Check if a key exists (returns 1 if exists, 0 otherwise)
  ```
  EXISTS mykey
  ```

- **SAVE path**: Save the current data to a compressed binary file
  ```
  SAVE data.bin
  ```

- **LOAD path**: Load data from a compressed binary file
  ```
  LOAD data.bin
  ```

### Example Session

```
SET hello world
OK
GET hello
world
EXISTS hello
1
DEL hello
OK
GET hello
(nil)
```

## Architecture

- **Store**: Core data structure using `HashMap` for key-value storage and expiry tracking
- **TCP Server**: Multi-threaded server handling concurrent connections
- **Background Thread**: Periodic cleanup of expired keys
- **Compression**: LZ4 for efficient binary persistence
- **Thread Safety**: Uses `RwLock` for safe concurrent access

## Dependencies

- `lz4_flex`: For fast LZ4 compression/decompression

## Development

### Running Tests

```bash
cargo test
```

### Code Formatting

```bash
cargo fmt
```

### Linting

```bash
cargo clippy
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Repository

[https://github.com/amitpatel-wstf/fastkv.git](https://github.com/amitpatel-wstf/fastkv.git)