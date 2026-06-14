# MiniRedis

A lightweight Redis implementation written in Go. MiniRedis is a minimal but functional in-memory data store that supports core Redis features including persistence, pub/sub messaging, and multiple data types.

## Features

### Core Data Types
- **Strings**: Basic key-value storage with atomic operations
- **Hashes**: Collections of field-value pairs
- **Sorted Sets (ZSets)**: Ordered collections with scores

### Advanced Features
- **Persistence**: Append-Only File (AOF) for durability
- **Pub/Sub**: Publish-subscribe messaging system
- **Key Expiration**: Automatic key expiration with background janitor process
- **Multiple Databases**: 16 isolated databases (Redis-compatible)
- **TTL Management**: Set and check key time-to-live

### Supported Commands

#### String Commands
- `GET key` - Get value of a key
- `SET key value` - Set a key to hold a string value
- `DEL key [key ...]` - Delete one or more keys
- `INCR key` - Increment integer value
- `DECR key` - Decrement integer value
- `MGET key [key ...]` - Get multiple keys
- `MSET key value [key value ...]` - Set multiple keys

#### Hash Commands
- `HSET key field value` - Set hash field
- `HGET key field` - Get hash field value
- `HDEL key field` - Delete hash field
- `HGETALL key` - Get all fields and values
- `HEXISTS key field` - Check if field exists
- `HLEN key` - Get number of fields

#### Sorted Set Commands
- `ZADD key score member` - Add member with score
- `ZRANGE key start stop` - Get members by index range
- `ZSCORE key member` - Get member score
- `ZREM key member` - Remove member
- `ZCARD key` - Get number of members
- `ZRANGEBYSCORE key min max` - Get members by score range

#### Pub/Sub Commands
- `PUBLISH channel message` - Publish message to channel
- `SUBSCRIBE channel [channel ...]` - Subscribe to channels
- `UNSUBSCRIBE channel [channel ...]` - Unsubscribe from channels

#### Server Commands
- `PING` - Ping the server
- `ECHO message` - Echo a message
- `EXISTS key [key ...]` - Check if keys exist
- `EXPIRE key seconds` - Set key expiration
- `PERSIST key` - Remove expiration
- `TTL key` - Get remaining time-to-live
- `FLUSHALL` - Delete all keys from all databases

## Getting Started

### Prerequisites
- Go 1.22.2 or higher

### Installation

1. Clone the repository:
```bash
cd /home/kavyan2/Desktop/Projects/MiniRedis
```

2. Build the project:
```bash
go build -o mini-redis
```

### Running the Server

Start the server:
```bash
./mini-redis
```

The server will start listening on `localhost:6379` and output:
```
Server(Mini-Redis) is listening on port 6379 ...
[AOF] AOF initialized
```

### Connecting as a Client

Use any Redis client (redis-cli, telnet, etc.):

```bash
# Using redis-cli
redis-cli -p 6379

# Using netcat
nc localhost 6379
```

Example session:
```
> PING
+PONG

> SET mykey "Hello"
+OK

> GET mykey
$5
Hello

> INCR counter
:1

> EXPIRE mykey 60
:1

> TTL mykey
:59
```

## Architecture

### Core Components

1. **main.go**: Entry point, TCP server setup, and connection handling
2. **store.go**: In-memory data storage with multiple databases and entry types
3. **commands.go**: Command parser and execution logic
4. **aof.go**: Append-Only File persistence mechanism
5. **pubsub.go**: Publish-subscribe implementation
6. **parser.go**: RESP (REdis Serialization Protocol) parser
7. **conn_context.go**: Connection context management
8. **HelperFunctions.go**: Utility functions

### Key Design Decisions

- **Thread-Safe Storage**: Uses RWMutex for concurrent access to data
- **AOF Persistence**: Logs all write operations to disk for durability
- **Background Processes**: Janitor goroutine for key expiration, fsync for AOF
- **Database Isolation**: 16 independent databases like Redis
- **Per-Connection Write Mutex**: Ensures atomic responses for pub/sub

## Persistence

MiniRedis uses an Append-Only File (AOF) for persistence:

- **File**: `appendonly.aof` - Stores all write operations
- **Replay**: AOF is replayed on server startup to restore state
- **Fsync**: Background process periodically flushes AOF to disk
- **Recovery**: The server automatically recovers from crashes by replaying the AOF

## Expiration Management

- Keys can be set with expiration times using the `EXPIRE` command
- A background janitor process periodically checks and removes expired keys
- Use `PERSIST` to remove expiration from a key
- Use `TTL` to check remaining time-to-live

## Performance Characteristics

- **Time Complexity**: Most operations are O(1) or O(n) where n is the size of the collection
- **Memory**: All data is stored in memory; AOF logs are written to disk
- **Concurrency**: Goroutine-based handling of multiple clients
- **Network**: Standard TCP protocol with RESP wire format

## Limitations

- Single-threaded command execution (one goroutine per client)
- In-memory storage only (with AOF persistence)
- No clustering or replication
- Limited to 16 databases
- Partial Redis compatibility (subset of commands)

## Future Enhancements

- [ ] Additional data types (Lists, Sets)
- [ ] More string commands (GETRANGE, SETRANGE, STRLEN)
- [ ] Transaction support (MULTI/EXEC)
- [ ] Lua scripting support
- [ ] Replication support
- [ ] Clustering
- [ ] RDB snapshots in addition to AOF
- [ ] CONFIG commands for runtime configuration

## Project Structure

```
MiniRedis/
├── main.go              # Server entry point
├── store.go             # In-memory storage
├── commands.go          # Command handlers
├── aof.go               # Append-only file persistence
├── pubsub.go            # Pub/sub implementation
├── parser.go            # RESP protocol parser
├── conn_context.go      # Connection management
├── HelperFunctions.go   # Utility functions
├── go.mod               # Go module file
└── README.md            # This file
```

## License

This project is an educational implementation of Redis. Please refer to your local regulations and the Redis license if you plan to use this in production.

## Contributing

Feel free to fork, modify, and improve this implementation!

## References

- [Redis Documentation](https://redis.io/documentation)
- [RESP Protocol](https://redis.io/docs/reference/protocol-spec/)
- [Go Documentation](https://golang.org/doc/)
