package main

import (
    "bufio"
    "fmt"
    "io"
    "net"
    "strings"
    "time"
)

func main() {

    // Initialize AOF
    err := InitAOF()
    if err != nil {
        fmt.Println("Error initializing AOF:", err)
        return
    }

    // Replay AOF BEFORE accepting clients
    ReplayAOF()

    // Start periodic fsync
    go BackgroundAOFFsync()

    // Start TCP server
    listener, err := net.Listen("tcp", ":6379")
    if err != nil {
        fmt.Println("-Error starting server:", err)
        return
    }

    fmt.Println("Server(Mini-Redis) is listening on port 6379 ...")

    // Start expiration janitor
    go startJanitor()

    for {
        conn, err := listener.Accept()
        if err != nil {
            continue
        }
        go handleConnection(conn)
    }
}

func handleConnection(conn net.Conn) {
    defer conn.Close()

    fmt.Println("New client connected:", conn.RemoteAddr())

    reader := bufio.NewReader(conn)

    for {
        args, err := parseResp(reader)
        if err != nil {

            // Client disconnected normally
            if err == io.EOF {
                fmt.Println("Client disconnected:", conn.RemoteAddr())
                return
            }

            // Other protocol errors
            conn.Write([]byte("-ERR protocol error: " + err.Error() + "\r\n"))
            return
        }

        if len(args) == 0 {
            conn.Write([]byte("-ERR empty command\r\n"))
            continue
        }

        command := strings.ToUpper(args[0])
        handleCommand(conn, command, args)
    }
}

func startJanitor() {
    ticker := time.NewTicker(10 * time.Second)
    for range ticker.C {
        cleanExpiredEntries()
    }
}

func cleanExpiredEntries() {
    now := time.Now().Unix()
    toDelete := []string{}

    mu.RLock()
    for key, entry := range db {
        if entry.ExpireAt != 0 && entry.ExpireAt <= now {
            toDelete = append(toDelete, key)
        }
    }
    mu.RUnlock()

    if len(toDelete) == 0 {
        return
    }

    mu.Lock()
    for _, key := range toDelete {
        delete(db, key)
    }
    mu.Unlock()

    fmt.Printf("[Janitor] Cleaned %d expired keys\n", len(toDelete))
}

func handleCommand(conn net.Conn, command string, args []string) {
    resp, err := execCommand(args)

    if err == nil && !isReplayingAOF {
        upper := strings.ToUpper(command)
        switch upper {
        case "MSET", "FLUSHALL", "DEL", "SET", "EXPIRE", "PERSIST", "INCR", "DECR", "ZADD":
            LogCommand(upper, args[1:])
        }
    }

    conn.Write([]byte(resp))
}

func execCommand(args []string) (string, error) {
    if len(args) == 0 {
        return "-ERR empty command\r\n", fmt.Errorf("empty")
    }

    cmd := strings.ToUpper(args[0])

    fn, ok := commandTable[cmd]
    if !ok {
        return "-ERR unknown command '" + cmd + "'\r\n", fmt.Errorf("unknown command")
    }

    return fn(args)
}