package main

import (
    "bufio"
    "fmt"
    "io"
    "net"
    "strconv"
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

    selectedDB := 0
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
        handleCommand(conn, command, args, &selectedDB)
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

    mu.Lock()
    defer mu.Unlock()

    totalDeleted := 0

    for dbIndex := 0; dbIndex < NumDatabases; dbIndex++ {
        for key, entry := range databases[dbIndex] {
            if entry.ExpireAt != 0 && entry.ExpireAt <= now {
                delete(databases[dbIndex], key)
                totalDeleted++
            }
        }
    }

    if totalDeleted > 0 {
        fmt.Printf("[Janitor] Cleaned %d expired keys\n", totalDeleted)
    }
}


func handleCommand(conn net.Conn, command string, args []string, selectedDB *int) {
    resp, err := execCommand(args, selectedDB)

    if err == nil && !isReplayingAOF {
        upper := strings.ToUpper(command)
        switch upper {
        case 
            "SET", 
            "DEL",
            "INCR",
            "DECR",
            "MSET", 
            "FLUSHALL",  
            "EXPIRE", 
            "PERSIST",
            "HSET", 
            "HDEL", 
            "ZADD", 
            "ZREM":
            LogCommand(upper, args[1:])
        }
    }

    conn.Write([]byte(resp))
}

func execCommand(args []string, selectedDB *int) (string, error) {
    if len(args) == 0 {
        return "-ERR empty command\r\n", fmt.Errorf("empty")
    }

    cmd := strings.ToUpper(args[0])

    if cmd == "SELECT" {
        if len(args) != 2 {
            return "-ERR wrong number of arguments for 'SELECT' command\r\n", fmt.Errorf("wrong args")
        }
        dbIndex, err := strconv.Atoi(args[1])
        if err != nil || dbIndex < 0 || dbIndex >= NumDatabases {
            return "-ERR invalid database index\r\n", fmt.Errorf("invalid db index")
        }
        *selectedDB = dbIndex
        return "+OK\r\n", nil
    }

    fn, ok := commandTable[cmd]
    if !ok {
        return "-ERR unknown command '" + cmd + "'\r\n", fmt.Errorf("unknown command")
    }

    return fn(args, selectedDB)
}