package main

import (
    "net"
    "strconv"
    "strings"
)

func handleCommand(conn net.Conn, command string, args []string) {
    switch command {
    case "SET":
        handleSet(conn, args)
    case "GET":
        handleGet(conn, args)
    case "DEL":
        handleDel(conn, args)
    case "PING":
        handlePing(conn, args)
    case "ECHO":
        handleEcho(conn, args)
    case "EXISTS":
        handleExists(conn, args)
    case "INCR":
        handleIncr(conn, args)
    case "DECR":
        handleDecr(conn, args)
    case "MGET":
        handleMget(conn, args)
    case "MSET":
        handleMset(conn, args)
    case "FLUSHALL":
        handleFlushall(conn, args)
    case "EXPIRE":
        handleExpire(conn, args)
    case "PERSIST":
        handlePersist(conn, args)
    case "EXIT":
        conn.Write([]byte("+OK\r\n"))
        conn.Close()
    default:
        conn.Write([]byte("-ERR unknown command\r\n"))
    }
}

func handleSet(conn net.Conn, args []string) {
    if len(args) != 3 {
        conn.Write([]byte("-ERR wrong number of arguments for 'SET' command\r\n"))
        return
    }
    key := args[1]
    value := args[2]

    mu.Lock()
    store[key] = value
    mu.Unlock()

    expMu.Lock()
    delete(expirations, key)
    expMu.Unlock()

    conn.Write([]byte("+OK\r\n"))
}

func handleGet(conn net.Conn, args []string) {
    if len(args) != 2 {
        conn.Write([]byte("-ERR wrong number of arguments for 'GET' command\r\n"))
        return
    }

    mu.RLock()
    key := args[1]
    value, exists := store[key]
    mu.RUnlock()

    if exists {
        conn.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"))
    } else {
        conn.Write([]byte("$-1\r\n"))
    }
}

func handleDel(conn net.Conn, args []string) {
    if len(args) != 2 {
        conn.Write([]byte("-ERR wrong number of arguments for 'DEL' command\r\n"))
        return
    }

    key := args[1]

    mu.Lock()
    _, exists := store[key]
    if exists {
        delete(store, key)
    }
    mu.Unlock()
    
    expMu.Lock()
    delete(expirations, key)
    expMu.Unlock()

    if exists {
        conn.Write([]byte(":1\r\n"))
    } else {
        conn.Write([]byte(":0\r\n"))
    }
}

func handlePing(conn net.Conn, args []string) {
    if len(args) == 1 {
        conn.Write([]byte("+PONG\r\n"))
    } else if len(args) == 2 {
        message := args[1]
        conn.Write([]byte("$" + strconv.Itoa(len(message)) + "\r\n" + message + "\r\n"))
    } else {
        conn.Write([]byte("-ERR wrong number of arguments for 'PING' command\r\n"))
    }
}

func handleEcho(conn net.Conn, args []string) {
    if len(args) != 2 {
        conn.Write([]byte("-ERR wrong number of arguments for 'ECHO' command\r\n"))
        return
    }
    message := args[1]
    conn.Write([]byte("$" + strconv.Itoa(len(message)) + "\r\n" + message + "\r\n"))
}

func handleExists(conn net.Conn, args []string) {
    if len(args) < 2 {
        conn.Write([]byte("-ERR wrong number of arguments for 'EXISTS' command\r\n"))
        return
    }
    count := 0
    mu.RLock()
    for _, key := range args[1:] {
        _, exists := store[key]
        if exists {
            count++
        }
    }
    mu.RUnlock()
    conn.Write([]byte(":" + strconv.Itoa(count) + "\r\n"))
}

func handleIncr(conn net.Conn, args []string) {
    if len(args) != 2 {
        conn.Write([]byte("-ERR wrong number of arguments for 'INCR' command\r\n"))
        return
    }

    key := args[1]

    mu.Lock()
    value, exists := store[key]
    if !exists {
        store[key] = "1"
        mu.Unlock()
        conn.Write([]byte(":1\r\n"))
        return
    }

    intValue, err := strconv.Atoi(value)
    if err != nil {
        mu.Unlock()
        conn.Write([]byte("-ERR value is not an integer\r\n"))
        return
    }

    intValue++
    store[key] = strconv.Itoa(intValue)
    mu.Unlock()
    conn.Write([]byte(":" + strconv.Itoa(intValue) + "\r\n"))
}

func handleDecr(conn net.Conn, args []string) {
    if len(args) != 2 {
        conn.Write([]byte("-ERR wrong number of arguments for 'DECR' command\r\n"))
        return
    }

    key := args[1]

    mu.Lock()
    value, exists := store[key]
    if !exists {
        store[key] = "-1"
        mu.Unlock()
        conn.Write([]byte(":-1\r\n"))
        return
    }

    intValue, err := strconv.Atoi(value)
    if err != nil {
        mu.Unlock()
        conn.Write([]byte("-ERR value is not an integer\r\n"))
        return
    }

    intValue--
    store[key] = strconv.Itoa(intValue)
    mu.Unlock()
    conn.Write([]byte(":" + strconv.Itoa(intValue) + "\r\n"))
}

func handleMget(conn net.Conn, args []string) {
    if len(args) < 2 {
        conn.Write([]byte("-ERR wrong number of arguments for 'MGET' command\r\n"))
        return
    }

    numKeys := len(args) - 1
    conn.Write([]byte("*" + strconv.Itoa(numKeys) + "\r\n"))

    for _, key := range args[1:] {
        mu.RLock()
        value, exists := store[key]
        mu.RUnlock()
        if exists {
            conn.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"))
        } else {
            conn.Write([]byte("$-1\r\n"))
        }
    }
}

func handleMset(conn net.Conn, args []string) {
    if len(args) < 3 || len(args[1:])%2 != 0 {
        conn.Write([]byte("-ERR wrong number of arguments for 'MSET' command\r\n"))
        return
    }

    mu.Lock()
    for i := 1; i < len(args); i += 2 {
        key := args[i]
        value := args[i+1]
        store[key] = value
    }
    mu.Unlock()
    conn.Write([]byte("+OK\r\n"))
}

func handleFlushall(conn net.Conn, args []string) {
    if !(len(args) == 1 || (len(args) == 2)) {
        conn.Write([]byte("-ERR wrong number of arguments for 'FLUSHALL' command\r\n"))
        return
    }

    mode := "SYNC"
    if len(args) == 2 {
        mode = strings.ToUpper(args[1])
    }

    switch mode {
    case "SYNC":
        mu.Lock()
        old := store
        store = make(map[string]string)
        mu.Unlock()

        go func(m map[string]string) {
            _ = m
        }(old)

        conn.Write([]byte("+OK\r\n"))

    case "ASYNC":
        go func() {
            mu.Lock()
            store = make(map[string]string)
            mu.Unlock()
        }()
        conn.Write([]byte("+OK\r\n"))

    default:
        conn.Write([]byte("-ERR invalid option for 'FLUSHALL' command\r\n"))
    }
}

func handleExpire(conn net.Conn, args []string) {
    if len(args) != 3 {
        conn.Write([]byte("-ERR wrong number of arguments for 'EXPIRE' command\r\n"))
        return
    }

    key := args[1]
    option := "NX"
    if len(args) == 4 {
        option = strings.ToUpper(args[3])
    }

    seconds, err := strconv.Atoi(args[2])
    if err != nil || seconds < 0 {
        conn.Write([]byte("-ERR invalid expire time\r\n"))
        return
    }

    expMu.Lock()
    switch option {
        case "NX":
            mu.Lock()
            _, exists := expirations[key]
            if !exists {
                expirations[key] = int64(seconds)
                conn.Write([]byte(":1\r\n"))
            } else {
                conn.Write([]byte(":0\r\n"))
            }
            mu.Unlock()

        case "XX":
            mu.Lock()
            _, exists := expirations[key]
            if exists {
                expirations[key] = int64(seconds)
                conn.Write([]byte(":1\r\n"))
            } else {
                conn.Write([]byte(":0\r\n"))
            }
            mu.Unlock()

        case "GT":
            mu.Lock()
            current, exists := expirations[key]
            if !exists || int64(seconds) > current {
                expirations[key] = int64(seconds)
                conn.Write([]byte(":1\r\n"))
            } else {
                conn.Write([]byte(":0\r\n"))
            }
            mu.Unlock()

        case "LT":
            mu.Lock()
            current, exists := expirations[key]
            if !exists || int64(seconds) < current {
                expirations[key] = int64(seconds)
                conn.Write([]byte(":1\r\n"))
            } else {
                conn.Write([]byte(":0\r\n"))
            }
            mu.Unlock()

        default:
            conn.Write([]byte("-ERR invalid option for 'EXPIRE' command\r\n"))
    }
    expMu.Unlock()
}

func handlePersist(conn net.Conn, args []string) {
    if len(args) != 2 {
        conn.Write([]byte("-ERR wrong number of arguments for 'PERSIST' command\r\n"))
        return
    }

    key := args[1]

    expMu.Lock()
    _, exists := expirations[key]
    if exists {
        delete(expirations, key)
        conn.Write([]byte(":1\r\n"))
    } else {
        conn.Write([]byte(":0\r\n"))
    }
    expMu.Unlock()
}