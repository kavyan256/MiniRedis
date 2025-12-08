package main

import (
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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
	case "TTL":
		handleTTL(conn, args)
	case "EXIT":
		conn.Write([]byte("+OK\r\n"))
		conn.Close()
	default:
		conn.Write([]byte("-ERR unknown command\r\n"))
	}
}

//14 command + exit

func handleSet(conn net.Conn, args []string) {
	if len(args) != 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'SET' command\r\n"))
		return
	}
	key := args[1]
	value := args[2]

	entry := Entry{
		Type:     TypeString,
		Value:    value,
		ExpireAt: 0,
	}

	setEntry(key, entry)
    LogCommand("SET", args[1:])

	conn.Write([]byte("+OK\r\n"))
}


func handleGet(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'GET' command\r\n"))
		return
	}

	entry, exists := getEntry(args[1])
	if !exists || (entry.ExpireAt != 0 && entry.ExpireAt <= time.Now().Unix()) {
		conn.Write([]byte("$-1\r\n"))
		return
	}

	if entry.Type != TypeString {
		conn.Write([]byte("-ERR wrong type of value for 'GET' command\r\n"))
		return
	}

	value := entry.Value.(string)
	conn.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"))
}

func handleDel(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'DEL' command\r\n"))
		return
	}

	removed := deleteEntry(args[1])
	if removed {
		LogCommand("DEL", args[1:])
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
	for _, key := range args[1:] {
		_, exists := getEntry(key)
		if exists {
			count++
		}
	}

	conn.Write([]byte(":" + strconv.Itoa(count) + "\r\n"))
}

func handleIncr(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'INCR' command\r\n"))
		return
	}

	key := args[1]
	entry, exists := getEntry(key)

	var intValue int

	if exists {
		if entry.Type != TypeString && entry.Type != TypeInt {
			conn.Write([]byte("-ERR value is not an integer\r\n"))
			return
		}

		switch v := entry.Value.(type) {
			case string:
				var err error
				intValue, err = strconv.Atoi(v)
				if err != nil {
					conn.Write([]byte("-ERR value is not an integer\r\n"))
					return
				}
			case int:
				intValue = v
		}
		intValue++
	} else {
		intValue = 1
	}

	newEntry := Entry{
		Type:     TypeInt,
		Value:    intValue,
		ExpireAt: 0,
	}

	setEntry(key, newEntry)
	LogCommand("INCR", args[1:])

	conn.Write([]byte(":" + strconv.Itoa(intValue) + "\r\n"))
}

func handleDecr(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'DECR' command\r\n"))
		return
	}

	key := args[1]
	entry, exists := getEntry(key)
	
	var intValue int

	if exists {
		if entry.Type != TypeString && entry.Type != TypeInt {
			conn.Write([]byte("-ERR value is not an integer\r\n"))
			return
		}

		switch v := entry.Value.(type) {
			case string:
				var err error
				intValue, err = strconv.Atoi(v)
				if err != nil {
					conn.Write([]byte("-ERR value is not an integer\r\n"))
					return
				}
			case int:
				intValue = v
		}
		intValue--
	} else {
		intValue = -1
	}

	newEntry := Entry{
		Type:     TypeInt,
		Value:    intValue,
		ExpireAt: 0,
	}

	setEntry(key, newEntry)
	LogCommand("DECR", args[1:])

	conn.Write([]byte(":" + strconv.Itoa(intValue) + "\r\n"))
	
}

func handleMget(conn net.Conn, args []string) {
	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'MGET' command\r\n"))
		return
	}

	conn.Write([]byte("*" + strconv.Itoa(len(args)-1) + "\r\n"))
	for _, key := range args[1:] {
		entry, exists := getEntry(key)
		if !exists || (entry.ExpireAt != 0 && entry.ExpireAt <= time.Now().Unix()) {
			conn.Write([]byte("$-1\r\n"))
			continue
		}

		val := entry.Value.(string)
		conn.Write([]byte("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n"))
	}
}

func handleMset(conn net.Conn, args []string) {
	if len(args) < 3 || len(args[1:])%2 != 0 {
		conn.Write([]byte("-ERR wrong number of arguments for 'MSET' command\r\n"))
		return
	}

	for i := 1; i < len(args); i += 2 {
		key := args[i]
		value := args[i+1]

		entry := Entry{
			Type:     TypeString,
			Value:    value,
			ExpireAt: 0,
		}

		setEntry(key, entry)
	}

	LogCommand("MSET", args[1:])
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
		old := db
		db = make(map[string]Entry)
		atomic.StoreInt64(&usedMemory, 0)
		mu.Unlock()

		go func(m map[string]Entry) {
			_ = m
		}(old)

		conn.Write([]byte("+OK\r\n"))

	case "ASYNC":
		go func() {
			mu.Lock()
			db = make(map[string]Entry)
			lastAccess = make(map[string]int64)
			atomic.StoreInt64(&usedMemory, 0)
			mu.Unlock()
		}()
		conn.Write([]byte("+OK\r\n"))

	default:
		conn.Write([]byte("-ERR invalid option for 'FLUSHALL' command\r\n"))
	}
}

func handleExpire(conn net.Conn, args []string) {
    // EXPIRE key seconds [NX|XX|GT|LT]
    if len(args) != 3 && len(args) != 4 {
        conn.Write([]byte("-ERR wrong number of arguments for 'EXPIRE' command\r\n"))
        return
    }

    key := args[1]

    seconds, err := strconv.Atoi(args[2])
    if err != nil || seconds < 0 {
        conn.Write([]byte("-ERR invalid expire time\r\n"))
        return
    }

    option := "NONE"
    if len(args) == 4 {
        option = strings.ToUpper(args[3])
    }

    // Load the entry
    entry, exists := getEntry(key)
    if !exists {
        conn.Write([]byte(":0\r\n")) // key does not exist
        return
    }

    newExpire := time.Now().Unix() + int64(seconds)
    oldExpire := entry.ExpireAt

    switch option {
    case "NONE":
        entry.ExpireAt = newExpire

    case "NX": // Only set if no expiration exists
        if oldExpire != 0 {
            conn.Write([]byte(":0\r\n"))
            return
        }
        entry.ExpireAt = newExpire

    case "XX": // Only set if expiration exists
        if oldExpire == 0 {
            conn.Write([]byte(":0\r\n"))
            return
        }
        entry.ExpireAt = newExpire

    case "GT": // Only set if new > old
        if oldExpire != 0 && newExpire <= oldExpire {
            conn.Write([]byte(":0\r\n"))
            return
        }
        entry.ExpireAt = newExpire

    case "LT": // Only set if new < old
        if oldExpire != 0 && newExpire >= oldExpire {
            conn.Write([]byte(":0\r\n"))
            return
        }
        entry.ExpireAt = newExpire

    default:
        conn.Write([]byte("-ERR invalid expire option\r\n"))
        return
    }

    // Save updated entry
    setEntry(key, entry)

    // Log to AOF
    LogCommand("EXPIRE", args[1:])

    conn.Write([]byte(":1\r\n"))
}

func handlePersist(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'PERSIST' command\r\n"))
		return
	}

	success := PersistEntry(args[1])
	if success {
		LogCommand("PERSIST", args[1:])
		conn.Write([]byte(":1\r\n"))
	} else {
		conn.Write([]byte(":0\r\n"))
	}
}

func handleTTL(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'TTL' command\r\n"))
		return
	}

	// Check if key exists
	entry, exists := getEntry(args[1])
	if !exists {
		conn.Write([]byte(":-2\r\n")) // key doesn't exist
		return
	}

	if entry.ExpireAt == 0 {
		conn.Write([]byte(":-1\r\n")) // key has no expiration
		return
	}

	ttl := entry.ExpireAt - time.Now().Unix()
	if ttl < 0 {
		conn.Write([]byte(":-2\r\n")) // key has expired
		return
	}

	conn.Write([]byte(":" + strconv.FormatInt(ttl, 10) + "\r\n"))
}
