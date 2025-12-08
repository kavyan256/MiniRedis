package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
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

//15 command

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

func isExpired(key string) bool {
	expMu.RLock()
	exp, ok := expirations[key]
	expMu.RUnlock()

	if !ok {
		return false // no expiration set
	}

	if time.Now().Unix() >= exp {
		// Key is expired → delete it atomically
		mu.Lock()
		delete(store, key)
		mu.Unlock()

		expMu.Lock()
		delete(expirations, key)
		expMu.Unlock()

		return true
	}
	return false
}

func handleGet(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'GET' command\r\n"))
		return
	}

	key := args[1]

	// Check if key is expired (lazy deletion)
	if isExpired(key) {
		conn.Write([]byte("$-1\r\n")) // nil - key expired
		return
	}

	mu.RLock()
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

	// FIX: Check key exists in store FIRST (with mu)
	mu.RLock()
	_, keyExists := store[key]
	mu.RUnlock()

	if !keyExists {
		conn.Write([]byte(":0\r\n")) // key doesn't exist
		return
	}

	// FIX: Convert to Unix timestamp (current time + seconds)
	expirationTime := time.Now().Unix() + int64(seconds)

	expMu.Lock()
	defer expMu.Unlock()

	switch option {
	case "NX":
		// Only set if no expiration exists
		_, exists := expirations[key]
		if !exists {
			expirations[key] = expirationTime
			conn.Write([]byte(":1\r\n"))
		} else {
			conn.Write([]byte(":0\r\n"))
		}

	case "XX":
		// Only set if expiration already exists
		_, exists := expirations[key]
		if exists {
			expirations[key] = expirationTime
			conn.Write([]byte(":1\r\n"))
		} else {
			conn.Write([]byte(":0\r\n"))
		}

	case "GT":
		// Only set if new expiration > current expiration
		current, exists := expirations[key]
		if !exists || expirationTime > current {
			expirations[key] = expirationTime
			conn.Write([]byte(":1\r\n"))
		} else {
			conn.Write([]byte(":0\r\n"))
		}

	case "LT":
		// Only set if new expiration < current expiration
		current, exists := expirations[key]
		if !exists || expirationTime < current {
			expirations[key] = expirationTime
			conn.Write([]byte(":1\r\n"))
		} else {
			conn.Write([]byte(":0\r\n"))
		}

	default:
		conn.Write([]byte("-ERR invalid option for 'EXPIRE' command\r\n"))
	}
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

func handleTTL(conn net.Conn, args []string) {
	if len(args) != 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'TTL' command\r\n"))
		return
	}

	key := args[1]

	// Check if key exists
	mu.RLock()
	_, exists := store[key]
	mu.RUnlock()

	if !exists {
		conn.Write([]byte(":-2\r\n")) // key doesn't exist
		return
	}

	// Check expiration
	expMu.RLock()
	exp, hasExpiration := expirations[key]
	expMu.RUnlock()

	if !hasExpiration {
		conn.Write([]byte(":-1\r\n")) // no expiration set
		return
	}

	// Return seconds remaining (not absolute timestamp)
	secondsRemaining := exp - time.Now().Unix()
	if secondsRemaining < 0 {
		secondsRemaining = 0
	}
	conn.Write([]byte(":" + strconv.FormatInt(secondsRemaining, 10) + "\r\n"))
}

func startJanitor() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			cleanExpiredKeys()
		}
	}()
}

func cleanExpiredKeys() {
	now := time.Now().Unix()
	var keysToDelete []string

	// Find expired keys (read-lock expirations)
	expMu.RLock()
	for key, exp := range expirations {
		if now >= exp {
			keysToDelete = append(keysToDelete, key)
		}
	}
	expMu.RUnlock()

	// Delete from store
	if len(keysToDelete) > 0 {
		mu.Lock()
		for _, key := range keysToDelete {
			delete(store, key)
		}
		mu.Unlock()

		// Delete from expirations
		expMu.Lock()
		for _, key := range keysToDelete {
			delete(expirations, key)
		}
		expMu.Unlock()

		fmt.Printf("[Janitor] Cleaned %d expired keys\n", len(keysToDelete))
	}
}
