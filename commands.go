package main

import (
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"fmt"
)

type CmdFunc func(args []string) (string, error)

var commandTable = map[string]CmdFunc{
	"GET":      cmdGET,
	"SET":      cmdSET,
	"DEL":      cmdDEL,
	"PING":     cmdPING,
	"ECHO":     cmdECHO,
	"EXISTS":   cmdEXISTS,
	"INCR":     cmdINCR,
	"DECR":     cmdDECR,
	"MGET":     cmdMGET,
	"MSET":     cmdMSET,
	"FLUSHALL": cmdFLUSHALL,
	"EXPIRE":   cmdEXPIRE,
	"PERSIST":  cmdPERSIST,
	"TTL":      cmdTTL,
	"HSET":     cmdHSET,
	"HGET":     cmdHGET,
	"HDEL":     cmdHDEL,
	"HGETALL":  cmdHGETALL,
	"HEXISTS":  cmdHEXISTS,
	"HLEN":     cmdHLEN,
}

//14 command + exit

func cmdGET(args []string) (string, error) {
    
    if len(args) != 2 {
        return "-ERR wrong number of arguments for 'GET'\r\n", fmt.Errorf("wrong args")
    }

    key := args[1]
    entry, exists := getEntry(key)

    if !exists {
        return "$-1\r\n", nil
    }

    if entry.Type != TypeString {
        return "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
            fmt.Errorf("wrong type")
    }

    value := entry.Value.(string)

    // Return bulk string
    resp := "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
    return resp, nil
}

func cmdSET(args []string) (string, error) {
	if len(args) != 3 {
		return "-ERR wrong number of arguments for 'SET'\r\n", fmt.Errorf("wrong args")
	}
	key := args[1]
	value := args[2]

	entry := Entry{
		Type:     TypeString,
		Value:    value,
		ExpireAt: 0,
	}

	setEntry(key, entry)
	return "+OK\r\n", nil
}

func cmdDEL(args []string) (string, error) {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'DEL'\r\n", fmt.Errorf("wrong args")
	}

	removed := deleteEntry(args[1])
	if removed {
		return ":1\r\n", nil
	} else {
		return ":0\r\n", nil
	}
}

func cmdPING(args []string) (string, error) {
	if len(args) == 1 {
		return "+PONG\r\n", nil
	} else if len(args) == 2 {
		message := args[1]
		return "$" + strconv.Itoa(len(message)) + "\r\n" + message + "\r\n", nil
	} else {
		return "-ERR wrong number of arguments for 'PING' command\r\n", fmt.Errorf("wrong args")
	}
}

func cmdECHO(args []string) (string, error) {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'ECHO' command\r\n", fmt.Errorf("wrong args")
	}
	message := args[1]
	return "$" + strconv.Itoa(len(message)) + "\r\n" + message + "\r\n", nil
}

func cmdEXISTS(args []string) (string, error) {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'EXISTS' command\r\n", fmt.Errorf("wrong args")
	}
	count := 0
	for _, key := range args[1:] {
		_, exists := getEntry(key)
		if exists {
			count++
		}
	}

	return ":" + strconv.Itoa(count) + "\r\n", nil
}

func cmdINCR(args []string) (string, error) {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'INCR' command\r\n", fmt.Errorf("wrong args")
	}

	key := args[1]
	entry, exists := getEntry(key)

	var intValue int

	if exists {
		if entry.Type != TypeString && entry.Type != TypeInt {
			return "-ERR value is not an integer\r\n", fmt.Errorf("wrong type")
		}

		switch v := entry.Value.(type) {
			case string:
				var err error
				intValue, err = strconv.Atoi(v)
				if err != nil {
					return "-ERR value is not an integer\r\n", fmt.Errorf("wrong type")
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

	return ":" + strconv.Itoa(intValue) + "\r\n", nil
}

func cmdDECR(args []string) (string, error) {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'DECR' command\r\n", fmt.Errorf("wrong args")
	}

	key := args[1]
	entry, exists := getEntry(key)
	
	var intValue int

	if exists {
		if entry.Type != TypeString && entry.Type != TypeInt {
			return "-ERR value is not an integer\r\n", fmt.Errorf("wrong type")
		}

		switch v := entry.Value.(type) {
			case string:
				var err error
				intValue, err = strconv.Atoi(v)
				if err != nil {
					return "-ERR value is not an integer\r\n", fmt.Errorf("wrong type")
				}
			case int:
				intValue = v
		}
		intValue--
	} else {
		intValue = -1
	}

	newEntry := Entry{
		Type:    TypeInt,
		Value:    intValue,
		ExpireAt: 0,
	}

	setEntry(key, newEntry)

	return ":" + strconv.Itoa(intValue) + "\r\n", nil
}

func cmdMGET(args []string) (string, error) {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'MGET' command\r\n", fmt.Errorf("wrong args")
	}

	var resp strings.Builder
	resp.WriteString("*" + strconv.Itoa(len(args)-1) + "\r\n")
	for _, key := range args[1:] {
		entry, exists := getEntry(key)
		if !exists || (entry.ExpireAt != 0 && entry.ExpireAt <= time.Now().Unix()) {
			resp.WriteString("$-1\r\n")
			continue
		}

		val := entry.Value.(string)
		resp.WriteString("$" + strconv.Itoa(len(val)) + "\r\n" + val + "\r\n")
	}
	return resp.String(), nil
}

func cmdMSET(args []string) (string, error) {
	if len(args) < 3 || len(args[1:])%2 != 0 {
		return "-ERR wrong number of arguments for 'MSET' command\r\n", fmt.Errorf("wrong args")
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

	return "+OK\r\n", nil
}

func cmdFLUSHALL(args []string) (string, error) {
	if !(len(args) == 1 || (len(args) == 2)) {
		return "-ERR wrong number of arguments for 'FLUSHALL' command\r\n", fmt.Errorf("wrong args")
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

		return "+OK\r\n", nil

	case "ASYNC":
		go func() {
			mu.Lock()
			db = make(map[string]Entry)
			lastAccess = make(map[string]int64)
			atomic.StoreInt64(&usedMemory, 0)
			mu.Unlock()
		}()
		return "+OK\r\n", nil

	default:
		return "-ERR unknown mode for 'FLUSHALL' command\r\n", fmt.Errorf("unknown mode")
	}
}

func cmdEXPIRE(args []string) (string, error) {
	if len(args) < 3 || len(args) > 4 {
		return "-ERR wrong number of arguments for 'EXPIRE' command\r\n", fmt.Errorf("wrong args")
	}

	key := args[1]

	seconds, err := strconv.Atoi(args[2])
	if err != nil || seconds < 0 {
		return "-ERR invalid expire time\r\n", fmt.Errorf("invalid expire time")
	}

	option := "NONE"
	if len(args) == 4 {
		option = strings.ToUpper(args[3])
	}

	// Load the entry
	entry, exists := getEntry(key)
	if !exists {
		return ":0\r\n", nil // key does not exist
	}

	newExpire := time.Now().Unix() + int64(seconds)
	oldExpire := entry.ExpireAt

	switch option {
	case "NONE":
		entry.ExpireAt = newExpire

	case "NX": // Only set if no expiration exists
		if oldExpire != 0 {
			return ":0\r\n", nil
		}
		entry.ExpireAt = newExpire

	case "XX": // Only set if expiration exists
		if oldExpire == 0 {
			return ":0\r\n", nil
		}
		entry.ExpireAt = newExpire

	case "GT": // Only set if new > old
		if oldExpire != 0 && newExpire <= oldExpire {
			return ":0\r\n", nil
		}
		entry.ExpireAt = newExpire

	case "LT": // Only set if new < old
		if oldExpire != 0 && newExpire >= oldExpire {
			return ":0\r\n", nil
		}
		entry.ExpireAt = newExpire

	default:
		return "-ERR invalid expire option\r\n", fmt.Errorf("invalid expire option")
	}

	// Save updated entry
	setEntry(key, entry)

	// Log to AOF

	return ":1\r\n", nil
}

func cmdPERSIST(args []string) (string, error) {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'PERSIST' command\r\n", fmt.Errorf("wrong args")
	}

	success := PersistEntry(args[1])
	if success {
		return ":1\r\n", nil
	} else {
		return ":0\r\n", nil
	}
}

func cmdTTL(args []string) (string, error) {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'TTL' command\r\n", fmt.Errorf("wrong args")
	}

	// Check if key exists
	entry, exists := getEntry(args[1])
	if !exists {
		return ":-2\r\n", nil // key doesn't exist
	}

	if entry.ExpireAt == 0 {
		return ":-1\r\n", nil // key has no expiration
	}

	ttl := entry.ExpireAt - time.Now().Unix()
	if ttl < 0 {
		return ":-2\r\n", nil // key has expired
	}

	return ":" + strconv.FormatInt(ttl, 10) + "\r\n", nil
}

//HSET
func cmdHSET(args []string) (string, error) {
    // Minimum 1 field/value pair (HSET key f v)
    if len(args) < 4 || (len(args)-2)%2 != 0 {
        return "-ERR wrong number of arguments for 'HSET' command\r\n", fmt.Errorf("wrong args")
    }

    key := args[1]
    entry, exists := getEntry(key)

    var hash map[string]string

    if exists {
        if entry.Type != TypeHash {
            return "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                fmt.Errorf("wrong type")
        }
        hash = entry.Value.(map[string]string)
    } else {
        hash = make(map[string]string)
    }

    added := 0

    // Process field/value pairs
    for i := 2; i < len(args); i += 2 {
        field := args[i]
        value := args[i+1]

        _, existedBefore := hash[field]
        hash[field] = value

        if !existedBefore {
            added++
        }
    }

    // Build new Entry
    newEntry := Entry{
        Type:  TypeHash,
        Value: hash,
    }

    if exists {
        newEntry.ExpireAt = entry.ExpireAt // preserve TTL
    }

    setEntry(key, newEntry)

    // DO NOT LOG inside cmd-layer
    return ":" + strconv.Itoa(added) + "\r\n", nil
}

//HGET
func cmdHGET(args []string) (string, error) {
	if len(args) != 3 {
		return "-ERR wrong number of arguments for 'HGET' command\r\n", fmt.Errorf("wrong args")
	}

	key := args[1]
	
	entry , exists := getEntry(key)
	if !exists {
		return "$-1\r\n", nil
	}

	if entry.Type != TypeHash {
		return "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
			fmt.Errorf("wrong type")
	}

	hash := entry.Value.(map[string]string)
	field := args[2]

	value, fieldExists := hash[field]
	if !fieldExists {
		return "$-1\r\n", nil
	}

	resp := "$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"
	return resp, nil
}

//HDEL
func cmdHDEL(args []string) (string, error) {
	if len(args) < 3 {
		return "-ERR wrong number of arguments for 'HDEL' command\r\n", fmt.Errorf("wrong args")
	}

	key := args[1]
	entry, exists := getEntry(key)
	if !exists {
		return ":0\r\n", nil
	}

	if entry.Type != TypeHash {
		return "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
			fmt.Errorf("wrong type")
	}

	hash := entry.Value.(map[string]string)
	deleted := 0

	for _, field := range args[2:] {
		_, fieldExists := hash[field]
		if fieldExists {
			delete(hash, field)
			deleted++
		}
	}

	// Update the entry
	entry.Value = hash
	setEntry(key, entry)

	return ":" + strconv.Itoa(deleted) + "\r\n", nil
}

//HGETALL
func cmdHGETALL(args []string) (string, error) {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'HGETALL' command\r\n", fmt.Errorf("wrong args")
	}

	key := args[1]
	entry, exists := getEntry(key)
	if !exists {
		return "*0\r\n", nil
	}

	if entry.Type != TypeHash {
		return "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
			fmt.Errorf("wrong type")
	}

	hash := entry.Value.(map[string]string)
	count := len(hash) * 2

	var resp strings.Builder
	resp.WriteString("*" + strconv.Itoa(count) + "\r\n")

	for field, value := range hash {
		resp.WriteString("$" + strconv.Itoa(len(field)) + "\r\n" + field + "\r\n")
		resp.WriteString("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n")
	}

	return resp.String(), nil
}

//HEXISTS
func cmdHEXISTS(args []string) (string, error) {
	if len(args) != 3 {
		return "-ERR wrong number of arguments for 'HEXISTS' command\r\n", fmt.Errorf("wrong args")
	}

	key := args[1]
	entry, exists := getEntry(key)
	if !exists {
		return ":0\r\n", nil
	}

	if entry.Type != TypeHash {
		return "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
			fmt.Errorf("wrong type")
	}

	hash := entry.Value.(map[string]string)
	field := args[2]

	_, fieldExists := hash[field]
	if fieldExists {
		return ":1\r\n", nil
	} else {
		return ":0\r\n", nil
	}
}

//HLEN
func cmdHLEN(args []string) (string, error) {
	if len(args) != 2 {
		return "-ERR wrong number of arguments for 'HLEN' command\r\n", fmt.Errorf("wrong args")
	}

	key := args[1]
	entry, exists := getEntry(key)
	if !exists {
		return ":0\r\n", nil
	}

	if entry.Type != TypeHash {
		return "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
			fmt.Errorf("wrong type")
	}

	hash := entry.Value.(map[string]string)
	length := len(hash)

	return ":" + strconv.Itoa(length) + "\r\n", nil
}