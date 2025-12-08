package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const AOFFileName = "appendonly.aof"

var (
	aofFile   *os.File
	aofWriter *bufio.Writer
)

// InitAOF opens or creates the AOF file
func InitAOF() error {
	f, err := os.OpenFile("appendonly.aof",
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	aofFile = f
	aofWriter = bufio.NewWriter(aofFile)

	fmt.Println("[AOF] AOF initialized")

	return nil
}

// buildRESPCommand creates RESP-formatted command
func buildRESPCommand(cmd string, args []string) string {
	parts := append([]string{cmd}, args...)

	var sb strings.Builder
	sb.WriteString("*")
	sb.WriteString(strconv.Itoa(len(parts)))
	sb.WriteString("\r\n")

	for _, p := range parts {
		sb.WriteString("$")
		sb.WriteString(strconv.Itoa(len(p)))
		sb.WriteString("\r\n")
		sb.WriteString(p)
		sb.WriteString("\r\n")
	}

	return sb.String()
}

// FlushAOF writes buffer to disk
func FlushAOF() error {
	aofMu.Lock()
	defer aofMu.Unlock()

	if aofWriter == nil {
		return nil
	}

	if err := aofWriter.Flush(); err != nil {
		fmt.Printf("[AOF] Error flushing: %v\n", err)
		return err
	}

	if err := aofFile.Sync(); err != nil {
		fmt.Printf("[AOF] Error syncing: %v\n", err)
		return err
	}

	return nil
}

// CloseAOF closes the AOF file gracefully
func CloseAOF() error {
	aofMu.Lock()
	defer aofMu.Unlock()

	if aofFile == nil {
		return nil
	}

	if err := aofWriter.Flush(); err != nil {
		return err
	}

	if err := aofFile.Sync(); err != nil {
		return err
	}

	return aofFile.Close()
}

// BackgroundFsync periodically flushes AOF to disk
func BackgroundAOFFsync() {
	ticker := time.NewTicker(time.Second)

	for range ticker.C {
		aofMu.Lock()
		if aofWriter != nil {
			aofWriter.Flush()
			aofFile.Sync()
		}
		aofMu.Unlock()
	}
}

// ReplayAOF reads and replays commands from the AOF file
func ReplayAOF() {
	if _, err := os.Stat(AOFFileName); os.IsNotExist(err) {
		fmt.Println("[Replay] No AOF file found, starting fresh")
		return
	}

	file, err := os.Open(AOFFileName)
	if err != nil {
		fmt.Printf("[Replay] Error opening AOF: %v\n", err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	count := 0

	isReplayingAOF = true
	defer func() { isReplayingAOF = false }()

	for {
		args, err := parseResp(reader)
		if err != nil {
			break // EOF or corrupted
		}

		if len(args) == 0 {
			continue
		}

		// Execute command directly on store
		replayCommand(args)
		count++
	}

	fmt.Printf("[Replay] Replayed %d commands from AOF\n", count)
}

// replayCommand executes a command during AOF replay
func replayCommand(args []string) {
	if len(args) == 0 {
		return
	}

	cmd := strings.ToUpper(args[0])

	switch cmd {
	case "SET":
		if len(args) == 3 {
			mu.Lock()
			db[args[1]] = Entry{
				Type:  TypeString,
				Value: args[2],
			}
			mu.Unlock()
		}

	case "DEL":
		if len(args) == 2 {
			mu.Lock()
			delete(db, args[1])
			mu.Unlock()
		}

	case "MSET":
		if len(args) >= 3 && len(args[1:])%2 == 0 {
			mu.Lock()
			for i := 1; i < len(args); i += 2 {
				db[args[i]] = Entry{
					Type:  TypeString,
					Value: args[i+1],
				}
			}
			mu.Unlock()
		}

	case "EXPIRE":
		if len(args) == 3 {
			mu.Lock()
			if entry, exists := db[args[1]]; exists {
				seconds, _ := strconv.ParseInt(args[2], 10, 64)
				entry.ExpireAt = time.Now().Unix() + seconds
				db[args[1]] = entry
			}
			mu.Unlock()
		}

	case "PERSIST":
		if len(args) == 2 {
			mu.Lock()
			if entry, exists := db[args[1]]; exists {
				entry.ExpireAt = 0
				db[args[1]] = entry
			}
			mu.Unlock()
		}

	case "FLUSHALL":
		mu.Lock()
		db = make(map[string]Entry)
		mu.Unlock()
	}
}
