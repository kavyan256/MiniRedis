package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"
	"strings"
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