package main

import (
	"fmt"
)

func WriteKey(key, value string) (oldValue string, existed bool) {
	oldValue, existed = store[key]
	store[key] = value
	delete(expirations, key)
	return oldValue, existed
}

func LogCommand(cmd string, args []string) error {
	if isReplayingAOF {
		return nil // Skip logging during replay
	}

	aofMu.Lock()
	defer aofMu.Unlock()

	if aofWriter == nil {
		return fmt.Errorf("AOF writer not initialized")
	}

	resp := buildRESPCommand(cmd, args)

	// Write to buffer
	_, err := aofWriter.WriteString(resp)
	if err != nil {
		fmt.Printf("[AOF] Error writing command: %v\n", err)
		return err
	}

	// Don't flush here - let BackgroundFsync handle it
	return nil
}
