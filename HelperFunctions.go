package main

import (
	"fmt"
	"time"
)

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

func getEntry(key string) (Entry, bool) {
	mu.RLock()
	entry, exists := db[key]
	mu.RUnlock()
	
	if !exists {
		return Entry{}, false
	}

	// Check expiration
	if entry.ExpireAt != 0 && entry.ExpireAt <= time.Now().Unix() {
		
		mu.Lock()
		delete(db, key)
		mu.Unlock()
		return Entry{}, false
	}
	return entry, true
}

func setEntry(key string, entry Entry) {
	mu.Lock()
	db[key] = entry
	mu.Unlock()
}

func deleteEntry(key string) bool {
    mu.Lock()
    _, exists := db[key]
    if exists {
        delete(db, key)
    }
    mu.Unlock()
    return exists
}

func setExpiration(key string, expireAt int64) bool {
	mu.Lock()
	entry, exists := db[key]
	if exists {
		entry.ExpireAt = expireAt
		db[key] = entry
	}
	mu.Unlock()
	return exists
}

func PersistEntry(key string) bool {
    // Use GetEntry which does lazy expiration and locking
    e, ok := getEntry(key)
    if !ok {
        return false
    }

    // Remove TTL
    e.ExpireAt = 0

    // Save back (SetEntry does locking)
    setEntry(key, e)
    return true
}