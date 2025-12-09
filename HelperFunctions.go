package main

import (
	"fmt"
	"sort"
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

func getEntry(key string, selectedDB *int) (Entry, bool) {
	mu.RLock()
	entry, exists := databases[*selectedDB][key]
	mu.RUnlock()
	
	if !exists {
		return Entry{}, false
	}

	// Check expiration
	if entry.ExpireAt != 0 && entry.ExpireAt <= time.Now().Unix() {
		
		mu.Lock()
		delete(databases[*selectedDB], key)
		mu.Unlock()
		return Entry{}, false
	}
	return entry, true
}

func setEntry(key string, entry Entry, selectedDB *int) {
	mu.Lock()
	databases[*selectedDB][key] = entry
	mu.Unlock()
}

func deleteEntry(key string, selectedDB *int) bool {
    mu.Lock()
    _, exists := databases[*selectedDB][key]
    if exists {
        delete(databases[*selectedDB], key)
    }
    mu.Unlock()
    return exists
}

func PersistEntry(key string, selectedDB *int) bool {
    // Use GetEntry which does lazy expiration and locking
    entry, ok := getEntry(key, selectedDB)
    if !ok {
        return false
    }

    // Remove TTL
    entry.ExpireAt = 0

    // Save back (SetEntry does locking)
    setEntry(key, entry, selectedDB)
    return true
}

func getHash(key string, selectedDB *int) (map[string]string) {
	entry, exists := getEntry(key, selectedDB)
	if exists {
		if entry.Type != TypeHash {
			return nil
		}
		return entry.Value.(map[string]string)
	}

	h := make(map[string]string)
	setEntry(key, Entry{Type: TypeHash, Value: h}, selectedDB)
	return h
}

func zsetUpdate(z *ZSet, member string, score float64 ) {
    // Remove old entry
    for i, item := range z.List {
        if item.Member == member {
            z.List = append(z.List[:i], z.List[i+1:]...)
            break
        }
    }

    // Insert new item
    z.List = append(z.List, ZItem{Member: member, Score: score})

    // Sort by score
    sort.Slice(z.List, func(i, j int) bool {
        return z.List[i].Score < z.List[j].Score
    })
}
