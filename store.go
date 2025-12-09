package main

import (
	"sync"
)

const NumDatabases = 16

var databases [NumDatabases]map[string]Entry

var db map[string]Entry

func init() {
	for i := 0; i < NumDatabases; i++ {
		databases[i] = make(map[string]Entry)
	}
	db = databases[0]
}

var usedMemory int64 = 0
var lastAccess = map[string]int64{}

var mu sync.RWMutex
var aofMu sync.Mutex

var isReplayingAOF = false

type EntryType int

const (
	TypeString EntryType = iota
	TypeInt
	TypeList
	TypeSet
	TypeHash
	TypeZSet
)

type Entry struct {
	Type     EntryType
	Value    interface{}
	ExpireAt int64
}

type ZSet struct {
	Dict map[string]float64
	List []ZItem
}

type ZItem struct {
	Member string
	Score  float64
}
