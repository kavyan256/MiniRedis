package main

import (
	"sync"
)

var db = make(map[string]Entry)
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
	Type       EntryType
	Value 	   interface{}
	ExpireAt   int64
} 

type ZSet struct {
	Dict map[string]float64
	List []ZItem
}

type ZItem struct {
	Member string
	Score  float64
}


