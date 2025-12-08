package main

import (
	"sync"
)

var store = make(map[string]string)
var expirations = make(map[string]int64)

var mu sync.RWMutex
var aofMu sync.RWMutex

var isReplayingAOF = false

//Expiration cleanup janitor
