package main

import (
	"net"
	"sync"
)

var currentConnMu sync.RWMutex
var currentConn net.Conn

func SetCurrentConn(conn net.Conn) {
	currentConnMu.Lock()
	currentConn = conn
	currentConnMu.Unlock()
}

func GetCurrentConn() net.Conn {
	currentConnMu.RLock()
	defer currentConnMu.RUnlock()
	return currentConn
}