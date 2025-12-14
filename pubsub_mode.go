package main

import (
	"net"
	"sync"
)

var (
	pubsubModeMu	sync.RWMutex
	pubsubMode     =make(map[net.Conn]bool)
)