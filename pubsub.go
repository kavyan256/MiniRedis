package main

import (
	"net"
	"strconv"
	"sync"
)

//chSet is the set of channels a client is subscribed to
//Set is the set of client connections subscribed to a channel
	
var (
	subsMu			sync.RWMutex
	subs			= make(map[string]map[net.Conn]struct{}) // channel -> set of client connections
	connectedChannels    = make(map[net.Conn]map[string]struct{}) // client connection -> set of subscribed channels
	writeMuPerConn	= make(map[net.Conn]*sync.Mutex)          // mutex per connection for writing
	writeMuPerConnMu	sync.Mutex
)

func ensureWriteMu(conn net.Conn) *sync.Mutex {
	writeMuPerConnMu.Lock()
	defer writeMuPerConnMu.Unlock()

	mu, exists := writeMuPerConn[conn]
	if !exists {
		mu = &sync.Mutex{}
		writeMuPerConn[conn] = mu
	}
	return mu
}

func buildArrayRESP(parts []string) string {		//array to RESP format
	resp := "*" + strconv.Itoa(len(parts)) + "\r\n"
	for _, p := range parts {
		resp += "$" + strconv.Itoa(len(p)) + "\r\n" + p + "\r\n"
	}
	return resp
}

func subscribeClient(conn net.Conn, channels []string) {             //client subscribes to channels
	subsMu.Lock()
	defer subsMu.Unlock()

	for _, channel := range channels {								//search for requested channels one by one

		set, exists := subs[channel]										//does said channel exists? has subscribers
		if !exists {
			set = make(map[net.Conn]struct{})	//set is a map of connections, if not exists create new set of channels			
			subs[channel] = set					//add new channel created to subs map
		}
		_ , alreadySubscribed := set[conn]		//check if the client is already subscribed to said channel
		set[conn] = struct{}{}       //pattern for sets since go doesn't have sets only kv pairs, dummy value

		chSet, exists := connectedChannels[conn]     //channels this connection listens to
		if !exists {
			chSet = make(map[string]struct{})
			connectedChannels[conn] = chSet
		}
		chSet[channel] = struct{}{}   //add channel to the set of channels this connection listens to

		num := strconv.Itoa(len(set))
		if !alreadySubscribed {
			ack := buildArrayRESP([]string{"subscribe", channel, num})	//subscription established ack
			mu := ensureWriteMu(conn)
			go func(b string) {
				mu.Lock()
				defer mu.Unlock()
				conn.Write([]byte(b))
			}(ack)
		}
	}
}

func unsubscribeClient(conn net.Conn, channels []string) {
	subsMu.Lock()
	defer subsMu.Unlock()

	if len(channels) == 0 { 		//if no channels specified, unsubscribe from all
		
		chSet, exists := connectedChannels[conn]
		if !exists {
			return
		}

		for channel := range chSet {
			if subSet, exists := subs[channel]; exists {
				delete(subSet, conn)

				if len(subSet) == 0 {
					delete(subs, channel)
				}

				ack := buildArrayRESP([]string{"unsubscribe", channel, strconv.Itoa(len(subSet))})
				mu := ensureWriteMu(conn)
				go func(b string) {
					mu.Lock()
					defer mu.Unlock()
					conn.Write([]byte(b))
				}(ack)
			}
		}
		delete(connectedChannels, conn)
		return
	}

	//unsubscribe from specified channels
	for _, channel := range channels {
		if subSet, exists := subs[channel]; exists {
			_, subscribed := subSet[conn]
			if subscribed {
				delete(subSet, conn)

				if len(subSet) == 0 {
					delete(subs, channel)
				}

				ack := buildArrayRESP([]string{"unsubscribe", channel, strconv.Itoa(len(subSet))})
				mu := ensureWriteMu(conn)
				go func(b string) {
					mu.Lock()
					defer mu.Unlock()
					conn.Write([]byte(b))
				}(ack)
			}
		}
		chSet, exists := connectedChannels[conn]
		if exists {
			delete(chSet, channel)
			if len(chSet) == 0 {
				delete(connectedChannels, conn)
			}
		}
	}
}