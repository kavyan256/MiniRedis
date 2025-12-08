package main

import (
	"bufio" //in of tcp connection
	"fmt"   //printing logs
	"net" //tcp connection
	"strings" //string manipulation
	"time" //for janitor ticker
)

//entry point of program & start the tcp server
func main() {
	listener, err := net.Listen("tcp", ":6379") //listen on port 8080
	if err != nil {
		fmt.Println("-Error starting server:", err)
		return
	}

	fmt.Println("Server(Mini-Redis) is listening on port 6379 ...")

	go startJanitor() //start expiration janitor

	//infinite loop to accept incoming clients
	for {
		conn, err := listener.Accept() //accept incoming connection
		if err != nil {
			continue
		}
		go handleConnection(conn) //handle connection concurrently
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close() //close connection when function exits

	fmt.Println("New client connected:", conn.RemoteAddr())

	reader := bufio.NewReader(conn) //create a buffered reader

	for {
		args, err := parseResp(reader)
		if err != nil {
			conn.Write([]byte("-ERR protocol error: " + err.Error() + "\r\n"))
			return
		}

		command := strings.ToUpper(args[0])

		//done with parsing command
		//handle commands
		handleCommand(conn, command, args)
	
	}
}

func startJanitor() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			cleanExpiredKeys()
		}
	}()
}

func cleanExpiredKeys() {
	var keysToDelete []string

	// Find expired keys (read-lock expirations)
	mu.RLock()
	for key := range expirations {
		if isExpired(key) {
			keysToDelete = append(keysToDelete, key)
		}
	}
	mu.RUnlock()

	// Delete from store
	if len(keysToDelete) > 0 {
		mu.Lock()
		for _, key := range keysToDelete {
			delete(store, key)
		}
		mu.Unlock()

		// Delete from expirations
		mu.Lock()
		for _, key := range keysToDelete {
			delete(expirations, key)
		}
		mu.Unlock()
		fmt.Printf("[Janitor] Cleaned %d expired keys\n", len(keysToDelete))
	}
}


