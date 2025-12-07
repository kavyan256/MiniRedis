package main

import (
	"bufio"   //in of tcp connection
	"fmt"     //printing logs
	"net"     //tcp connection
	"strings" //string manipulation
	"sync"
)

var store = make(map[string]string)        //global kv store 
var mu sync.RWMutex				//mutex for concurrent access

//entry point of program & start the tcp server
func main() {
	listener, err := net.Listen("tcp", ":6379") //listen on port 8080
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}

	fmt.Println("Server(Mini-Redis) is listening on port 6379 ...")

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
		line, err := reader.ReadString('\n') //read until newline
		if err != nil {
			return //exit on error
		}

		parts := strings.Fields(strings.TrimSpace(line))
		if len(parts) == 0 {
			continue
		}

		command := strings.ToUpper(parts[0])

		//done with parsing command
		//handle commands

		switch command {
			case "SET":
				if len(parts) != 3 {
					conn.Write([]byte("ERR wrong number of arguments for 'SET' command\r\n"))
				}
				//example "SET A 10"
				key := parts[1]
				value := parts[2]
				
				mu.Lock()
				defer mu.Unlock()
				store[key] = value
				
				conn.Write([]byte("OK\r\n"))

				case "GET":
					if len(parts) != 2 {
						conn.Write([]byte("ERR wrong number of arguments for 'GET' command\r\n"))
					}

					mu.RLock()
					defer mu.RUnlock()
					//example "GET A"
					key := parts[1]
					value, exists := store[key]
					
					if exists {
						conn.Write([]byte(value + "\r\n"))
					} else {
						conn.Write([]byte("nil\r\n"))
					}
				
				case "DEL":
					if len(parts) != 2 {
						conn.Write([]byte("ERR wrong number of arguments for 'DEL' command\r\n"))
					}
					
					//example "DEL A"
					key := parts[1]
					_, exists := store[key]

					if exists {
						mu.Lock()
						defer mu.Unlock()
						delete(store, key)
						conn.Write([]byte("1\r\n")) //1 indicates one key deleted
					} else {
						conn.Write([]byte("0\r\n")) //0 indicates no key deleted
					}
					
					default:
						conn.Write([]byte("ERR unknown command\r\n"))
		}
	
	}
}