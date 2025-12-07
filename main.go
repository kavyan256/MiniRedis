package main

import (
	"bufio" //in of tcp connection
	"fmt"   //printing logs
	"io"
	"net" //tcp connection
	"strconv"
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

func parseResp(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSpace(line)
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("invalid RESP format.(* expected)")
	}

	numArgs, err := strconv.Atoi(line[1:])  // ASCII to int
	if( err != nil || numArgs <= 0 ){
		return nil, fmt.Errorf("invalid RESP length")
	}

	args := make([]string, numArgs)

	for i := 0 ; i<numArgs ; i++ {
		lenline, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("io error reading bulk header")
		}

		lenline = strings.TrimSpace(lenline)
		if len(lenline) == 0 || lenline[0] != '$' {
			return nil, fmt.Errorf("invalid RESP format. $ expected")
		}

		length, err := strconv.Atoi(lenline[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk length")
		}

		buff := make([]byte, length)
		if _ , err := io.ReadFull(reader, buff); err != nil {
			return nil, fmt.Errorf("io error reading bulk data")
		}

		crlf, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("protocol error: missing terminating CRLF")
		}
		if !strings.HasSuffix(crlf, "\r\n") {
			return nil, fmt.Errorf("protocol error: invalid terminating CRLF")
		}
		
		args[i] = string(buff)
	}

	return args, nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close() //close connection when function exits

	fmt.Println("New client connected:", conn.RemoteAddr())

	reader := bufio.NewReader(conn) //create a buffered reader

	for {
		args, err := parseResp(reader)
		if err != nil {
			conn.Write([]byte("ERR protocol error: " + err.Error() + "\r\n"))
			return
		}

		command := strings.ToUpper(args[0])

		//done with parsing command
		//handle commands

		switch command {
			case "SET":
				if len(args) != 3 {
					conn.Write([]byte("ERR wrong number of arguments for 'SET' command\r\n"))
					continue
				}
				//example "SET A 10"
				key := args[1]
				value := args[2]
				
				mu.Lock()
				store[key] = value
				mu.Unlock()
				
				conn.Write([]byte("OK\r\n"))

				case "GET":
					if len(args) != 2 {
						conn.Write([]byte("ERR wrong number of arguments for 'GET' command\r\n"))
						continue
					}

					mu.RLock()
					//example "GET A"
					key := args[1]
					value, exists := store[key]
					mu.RUnlock()
					
					if exists {
						conn.Write([]byte(value + "\r\n"))
					} else {
						conn.Write([]byte("nil\r\n"))
					}
				
				case "DEL":
					if len(args) != 2 {
						conn.Write([]byte("ERR wrong number of arguments for 'DEL' command\r\n"))
						continue
					}
					
					//example "DEL A"
					key := args[1]

					mu.Lock()
					_, exists := store[key]
					if exists {
						delete(store, key)
						mu.Unlock()
						conn.Write([]byte("1(delete success)\r\n")) //1 indicates one key deleted
					} else {
						mu.Unlock()
						conn.Write([]byte("0(delete failed)\r\n")) //0 indicates no key deleted
					}
					default:
						conn.Write([]byte("ERR unknown command\r\n"))
		}
	
	}
}