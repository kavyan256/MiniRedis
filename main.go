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
		fmt.Println("-Error starting server:", err)
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
		return nil, fmt.Errorf("-ERR invalid RESP format.(* expected)")
	}

	numArgs, err := strconv.Atoi(line[1:])  // ASCII to int
	if( err != nil || numArgs <= 0 ){
		return nil, fmt.Errorf("-ERR invalid RESP length")
	}

	args := make([]string, numArgs)

	for i := 0 ; i<numArgs ; i++ {
		lenline, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("-ERR io error reading bulk header")
		}

		lenline = strings.TrimSpace(lenline)
		if len(lenline) == 0 || lenline[0] != '$' {
			return nil, fmt.Errorf("-ERR invalid RESP format. $ expected")
		}

		length, err := strconv.Atoi(lenline[1:])
		if err != nil {
			return nil, fmt.Errorf("-ERR invalid bulk length")
		}

		buff := make([]byte, length)
		if _ , err := io.ReadFull(reader, buff); err != nil {
			return nil, fmt.Errorf("-ERR io error reading bulk data")
		}

		crlf, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("-ERR protocol error: missing terminating CRLF")
		}
		if !strings.HasSuffix(crlf, "\r\n") {
			return nil, fmt.Errorf("-ERR protocol error: invalid terminating CRLF")
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
			conn.Write([]byte("-ERR protocol error: " + err.Error() + "\r\n"))
			return
		}

		command := strings.ToUpper(args[0])

		//done with parsing command
		//handle commands

		switch command {
			case "SET":
				if len(args) != 3 {
					conn.Write([]byte("-ERR wrong number of arguments for 'SET' command\r\n"))
					continue
				}
				//example "SET A 10"
				key := args[1]
				value := args[2]
				
				mu.Lock()
				store[key] = value
				mu.Unlock()
				
				conn.Write([]byte("+OK\r\n"))

				case "GET":
					if len(args) != 2 {
						conn.Write([]byte("-ERR wrong number of arguments for 'GET' command\r\n"))
						continue
					}

					mu.RLock()
					//example "GET A"
					key := args[1]
					value, exists := store[key]
					mu.RUnlock()
					
					if exists {
						conn.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"))
					} else {
						conn.Write([]byte("$-1\r\n"))
					}
				
				case "DEL":
					if len(args) != 2 {
						conn.Write([]byte("-ERR wrong number of arguments for 'DEL' command\r\n"))
						continue
					}
					
					//example "DEL A"
					key := args[1]

					mu.Lock()
					_, exists := store[key]
					if exists {
						delete(store, key)
						mu.Unlock()
						conn.Write([]byte(":1\r\n")) //1 indicates one key deleted
					} else {
						mu.Unlock()
						conn.Write([]byte(":0\r\n")) //0 indicates no key deleted
					}

				case "PING":
					if len(args) == 1 {
						conn.Write([]byte("+PONG\r\n"))
					} else if len(args) == 2 {
						// Echo back the message
						message := args[1]
						conn.Write([]byte("$" + strconv.Itoa(len(message)) + "\r\n" + message + "\r\n"))
					} else {
						conn.Write([]byte("-ERR wrong number of arguments for 'PING' command\r\n"))
					}

				case "ECHO":
					if len(args) != 2 {
						conn.Write([]byte("-ERR wrong number of arguments for 'ECHO' command\r\n"))
						continue
					}
					message := args[1]
					conn.Write([]byte("$" + strconv.Itoa(len(message)) + "\r\n" + message + "\r\n"))

				case "EXISTS":
					if len(args) != 2 {
						conn.Write([]byte("-ERR wrong number of arguments for 'EXISTS' command\r\n"))
						continue
					}
					//example "EXISTS A"
					count := 0
					for _, key := range args[1:] {
						mu.RLock()
						_, exists := store[key]
						mu.RUnlock()
						if exists {
							count++
						}
					}
					conn.Write([]byte(":" + strconv.Itoa(count) + "\r\n"))

				case "INCR":
					if len(args) != 2 {
						conn.Write([]byte("-ERR wrong number of arguments for 'INCR' command\r\n"))
						continue
					}
					//example "INCR A"
					key := args[1]

					mu.Lock()
					value, exists := store[key]
					if !exists {
						store[key] = "1"
						mu.Unlock()
						conn.Write([]byte(":1\r\n"))
						continue
					}
					
					intValue, err := strconv.Atoi(value)
					if err != nil {
						mu.Unlock()
						conn.Write([]byte("-ERR value is not an integer\r\n"))
						continue
					}
					
					intValue++
					store[key] = strconv.Itoa(intValue)
					mu.Unlock()
					conn.Write([]byte(":" + strconv.Itoa(intValue) + "\r\n"))

				case "DECR":
					if len(args) != 2 {
						conn.Write([]byte("-ERR wrong number of arguments for 'DECR' command\r\n"))
						continue
					}
					//example "DECR A"
					key := args[1]

					mu.Lock()
					value, exists := store[key]
					if !exists {
						store[key] = "-1"
						mu.Unlock()
						conn.Write([]byte(":-1\r\n"))
						continue
					}
					
					intValue, err := strconv.Atoi(value)
					if err != nil {
						mu.Unlock()
						conn.Write([]byte("-ERR value is not an integer\r\n"))
						continue
					}
					
					intValue--
					store[key] = strconv.Itoa(intValue)
					mu.Unlock()
					conn.Write([]byte(":" + strconv.Itoa(intValue) + "\r\n"))

				case "MGET":
					if len(args) < 2 {
						conn.Write([]byte("-ERR wrong number of arguments for 'MGET' command\r\n"))
						continue
					}

					numKeys := len(args) - 1
					conn.Write([]byte("*" + strconv.Itoa(numKeys) + "\r\n"))

					for _, key := range args[1:] {
						mu.RLock()
						value, exists := store[key]
						mu.RUnlock()
						if exists {
							conn.Write([]byte("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n"))
						} else {
							conn.Write([]byte("$(NIL)\r\n"))
						}
					}

				case "MSET":
					if len(args) < 3 || len(args[1:])%2 != 0 {
						conn.Write([]byte("-ERR wrong number of arguments for 'MSET' command\r\n"))
						continue
					}

					mu.Lock()
					for i := 1; i < len(args); i += 2 {
						key := args[i]
						value := args[i+1]
						store[key] = value
					}
					mu.Unlock()
					conn.Write([]byte("+OK\r\n"))

				case "FLUSHALL":
					if !( len(args) == 1 || (len(args) == 2) ){
						conn.Write([]byte("-ERR wrong number of arguments for 'FLUSHALL' command\r\n"))
						continue
					}
					
					mode := "SYNC"
					if len(args) == 2 {
						mode = strings.ToUpper(args[1])
					}

					switch mode {
						case "SYNC":
							mu.Lock()
							old := store
							store = make(map[string]string)
							mu.Unlock()
							
							go func(m map[string]string) {
								_ = m //simulating background deletion
							}(old)
							
							conn.Write([]byte("+OK\r\n"))
						
						case "ASYNC":
							go func() {
								mu.Lock()
								store = make(map[string]string)
								mu.Unlock()
							}()
							conn.Write([]byte("+OK\r\n"))
						
						default:
							conn.Write([]byte("-ERR invalid option for 'FLUSHALL' command\r\n"))
					}

				case "EXIT":
					conn.Write([]byte("+OK\r\n"))
					return //close connection
					

				
				default:
						conn.Write([]byte("-ERR something\r\n"))
				
				
		}
	
	}
}