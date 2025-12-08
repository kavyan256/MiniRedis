package main

import (
	"bufio" //in of tcp connection
	"fmt"   //printing logs
	"net" //tcp connection
	"strings" //string manipulation
)

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