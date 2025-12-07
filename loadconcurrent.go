package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

func send(cmd string, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", "localhost:6379") // FIX: correct syntax
	if err != nil {
		fmt.Printf("Connection error: %v\n", err)
		return
	}
	defer conn.Close()

	// FIX: use io.WriteString for proper RESP sending
	_, err = io.WriteString(conn, cmd)
	if err != nil {
		fmt.Printf("Write error: %v\n", err)
		return
	}

	r := bufio.NewReader(conn)

	// FIX: read RESP response properly
	msg, err := r.ReadString('\n')
	if err != nil {
		fmt.Printf("Read error: %v\n", err)
		return
	}

	// FIX: trim CRLF and validate response
	response := strings.TrimSpace(msg)
	if response == "OK" {
		fmt.Printf("✓ SET succeeded\n")
	} else if strings.HasPrefix(response, "ERR") {
		fmt.Printf("✗ Error: %s\n", response)
	} else {
		fmt.Printf("? Unknown response: %s\n", response)
	}
}

func main() {
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < 5; i++ {
		wg.Add(1)
		// RESP format: *3\r\n$3\r\nSET\r\n$1\r\nA\r\n$3\r\n100\r\n
		go send("*3\r\n$3\r\nSET\r\n$1\r\nA\r\n$3\r\n100\r\n", &wg)
	}

	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("\nCompleted 5 concurrent SETs in %v\n", elapsed)
	// FIX: verify final value
	conn, _ := net.Dial("tcp", "localhost:6379")
	io.WriteString(conn, "*2\r\n$3\r\nGET\r\n$1\r\nA\r\n")
	r := bufio.NewReader(conn)
	msg, _ := r.ReadString('\n')
	fmt.Printf("Final value of A: %s\n", strings.TrimSpace(msg))
	conn.Close()
}
