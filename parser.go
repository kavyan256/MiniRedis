package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

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
