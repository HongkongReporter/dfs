package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide host:port.")
		return
	}

	CONNECT := arguments[1]
	for {
		// connect...
		c, err := net.Dial("tcp", CONNECT)
		if err != nil {
			fmt.Println(err)
			return
		}
		// reading until it hits the \n
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Please enter <Command> <Key> <value>\r\n")
		fmt.Print(">> ")
		text, _ := reader.ReadString('\n')
		n, err := c.Write([]byte(text))
		if len(text+"\r\n") < n || err != nil {
			log.Println(">> Server disconnected...")
			return
		}
		if strings.TrimSpace(string(text)) == "exit" {
			fmt.Println("TCP client exiting...")
			return
		}
		if strings.TrimSpace(string(text)) == "kill" {
			fmt.Println("Server and TCP client exiting...")
			return
		}
		ServerResponse := bufio.NewReader(c)
		message, _ := ServerResponse.ReadString('\n')
		switch err {
		case nil:
			log.Println(message)
		case io.EOF:
			log.Println("server closed the connection")
			return
		default:
			log.Printf("server error: %v\n", err)
			return
		}

	}
}
