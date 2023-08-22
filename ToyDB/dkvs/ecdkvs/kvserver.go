package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please enter port number.")
		return
	}
	mydir, err := os.Getwd()
	mydir += "/data"
	if err != nil {
		log.Println(">> Cannot find disk access!")
		return
	}
	PORT := arguments[1]
	OpenAndListen(mydir, PORT, 2)
}
