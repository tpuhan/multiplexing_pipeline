//go:build !plugin

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type logEntry struct {
	Time string `json:"Time"`
	Text string `json:"Text"`
}

func writeToFirstLog() {
	file, err := os.OpenFile("logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}
	logger := log.New(file, "", 0)

	for {
		curr := logEntry{
			Time: time.Now().Format(time.RFC3339),
			Text: "Hello World!",
		}
		entry, err := json.Marshal(curr)
		if err != nil {
			fmt.Println("Error marshaling JSON:", err)
			continue
		}

		logger.Println(string(entry))
		time.Sleep(time.Second)
	}
}

func writeToSecondLog() {
	file, err := os.OpenFile("another_logfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Error opening file:", err)
		os.Exit(1)
	}
	logger := log.New(file, "", 0)

	for {
		curr := logEntry{
			Time: time.Now().Format(time.RFC3339),
			Text: "Until we meet again!",
		}
		entry, err := json.Marshal(curr)
		if err != nil {
			fmt.Println("Error marshaling JSON:", err)
			continue
		}

		logger.Println(string(entry))
		time.Sleep(time.Second)
	}
}

func main() {
	go writeToFirstLog()
	go writeToSecondLog()

	// Prevent main from exiting immediately
	select {}
}
