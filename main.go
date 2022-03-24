package main

import (
	"fmt"
	"golang-petproject/elastic_search"
	"golang-petproject/sqs_receive_message"
	"os"
	"os/signal"
)

func main() {
	elastic_search.InitializeElasticSearch()

	sqsLocalstack := sqs_receive_message.SQSLocalstack{}

	sqsLocalstack.Setup()

	fmt.Println("Start listen incoming message")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	for {
		select {
		case <-quit:
			fmt.Println("app stopped")
			os.Exit(0)
		default:
			err := sqsLocalstack.ReceiveSQSMessage()
			if err != nil {
				os.Exit(1)
			}
		}
	}

}
