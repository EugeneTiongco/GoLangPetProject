package sqs_receive_message

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSLocalstack struct {
	region         string
	queueName      string
	timeoutSeconds int
	localstackURL  string
}

type Message struct {
	Id     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

// setup the data by collecting from args parameter
func (s *SQSLocalstack) Setup() {
	region := flag.String("r", "us-east-1", "The region of localstack. Default: us-east-1")
	queue := flag.String("q", "", "The name of the queue")
	timeout := flag.Int("t", 5, "How long, in seconds, that the message is hidden from others")
	localstackURL := flag.String("u", "http://localhost:4566", "The Localstack url. Default : http://localhost:4566")
	flag.Parse()

	if *queue == "" {
		fmt.Println("You must supply the name of a queue (-q QUEUE)")
		return
	}

	if *timeout < 0 {
		*timeout = 0
	}

	if *timeout > 12*60*60 {
		*timeout = 12 * 60 * 60
	}
	s.region = *region
	s.queueName = *queue
	s.timeoutSeconds = *timeout
	s.localstackURL = *localstackURL
}

func (s *SQSLocalstack) ReceiveSQSMessage() error {
	ctx := context.TODO()

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		//if service == dynamodb.ServiceID && region == "us-west-2" {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           "http://localhost:4566",
			SigningRegion: "us-east-1",
		}, nil
		//}
		// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
		//return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	cfg, err := config.LoadDefaultConfig(ctx, config.WithEndpointResolverWithOptions(customResolver), config.WithRegion("us-east-1"))

	if err != nil {
		panic("configuration error, " + err.Error())
	}

	client := sqs.NewFromConfig(cfg)

	gQInput := &sqs.GetQueueUrlInput{
		QueueName: &s.queueName,
	}

	// Get URL of queue
	urlResult, err := client.GetQueueUrl(ctx, gQInput)
	if err != nil {
		fmt.Println("Got an error getting the queue URL:", err)
		return err
	}

	queueURL := urlResult.QueueUrl

	gMInput := &sqs.ReceiveMessageInput{
		MessageAttributeNames: []string{
			string(types.QueueAttributeNameAll),
		},
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   int32(s.timeoutSeconds),
	}

	msgResult, err := client.ReceiveMessage(ctx, gMInput)
	if err != nil {
		fmt.Println("Got an error receiving messages:")
		fmt.Println(err)
		return err
	}

	if msgResult != nil {
		messages := msgResult.Messages
		if len(messages) > 0 {
			var result Message
			msg := messages[0]
			fmt.Println("Message received")
			fmt.Println("Message ID:     " + *msg.MessageId)
			fmt.Println("Message Body: " + *msg.Body)

			err1 := json.Unmarshal([]byte(*msg.Body), &result)
			if err1 != nil {
				fmt.Println("failed to unmarshal message")
			}

			fmt.Println("Unmarshal message: ")
			fmt.Println("ID: " + result.Id)
			fmt.Println("Name: " + result.Name)
			fmt.Println("Status: " + result.Status)

			//delete the message
			dMInput := &sqs.DeleteMessageInput{
				QueueUrl:      queueURL,
				ReceiptHandle: msg.ReceiptHandle,
			}
			_, err := client.DeleteMessage(ctx, dMInput)
			if err != nil {
				fmt.Println("failed to delete message", err)
				return err
			}
			fmt.Println("Message succesfully deleted")
		}
	}
	return nil
}

/*
func main() {
	sqsLocalstack := SQSLocalstack{}

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
*/
