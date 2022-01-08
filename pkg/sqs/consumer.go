package sqs

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/patriciabonaldy/big_queue/pkg"
	"log"
)

type consumer struct {
	queue   string
	timeout int64
	svc     *sqs.SQS
}

func NewConsumer(region, key, secret, quueue, endpoint string, timeout int64) (pkg.Consumer, error) {
	config := aws.NewConfig()
	config.WithRegion(region).WithCredentials(credentials.NewStaticCredentialsFromCreds(credentials.Value{
		AccessKeyID:     key,
		SecretAccessKey: secret,
	})).WithEndpoint(endpoint)
	config = config.WithDisableSSL(true)

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}

	svc := sqs.New(sess)

	return &consumer{queue: quueue, timeout: timeout, svc: svc}, nil
}

func (c *consumer) Read(message chan interface{}, chErr chan error) {
	// Get URL of queue
	urlResult, err := c.getQueueURL()
	if err != nil {
		fmt.Println("Got an error getting the queue URL:")
		fmt.Println(err)
		return
	}

	// snippet-start:[sqs.go.receive_message.url]
	queueURL := urlResult.QueueUrl
	// snippet-end:[sqs.go.receive_message.url]

	msgResult, err := c.getMessages(queueURL)
	if err != nil {
		chErr <- err
		return
	}

	message <- msgResult
}

func (c *consumer) getQueueURL() (*sqs.GetQueueUrlOutput, error) {
	// snippet-start:[sqs.go.receive_messages.queue_url]
	urlResult, err := c.svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &c.queue,
	})
	// snippet-end:[sqs.go.receive_messages.queue_url]
	if err != nil {
		return nil, err
	}

	return urlResult, nil
}

func (c *consumer) getMessages(queueURL *string) (*sqs.ReceiveMessageOutput, error) {
	// snippet-start:[sqs.go.receive_messages.call]
	msgResult, err := c.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            queueURL,
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   &c.timeout,
	})
	// snippet-end:[sqs.go.receive_messages.call]
	if err != nil {
		return nil, err
	}

	return msgResult, nil
}

func (c *consumer) Ack(queueURL string, messages interface{}) (int64, error) {
	var count int64
	m, ok := messages.(*sqs.ReceiveMessageOutput)
	if !ok {
		return count, fmt.Errorf("invalid message")
	}

	for _, m := range m.Messages {
		_, err := c.svc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      aws.String(queueURL),
			ReceiptHandle: m.ReceiptHandle,
		})
		// snippet-end:[sqs.go.delete_message.call]
		if err != nil {
			return count, err
		}

		count++
		log.Printf("[WORKER][INFO][DELETE] Message ID: %s\n", *m.MessageId)
	}

	return count, nil
}
