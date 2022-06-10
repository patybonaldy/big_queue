package sqs

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
)

type consumer struct {
	config  *aws.Config
	queue   string
	timeout int64
}

func NewConsumer(region, key, secret, quueue, endpoint string, timeout int64) (*consumer, error) {
	config := aws.NewConfig()
	config.WithRegion(region).WithCredentials(credentials.NewStaticCredentialsFromCreds(credentials.Value{
		AccessKeyID:     key,
		SecretAccessKey: secret,
	})).WithEndpoint(endpoint)
	config = config.WithDisableSSL(true)

	return &consumer{config: config, queue: quueue, timeout: timeout}, nil
}

func (c *consumer) createSQSClient() (*sqs.SQS, error) {
	sess, err := session.NewSession(c.config)
	if err != nil {
		return nil, err
	}

	svc := sqs.New(sess)
	return svc, nil
}

func (c *consumer) Read(message chan interface{}, chErr chan error) {
	// Get URL of queue
	urlResult, err := c.getQueueURL()
	if err != nil {
		fmt.Println("Got an error getting the queue URL:")
		fmt.Println(err)
		return
	}

	queueURL := urlResult.QueueUrl
	msgResult, err := c.getMessages(queueURL)
	if err != nil {
		chErr <- err
		return
	}

	message <- msgResult
}

func (c *consumer) getQueueURL() (*sqs.GetQueueUrlOutput, error) {
	// snippet-start:[sqs.go.receive_messages.queue_url]
	svc, err := c.createSQSClient()
	if err != nil {
		return nil, err
	}

	urlResult, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &c.queue,
	})
	// snippet-end:[sqs.go.receive_messages.queue_url]
	if err != nil {
		return nil, err
	}

	return urlResult, nil
}

func (c *consumer) getMessages(queueURL *string) (*sqs.ReceiveMessageOutput, error) {
	svc, err := c.createSQSClient()
	if err != nil {
		return nil, err
	}

	msgResult, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
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

func (c *consumer) Ack(messages interface{}) (int64, error) {
	var count int64
	m, ok := messages.(*sqs.ReceiveMessageOutput)
	if !ok {
		return count, fmt.Errorf("invalid message")
	}

	svc, err := c.createSQSClient()
	if err != nil {
		return count, err
	}

	urlResult, err := c.getQueueURL()
	if err != nil {
		fmt.Println("Got an error getting the queue URL:")
		fmt.Println(err)
		return count, err
	}

	queueURL := urlResult.QueueUrl
	for _, msg := range m.Messages {
		_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      queueURL,
			ReceiptHandle: msg.ReceiptHandle,
		})
		if err != nil {
			return count, err
		}

		count++
		log.Printf("[WORKER][INFO][DELETE] Message ID: %s\n", *msg.MessageId)
	}

	return count, nil
}
