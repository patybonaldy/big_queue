package sqs

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConsumer_Read(t *testing.T) {
	quueue := "my-queue"
	sqsConsumer, err := NewConsumer("us-east-1",
		"223344",
		"wJalrXUtTHISI/DYNAMODB/bPxRfiCYEXAMPLEKEY",
		quueue,
		"http://localhost:9324",
		int64(5))
	require.NoError(t, err)

	message := make(chan interface{})
	errSqs := make(chan error)
	go sqsConsumer.Read(message, errSqs)
	go func() {
		err = <-errSqs
		require.NoError(t, err)
	}()

	msg := <-message
	msg2, _ := msg.(*sqs.ReceiveMessageOutput)
	fmt.Println(msg2)
	if msg2.Messages != nil {
		sqsConsumer.Ack(msg)
	}
}
