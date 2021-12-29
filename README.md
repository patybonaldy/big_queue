# Big Queue on Go

This is a simple big queue and implementation in kafka, rabbit and aws sqs.


### Publish in a topic in kafka:
Use NewPublisher method to create an instance and call Publish method
```go
var (
    brokers = os.Getenv("KAFKA_BROKERS")
    topic   = os.Getenv("KAFKA_TOPIC")
)

publisher := kafka.NewPublisher(strings.Split(brokers, ","), topic)
message := pkg.NewSystemMessage("sending msg")

if err := publisher.Publish(context.Background(), message); err != nil {
    // TODO: something
}

```

### Consume in a topic in kafka:
Use NewPublisher method to create an instance and call Publish method
```go
var (
    brokers = os.Getenv("KAFKA_BROKERS")
    topic   = os.Getenv("KAFKA_TOPIC")
)

chMsg := make(chan pkg.Message)
chErr := make(chan error)
consumer := kafka.NewConsumer(strings.Split(brokers, ","), topic)

go func() {
    consumer.Read(context.Background(), chMsg, chErr)
}()

// read/process message
for {
    select {
        case m := <-chMsg:
            printMessage(m)
        case err := <-chErr:
            log.Println(err)
    }
}

```