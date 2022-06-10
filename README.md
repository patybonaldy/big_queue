# Big Queue on Go [![Codacy Badge](https://app.codacy.com/project/badge/Grade/53ec882dd04f49be98108da8ec0f0dcd)](https://www.codacy.com/gh/patriciabonaldy/big_queue/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=patriciabonaldy/big_queue&amp;utm_campaign=Badge_Grade)

This is a simple big queue and implementation in kafka, rabbit and aws sqs.
* 
* Note: We have used package-oriented-design to create this API, you can know more about this
  [link](https://www.ardanlabs.com/blog/2017/02/package-oriented-design.html)


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
