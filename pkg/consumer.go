package pkg

type Message interface{}

// Consumer an instance that consumes messages
type Consumer interface {
	// Read into the stream
	Read(message chan interface{}, chErr chan error)
	Ack(queueURL string, messages interface{}) (int64, error)
}
