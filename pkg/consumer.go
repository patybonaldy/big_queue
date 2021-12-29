package pkg

import (
	"context"
)

type Message interface{}

// Consumer an instance that consumes messages
type Consumer interface {
	// Read read into the stream
	Read(ctx context.Context, chMsg chan Message, chErr chan error)
}
