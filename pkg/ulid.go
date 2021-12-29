package pkg

import (
	"crypto/rand"
	"github.com/oklog/ulid/v2"
	"time"
)

// Ulid encapsulate the way to generate ulids
func Ulid() string {
	t := time.Now().UTC()
	id := ulid.MustNew(ulid.Timestamp(t), rand.Reader)

	return id.String()
}
