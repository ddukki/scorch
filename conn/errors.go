package conn

import "fmt"

// Kind classifies an Error.
type Kind int

const (
	// KindNetwork is a network-level error (dial, read, write).
	KindNetwork  Kind = iota
	// KindProtocol is a protocol-level error (unexpected packet, etc.).
	KindProtocol
	// KindConfig is a configuration error (invalid DSN, etc.).
	KindConfig
	// KindServer is a server-side error (query rejection, etc.).
	KindServer
	// KindInternal is an internal/implementation error.
	KindInternal
)

// Error represents a chu-go error with a Kind and optional wrapped error.
type Error struct {
	Kind    Kind
	Message string
	ServerCode int
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Err }
