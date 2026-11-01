package security

import "fmt"

type KeyError struct {
	Reason string
}

func (e *KeyError) Error() string {
	return fmt.Sprintf("KeyError: %s", e.Reason)
}
