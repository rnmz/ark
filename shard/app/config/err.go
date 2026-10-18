package config

import "fmt"

type AppConfigError struct {
	Reason string
}

func (e AppConfigError) Error() string {
	return fmt.Sprintf("Config error: %s", e.Reason)
}
