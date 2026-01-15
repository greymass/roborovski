package serviceclient

import (
	"fmt"
)

type ServiceError struct {
	StatusCode int
	Message    string
	Body       []byte
}

func (e *ServiceError) Error() string {
	if len(e.Body) > 0 {
		return fmt.Sprintf("service error %d: %s", e.StatusCode, string(e.Body))
	}
	return fmt.Sprintf("service error %d: %s", e.StatusCode, e.Message)
}
