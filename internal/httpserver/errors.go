package httpserver

import "github.com/gin-gonic/gin"

// RequestError carries the operation that failed alongside the underlying
// error. Handlers record it in the Gin context; ObservabilityMiddleware logs it.
type RequestError struct {
	Operation string
	Err       error
}

func (e *RequestError) Error() string {
	if e.Err == nil {
		return e.Operation
	}

	return e.Operation + ": " + e.Err.Error()
}

func (e *RequestError) Unwrap() error {
	return e.Err
}

func RecordError(c *gin.Context, operation string, err error) {
	_ = c.Error(&RequestError{
		Operation: operation,
		Err:       err,
	})
}
