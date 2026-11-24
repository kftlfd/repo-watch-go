package rabbitmq

import (
	"strconv"

	"github.com/rabbitmq/amqp091-go"
)

// StringHeader reads a string AMQP header.
func StringHeader(headers amqp091.Table, key string) string {
	value, _ := headers[key].(string)
	return value
}

// Int32Header reads an integer AMQP header. Field tables can decode the same
// logical value as different Go integer types depending on how it was encoded.
func Int32Header(headers amqp091.Table, key string) int32 {
	switch value := headers[key].(type) {
	case int:
		return int32(value)
	case int8:
		return int32(value)
	case int16:
		return int32(value)
	case int32:
		return value
	case int64:
		return int32(value)
	case string:
		val, _ := strconv.Atoi(value)
		return int32(val)
	default:
		return 0
	}
}
