package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	HeaderEventID     = "x-event-id"
	HeaderEventType   = "x-event-type"
	HeaderChainID     = "x-chain-id"
	HeaderRetryCount  = "x-retry-count"
	HeaderPublishedAt = "x-published-at"
)

func CloneHeaders(in amqp.Table) amqp.Table {
	out := amqp.Table{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func GetRetryCount(headers amqp.Table) (int, error) {
	if headers == nil {
		return 0, nil
	}

	raw, ok := headers[HeaderRetryCount]
	if !ok {
		return 0, nil
	}

	switch v := raw.(type) {
	case int:
		return v, nil
	case int8:
		return int(v), nil
	case int16:
		return int(v), nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case uint:
		return int(v), nil
	case uint8:
		return int(v), nil
	case uint16:
		return int(v), nil
	case uint32:
		return int(v), nil
	case uint64:
		return int(v), nil
	case string:
		var parsed int
		_, err := fmt.Sscanf(v, "%d", &parsed)
		if err != nil {
			return 0, fmt.Errorf("parse retry count from string: %w", err)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported retry count header type: %T", raw)
	}
}

func SetRetryCount(headers amqp.Table, count int) amqp.Table {
	cloned := CloneHeaders(headers)
	cloned[HeaderRetryCount] = count
	return cloned
}
