package crumble

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

const (
	// Snapshots can hold 50 messages of up to 32,000 characters each.
	maxEventBytes = 16 << 20
	scanBuffer    = 64 << 10
)

// Event is one server-sent event.
type Event struct {
	Name string
	Data []byte
}

// readEvents parses a text/event-stream body and calls onEvent for each event.
// It returns io.ErrUnexpectedEOF when the stream ends, since Crumble never closes it on purpose.
func readEvents(body io.Reader, onEvent func(Event) error) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, scanBuffer), maxEventBytes)

	var (
		name string
		data bytes.Buffer
		has  bool
	)

	for scanner.Scan() {
		line := scanner.Bytes()

		if len(line) == 0 {
			if err := dispatch(name, &data, has, onEvent); err != nil {
				return err
			}

			name, has = "", false
			data.Reset()

			continue
		}

		if line[0] == ':' {
			continue
		}

		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))

		switch string(field) {
		case "event":
			name = string(value)
		case "data":
			if has {
				data.WriteByte('\n')
			}

			data.Write(value)

			has = true
		case "id", "retry":
		default:
		}
	}

	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return fmt.Errorf("event exceeds %d bytes: %w", maxEventBytes, err)
		}

		return fmt.Errorf("read event stream: %w", err)
	}

	return io.ErrUnexpectedEOF
}

func dispatch(name string, data *bytes.Buffer, has bool, onEvent func(Event) error) error {
	if !has {
		return nil
	}

	if name == "" {
		name = "message"
	}

	return onEvent(Event{Name: name, Data: bytes.Clone(data.Bytes())})
}
