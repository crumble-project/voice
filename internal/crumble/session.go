package crumble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// ErrReplaced means another voice gateway connected with the same token and took over.
var ErrReplaced = errors.New("another voice gateway connected with this token")

var errStreamIdle = errors.New("the voice gateway events stream sent nothing for too long")

const (
	reconnectFirst = time.Second
	reconnectCap   = 30 * time.Second
	backoffFactor  = 2
	// The server sends a heartbeat every 15 seconds; three missed ones mean the connection is dead.
	streamIdleTimeout = 45 * time.Second
)

// VoiceConfig is what the server says the gateway serves: the lines and the owner's numbers.
type VoiceConfig struct {
	Enabled      bool        `json:"enabled"`
	OwnerNumbers []string    `json:"ownerNumbers"`
	Lines        []VoiceLine `json:"lines"`
}

// VoiceLine is one line the server serves: a SIP username answered by one bot.
type VoiceLine struct {
	Username    string `json:"username"`
	BotID       string `json:"botId"`
	BotName     string `json:"botName"`
	DisplayName string `json:"displayName"`
	CallsOwner  bool   `json:"callsOwner"`
}

// Handlers receive the events of the voice gateway's events stream. They run on the
// stream's goroutine, so a handler that dials or waits must hand the work off.
type Handlers struct {
	Config func(VoiceConfig)
	Call   func(VoiceCall)
	Cancel func(callID string)
}

// Serve keeps the voice gateway's events stream open until ctx ends and passes every
// event to handlers. The stream declares that the gateway places calls. A dropped stream
// reconnects with backoff; a refused token or a replaced connection ends Serve with
// ErrUnauthorized or ErrReplaced.
func (c *Client) Serve(ctx context.Context, handlers Handlers, log *slog.Logger) error {
	delay := c.reconnectFirst

	for {
		configured := false
		connection := handlers
		connection.Config = func(config VoiceConfig) {
			configured = true

			handlers.Config(config)
		}

		err := c.session(ctx, connection)

		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrReplaced) {
			return err
		}

		if configured {
			delay = c.reconnectFirst
		}

		log.Warn("voice gateway events stream lost, reconnecting", "error", err, "in", delay.String())

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}

		delay = min(backoffFactor*delay, c.reconnectCap)
	}
}

// session reads one connection of the events stream. Events it does not know are ignored.
func (c *Client) session(ctx context.Context, handlers Handlers) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	idle := time.AfterFunc(streamIdleTimeout, func() { cancel(errStreamIdle) })
	defer idle.Stop()

	response, err := c.do(ctx, http.MethodGet, "/v1/voice/events?calls=true", nil, "")
	if err != nil {
		return err
	}

	defer closeBody(response)

	if response.StatusCode != http.StatusOK {
		return statusError("stream voice gateway events", response)
	}

	if media := response.Header.Get("Content-Type"); !strings.HasPrefix(media, "text/event-stream") {
		return fmt.Errorf("stream voice gateway events: %w: content type %q", ErrProtocol, media)
	}

	err = readEvents(response.Body, func(event Event) error {
		idle.Reset(streamIdleTimeout)

		switch event.Name {
		case "config":
			var config VoiceConfig
			if decodeErr := json.Unmarshal(event.Data, &config); decodeErr != nil {
				return fmt.Errorf("decode config: %w", decodeErr)
			}

			handlers.Config(config)
		case "call":
			var call VoiceCall
			if decodeErr := json.Unmarshal(event.Data, &call); decodeErr != nil || call.CallID == "" {
				return fmt.Errorf("decode call: %w", errors.Join(ErrProtocol, decodeErr))
			}

			handlers.Call(call)
		case "cancel":
			var cancelled struct {
				CallID string `json:"callId"`
			}
			if decodeErr := json.Unmarshal(event.Data, &cancelled); decodeErr != nil || cancelled.CallID == "" {
				return fmt.Errorf("decode cancel: %w", errors.Join(ErrProtocol, decodeErr))
			}

			handlers.Cancel(cancelled.CallID)
		case "replaced":
			return ErrReplaced
		}

		return nil
	})

	if cause := context.Cause(ctx); errors.Is(cause, errStreamIdle) {
		return fmt.Errorf("stream voice gateway events: %w", cause)
	}

	return err
}
