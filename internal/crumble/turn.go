package crumble

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/crumble-project/crumble-voice/internal/turn"
)

var (
	// ErrNoReply means the turn finished without assistant text.
	ErrNoReply = errors.New("the bot finished without a text reply")
	// ErrStreamClosed means the event stream ended before the turn did.
	ErrStreamClosed = errors.New("chat event stream closed before the bot replied")
)

// TurnError is a turn the bot could not complete.
type TurnError struct {
	Status string
	Detail string
}

func (e *TurnError) Error() string {
	return fmt.Sprintf("bot turn %s: %s", e.Status, e.Detail)
}

// Turn sends content to a bot chat and waits for the bot's reply, reporting its
// progress to observer. The result holds the reply's assistant messages in order.
func (c *Client) Turn(ctx context.Context, botID, content string, observer turn.Observer) ([]string, error) {
	requestID := newRequestID()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The stream opens before the message is sent, so no snapshot is missed.
	opened := make(chan struct{})
	snapshots := make(chan Snapshot, 1)
	streamErr := make(chan error, 1)

	go func() {
		first := true

		streamErr <- c.Events(ctx, botID, func(snapshot Snapshot) error {
			if first {
				first = false

				close(opened)
			}

			// Only the latest snapshot matters: each one carries the whole state.
			select {
			case <-snapshots:
			default:
			}

			snapshots <- snapshot

			return nil
		})
	}()

	select {
	case <-opened:
	case err := <-streamErr:
		return nil, fmt.Errorf("open chat events: %w", err)
	case <-ctx.Done():
		return nil, fmt.Errorf("open chat events: %w", context.Cause(ctx))
	}

	execution, err := c.SendMessage(ctx, botID, content, requestID)
	if err != nil {
		return nil, err
	}

	observer.NotifyAccepted()

	stream := partialStream{observer: observer, requestID: requestID}

	for {
		select {
		case snapshot := <-snapshots:
			stream.observe(snapshot)

			reply, done, err := c.settle(ctx, botID, execution.ID, requestID, snapshot)
			if done || err != nil {
				return reply, err
			}
		case err := <-streamErr:
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, ErrStreamClosed
			}

			return nil, fmt.Errorf("chat events: %w", err)
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for reply: %w", context.Cause(ctx))
		}
	}
}

// partialStream turns snapshots into progress. A partial that does not extend
// the previous one starts a new generation.
type partialStream struct {
	observer   turn.Observer
	requestID  string
	running    bool
	generation int
	last       string
}

func (p *partialStream) observe(snapshot Snapshot) {
	if snapshot.Status != "running" {
		return
	}

	// The first running snapshot can belong to another turn the bot was still finishing.
	if !p.running {
		p.running = true
		p.observer.NotifyRunning()
	}

	if snapshot.Partial == "" || snapshot.Partial == p.last || !snapshot.Answering(p.requestID) {
		return
	}

	if p.last != "" && !strings.HasPrefix(snapshot.Partial, p.last) {
		p.generation++
	}

	p.last = snapshot.Partial
	p.observer.NotifyPartial(p.generation, snapshot.Partial)
}

func (c *Client) settle(
	ctx context.Context, botID, executionID, requestID string, snapshot Snapshot,
) ([]string, bool, error) {
	if snapshot.Status == "running" {
		return nil, false, nil
	}

	execution, err := c.Execution(ctx, botID, executionID)
	if err != nil {
		return nil, false, err
	}

	if !execution.Finished() {
		return nil, false, nil
	}

	if execution.Status != "done" {
		detail := "no detail"
		if execution.Error != nil && *execution.Error != "" {
			detail = *execution.Error
		}

		return nil, true, &TurnError{Status: execution.Status, Detail: detail}
	}

	reply, found := snapshot.Reply(requestID)
	if !found {
		// The status changed between snapshots; the next snapshot carries the transcript.
		return nil, false, nil
	}

	if len(reply) == 0 {
		return nil, true, ErrNoReply
	}

	return reply, true, nil
}

const (
	uuidVersionMask = 0x0f
	uuidVersion4    = 0x40
	uuidVariantMask = 0x3f
	uuidVariantRFC  = 0x80
)

// newRequestID returns a random version 4 UUID, the request ID format the API requires.
func newRequestID() string {
	var b [16]byte

	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error.

	b[6] = (b[6] & uuidVersionMask) | uuidVersion4
	b[8] = (b[8] & uuidVariantMask) | uuidVariantRFC

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
