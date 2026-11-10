package crumble

import (
	"context"
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

// Turn sends what the owner said on a call to a line's bot and waits for the bot's
// reply, reporting its progress to observer. The result holds the reply's messages in order.
func (c *Client) Turn(ctx context.Context, username, callID, content string, observer turn.Observer) ([]string, error) {
	requestID := turn.NewID()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The stream opens before the message is sent, so no snapshot is missed.
	opened := make(chan struct{})
	snapshots := make(chan Snapshot, 1)
	streamErr := make(chan error, 1)

	go func() {
		first := true

		streamErr <- c.Events(ctx, username, func(snapshot Snapshot) error {
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

	execution, err := c.SendSpoken(ctx, username, callID, requestID, content)
	if err != nil {
		return nil, err
	}

	observer.NotifyAccepted()

	// Transcript messages carry the request ID the server built, not the one sent.
	stream := partialStream{observer: observer, requestID: execution.RequestID}

	for {
		select {
		case snapshot := <-snapshots:
			stream.observe(snapshot)

			reply, done, err := c.settle(ctx, username, execution.ID, snapshot, observer)
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

// settle reads the reply by the execution, not from the snapshot, once the chat stopped running.
func (c *Client) settle(
	ctx context.Context, username, executionID string, snapshot Snapshot, observer turn.Observer,
) ([]string, bool, error) {
	if snapshot.Status == "running" {
		return nil, false, nil
	}

	spoken, err := c.SpokenExecution(ctx, username, executionID)
	if err != nil {
		return nil, false, err
	}

	execution := spoken.Execution
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

	var reply []string

	for _, part := range spoken.Reply {
		if text := strings.TrimSpace(part); text != "" {
			reply = append(reply, text)
		}
	}

	if spoken.HangUp {
		observer.NotifyHangUp()
	}

	// A bot may hang up without a goodbye.
	if len(reply) == 0 && !spoken.HangUp {
		return nil, true, ErrNoReply
	}

	return reply, true, nil
}
