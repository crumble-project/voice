package crumble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
	"unicode/utf16"

	"github.com/crumble-project/crumble-voice/internal/turn"
)

// ErrCallGone means the server no longer knows the call: it is unknown, settled, or
// cancelled, and the gateway must hang up.
var ErrCallGone = errors.New("the server no longer knows this call")

var (
	// ErrCallEnded means the server ended the call (409), and the gateway must hang up.
	ErrCallEnded = turn.ErrCallEnded
	// ErrNoCallState means the server answered a start report with 404: an older server
	// without call state, or a line it no longer serves. The call goes on.
	ErrNoCallState = errors.New("the server keeps no state for this call")
	// ErrRefused means the server refused a report as invalid, so sending it again cannot help.
	ErrRefused = errors.New("the server refused the report")
)

// The outcomes of a call report. Ringing acknowledges a call; ended closes a running
// call; the others settle a call the server asked for.
const (
	OutcomeRinging    = "ringing"
	OutcomeAnswered   = "answered"
	OutcomeUnanswered = "unanswered"
	OutcomeBusy       = "busy"
	OutcomeFailed     = "failed"
	OutcomeEnded      = "ended"
)

// The reasons of an ended report.
const (
	ReasonHungUp      = "hungUp"
	ReasonMaxDuration = "maxDuration"
	ReasonFailed      = "failed"
	ReasonStopping    = "stopping"
)

// The server measures its limits in JavaScript string length, UTF-16 code units.
const maxReportError = 500

// The parties of a call event. A call event without a party is a call to the owner.
const (
	PartyOwner   = "owner"
	PartyOutside = "outside"
)

// VoiceCall is a bot's request to call from a line: the owner, or with Party PartyOutside
// someone the owner approved.
type VoiceCall struct {
	CallID   string `json:"callId"`
	Username string `json:"username"`
	To       string `json:"to"`
	Greeting string `json:"greeting"`
	Party    string `json:"party,omitempty"`
}

// LineReport is a line's registration state as the owner's settings page shows it.
type LineReport struct {
	Username   string  `json:"username"`
	Registered bool    `json:"registered"`
	Error      *string `json:"error"`
}

// ReportCall reports a call's outcome; detail is the error text of a failed call.
// It returns ErrCallGone when the server answers 404. A lost connection or a server
// error is retried, since a lost report makes the server cancel a call the owner is on.
func (c *Client) ReportCall(ctx context.Context, callID, outcome, detail string) error {
	report := map[string]string{"outcome": outcome}
	if detail != "" {
		report["error"] = truncate(detail, maxReportError)
	}

	return c.reportCall(ctx, callID, report)
}

// ReportEnded reports that a running call ended and why, with ReportCall's retries.
func (c *Client) ReportEnded(ctx context.Context, callID, reason string) error {
	return c.reportCall(ctx, callID, map[string]string{"outcome": OutcomeEnded, "reason": reason})
}

func (c *Client) reportCall(ctx context.Context, callID string, report map[string]string) error {
	outcome := report["outcome"]

	body, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("encode call report: %w", err)
	}

	var (
		last error
		// An attempt that got no answer may still have reached the server.
		unanswered bool
	)

	for attempt := range submitAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("report call: %w", errors.Join(context.Cause(ctx), last))
			case <-time.After(c.submitRetry):
			}
		}

		retry, err := c.reportCallOnce(ctx, callID, body)

		// The lost attempt settled the call, so the server already knows the outcome.
		if errors.Is(err, ErrCallGone) && unanswered && outcome != OutcomeRinging {
			return nil
		}

		if !retry || ctx.Err() != nil {
			return err
		}

		unanswered = unanswered || !errors.Is(err, ErrStatus)
		last = err
	}

	return last
}

// reportCallOnce reports whether a failure may be retried: a lost connection or a server error.
func (c *Client) reportCallOnce(ctx context.Context, callID string, body []byte) (bool, error) {
	response, err := c.do(ctx, http.MethodPost, "/v1/voice/calls/"+url.PathEscape(callID), body, "application/json")
	if err != nil {
		return !errors.Is(err, ErrUnauthorized), err
	}

	defer closeBody(response)

	switch response.StatusCode {
	case http.StatusNoContent:
		return false, nil
	case http.StatusNotFound:
		return false, ErrCallGone
	default:
		if response.StatusCode < http.StatusInternalServerError {
			return false, fmt.Errorf("%w: %w", ErrRefused, statusError("report call", response))
		}

		return true, statusError("report call", response)
	}
}

// StartCall tells the server a call on a line started, before its greeting. It answers
// ErrCallEnded for 409 and ErrNoCallState for 404. A lost connection or a server error is
// retried; a repeated start only confirms the call.
func (c *Client) StartCall(ctx context.Context, username, callID string) error {
	path := linePath(username) + "/calls/" + url.PathEscape(callID)

	var last error

	for attempt := range submitAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("start call: %w", errors.Join(context.Cause(ctx), last))
			case <-time.After(c.submitRetry):
			}
		}

		retry, err := c.startCallOnce(ctx, path)
		if !retry || ctx.Err() != nil {
			return err
		}

		last = err
	}

	return last
}

func (c *Client) startCallOnce(ctx context.Context, path string) (bool, error) {
	response, err := c.do(ctx, http.MethodPut, path, []byte("{}"), "application/json")
	if err != nil {
		return !errors.Is(err, ErrUnauthorized), err
	}

	defer closeBody(response)

	switch response.StatusCode {
	case http.StatusNoContent:
		return false, nil
	case http.StatusNotFound:
		return false, ErrNoCallState
	case http.StatusConflict:
		return false, ErrCallEnded
	default:
		return response.StatusCode >= http.StatusInternalServerError, statusError("start call", response)
	}
}

// ReportLines replaces the registration state the owner's settings page shows.
func (c *Client) ReportLines(ctx context.Context, lines []LineReport) error {
	for i := range lines {
		if lines[i].Error != nil {
			text := truncate(*lines[i].Error, maxReportError)
			lines[i].Error = &text
		}
	}

	body, err := json.Marshal(map[string][]LineReport{"lines": lines})
	if err != nil {
		return fmt.Errorf("encode line report: %w", err)
	}

	response, err := c.do(ctx, http.MethodPut, "/v1/voice/lines", body, "application/json")
	if err != nil {
		return err
	}

	defer closeBody(response)

	if response.StatusCode != http.StatusNoContent {
		return statusError("report lines", response)
	}

	return nil
}

func truncate(text string, limit int) string {
	units := 0

	for i, r := range text {
		units += max(utf16.RuneLen(r), 1)
		if units > limit {
			return text[:i]
		}
	}

	return text
}
