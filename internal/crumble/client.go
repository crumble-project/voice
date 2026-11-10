// Package crumble is a client for the server routes a voice gateway token reaches, under /v1/voice/.
package crumble

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

const (
	maxErrorBody    = 4096
	maxResponseBody = 8 << 20
	snapshotLimit   = "50"
)

var (
	// ErrUnauthorized means the server refused the voice gateway token: it was revoked or replaced.
	ErrUnauthorized = errors.New("the server refused the voice gateway token")
	// ErrStatus is an HTTP status the client does not expect.
	ErrStatus = errors.New("unexpected status")
	// ErrProtocol means the server's answer does not match the API contract.
	ErrProtocol = errors.New("response does not match the API contract")
)

const (
	submitAttempts = 3
	submitRetry    = 500 * time.Millisecond
)

// Client calls the server's voice gateway routes with the voice gateway token.
type Client struct {
	base  *url.URL
	token string
	http  *http.Client
	// Waits between attempts; tests shorten them.
	submitRetry    time.Duration
	reconnectFirst time.Duration
	reconnectCap   time.Duration
}

// NewClient returns a client. httpClient must not follow redirects to other hosts
// with the Authorization header; the standard client drops it on cross-host redirects.
func NewClient(base *url.URL, token string, httpClient *http.Client) *Client {
	return &Client{
		base: base, token: token, http: httpClient,
		submitRetry: submitRetry, reconnectFirst: reconnectFirst, reconnectCap: reconnectCap,
	}
}

// Execution is one queued or finished bot turn.
type Execution struct {
	ID        string  `json:"id"`
	AgentID   string  `json:"agentId"`
	RequestID string  `json:"requestId"`
	Status    string  `json:"status"`
	Error     *string `json:"error"`
}

// Finished reports whether the execution reached a final state.
func (e Execution) Finished() bool {
	return e.Status == "done" || e.Status == "failed" || e.Status == "cancelled"
}

func linePath(username string) string {
	return "/v1/voice/lines/" + url.PathEscape(username)
}

// answersSpoken reports whether got is the request ID the server builds from a spoken
// message's IDs: "voice:" on a call with the owner, "outside-call:" on an outside call.
func answersSpoken(got, callID, requestID string) bool {
	rest := ":" + callID + ":" + requestID

	return got == "voice"+rest || got == "outside-call"+rest
}

// SendSpoken posts what was said on a call to a line's bot. A failed attempt is
// retried with the same IDs, which the server answers with the same execution.
func (c *Client) SendSpoken(ctx context.Context, username, callID, requestID, content string) (Execution, error) {
	body, err := json.Marshal(map[string]string{"callId": callID, "requestId": requestID, "content": content})
	if err != nil {
		return Execution{}, fmt.Errorf("encode message: %w", err)
	}

	var last error

	for attempt := range submitAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return Execution{}, fmt.Errorf("send message: %w", errors.Join(context.Cause(ctx), last))
			case <-time.After(c.submitRetry):
			}
		}

		execution, retry, err := c.sendSpokenOnce(ctx, username, body)
		if err == nil {
			if !answersSpoken(execution.RequestID, callID, requestID) {
				return execution, fmt.Errorf("send message: %w: answered for a different request", ErrProtocol)
			}

			return execution, nil
		}

		if !retry || ctx.Err() != nil {
			return Execution{}, err
		}

		last = err
	}

	return Execution{}, last
}

// SpokenExecution is a spoken message's execution and, once it is done, the bot's reply.
type SpokenExecution struct {
	Execution Execution `json:"execution"`
	Reply     []string  `json:"reply"`
	// HangUp is true when the bot asked to end the call once the reply was said.
	HangUp bool `json:"hangUp"`
}

// SpokenExecution fetches the state of a spoken message and the reply read by its submission.
func (c *Client) SpokenExecution(ctx context.Context, username, executionID string) (SpokenExecution, error) {
	var result SpokenExecution

	response, err := c.do(ctx, http.MethodGet, linePath(username)+"/executions/"+url.PathEscape(executionID), nil, "")
	if err != nil {
		return result, err
	}

	defer closeBody(response)

	if response.StatusCode != http.StatusOK {
		return result, statusError("get spoken execution", response)
	}

	if err := decode(response, &result); err != nil {
		return result, fmt.Errorf("get spoken execution: %w", err)
	}

	if result.Execution.ID != executionID {
		return result, fmt.Errorf("get spoken execution: %w: answered for execution %q", ErrProtocol, result.Execution.ID)
	}

	return result, nil
}

// roleUser marks the owner's messages in a transcript.
const roleUser = "user"

// Message is one transcript message of a chat snapshot.
type Message struct {
	ID        string `json:"id"`
	RequestID string `json:"requestId"`
	Role      string `json:"role"`
	Content   string `json:"content"`
}

// Snapshot is the part of a ChatSnapshot the gateway reads.
type Snapshot struct {
	Messages []Message `json:"messages"`
	Status   string    `json:"status"`
	Error    *string   `json:"error"`
	// Partial is the reply text being written; the stream sends it only with partial=true.
	Partial string `json:"partial"`
}

// Answering reports whether the newest owner message is the one with requestID,
// so the reply being written answers it rather than an earlier or later turn.
func (s Snapshot) Answering(requestID string) bool {
	for _, message := range slices.Backward(s.Messages) {
		if message.Role == roleUser {
			return message.RequestID == requestID
		}
	}

	return false
}

// Events streams chat snapshots of a line's bot until ctx ends or the stream breaks.
// The first snapshot arrives right after the connection opens.
func (c *Client) Events(ctx context.Context, username string, onSnapshot func(Snapshot) error) error {
	path := linePath(username) + "/events?partial=true&limit=" + snapshotLimit

	response, err := c.do(ctx, http.MethodGet, path, nil, "")
	if err != nil {
		return err
	}

	defer closeBody(response)

	if response.StatusCode != http.StatusOK {
		return statusError("stream chat events", response)
	}

	if media := response.Header.Get("Content-Type"); !strings.HasPrefix(media, "text/event-stream") {
		return fmt.Errorf("stream chat events: %w: content type %q", ErrProtocol, media)
	}

	return readEvents(response.Body, func(event Event) error {
		if event.Name != "snapshot" {
			return nil
		}

		var snapshot Snapshot
		if err := json.Unmarshal(event.Data, &snapshot); err != nil {
			return fmt.Errorf("decode snapshot: %w", err)
		}

		return onSnapshot(snapshot)
	})
}

// sendSpokenOnce reports whether a failure may be retried: a lost connection or a server error.
func (c *Client) sendSpokenOnce(ctx context.Context, username string, body []byte) (Execution, bool, error) {
	var execution Execution

	response, err := c.do(ctx, http.MethodPost, linePath(username)+"/messages", body, "application/json")
	if err != nil {
		return execution, !errors.Is(err, ErrUnauthorized), err
	}

	defer closeBody(response)

	if response.StatusCode == http.StatusConflict {
		return execution, false, fmt.Errorf("send message: %w", ErrCallEnded)
	}

	// The contract answers a queued message with 202 Accepted only.
	if response.StatusCode != http.StatusAccepted {
		return execution, response.StatusCode >= http.StatusInternalServerError, statusError("send message", response)
	}

	if err := decode(response, &execution); err != nil {
		return execution, false, fmt.Errorf("send message: %w", err)
	}

	return execution, false, nil
}

func (c *Client) do(ctx context.Context, method, path string, body []byte, contentType string) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	request, err := http.NewRequestWithContext(ctx, method, c.endpoint(path), reader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	request.Header.Set("Authorization", "Bearer "+c.token)

	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}

	response, err := c.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}

	if response.StatusCode == http.StatusUnauthorized {
		closeBody(response)

		return nil, ErrUnauthorized
	}

	return response, nil
}

func (c *Client) endpoint(path string) string {
	return strings.TrimSuffix(c.base.String(), "/") + path
}

func decode(response *http.Response, target any) error {
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxResponseBody))
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

func statusError(action string, response *http.Response) error {
	text, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBody))

	return fmt.Errorf("%s: %w %d: %s", action, ErrStatus, response.StatusCode, strings.TrimSpace(string(text)))
}

func closeBody(response *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxErrorBody))
	_ = response.Body.Close()
}

const (
	tlsHandshakeTimeout   = 10 * time.Second
	responseHeaderTimeout = 30 * time.Second
	idleConnTimeout       = 90 * time.Second
	maxIdleConnsPerHost   = 4
)

// DefaultHTTPClient has timeouts for everything except the event stream body.
func DefaultHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			TLSHandshakeTimeout:   tlsHandshakeTimeout,
			ResponseHeaderTimeout: responseHeaderTimeout,
			IdleConnTimeout:       idleConnTimeout,
			MaxIdleConnsPerHost:   maxIdleConnsPerHost,
			ForceAttemptHTTP2:     true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
