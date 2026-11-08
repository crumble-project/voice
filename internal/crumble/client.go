// Package crumble is a client for the parts of the Crumble /v1 API the voice gateway uses.
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
	"sync"
	"time"
)

const (
	maxErrorBody    = 4096
	maxResponseBody = 8 << 20
	snapshotLimit   = "50"
)

var (
	// ErrUnauthorized means the owner's credentials were refused.
	ErrUnauthorized = errors.New("crumble refused the owner's credentials")
	// ErrStatus is an HTTP status the client does not expect.
	ErrStatus = errors.New("unexpected status")
	// ErrProtocol means the server's answer does not match the API contract.
	ErrProtocol = errors.New("response does not match the API contract")
)

// Client signs in as the owner and keeps the session token for later requests.
type Client struct {
	base     *url.URL
	email    string
	password string
	http     *http.Client

	mu    sync.Mutex
	token string
}

// NewClient returns a client. httpClient must not follow redirects to other hosts
// with the Authorization header; the standard client drops it on cross-host redirects.
func NewClient(base *url.URL, email, password string, httpClient *http.Client) *Client {
	return &Client{base: base, email: email, password: password, http: httpClient}
}

// SignIn exchanges the owner's email and password for a session token.
func (c *Client) SignIn(ctx context.Context) error {
	body, err := json.Marshal(map[string]string{"email": c.email, "password": c.password})
	if err != nil {
		return fmt.Errorf("encode sign-in: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("/api/auth/sign-in/email"), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build sign-in: %w", err)
	}

	request.Header.Set("Content-Type", "application/json")

	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("sign in: %w", err)
	}

	defer closeBody(response)

	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return ErrUnauthorized
	}

	if response.StatusCode != http.StatusOK {
		return statusError("sign in", response)
	}

	var result struct {
		Token string `json:"token"`
	}

	if err := decode(response, &result); err != nil {
		return fmt.Errorf("sign in: %w", err)
	}

	if result.Token == "" {
		return fmt.Errorf("sign in: %w: no session token", ErrProtocol)
	}

	c.mu.Lock()
	c.token = result.Token
	c.mu.Unlock()

	return nil
}

// Agent is the part of a bot the gateway reads.
type Agent struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
}

// Agent fetches one bot.
func (c *Client) Agent(ctx context.Context, botID string) (Agent, error) {
	var agent Agent

	response, err := c.do(ctx, http.MethodGet, "/v1/agents/"+url.PathEscape(botID), nil, "")
	if err != nil {
		return agent, err
	}

	defer closeBody(response)

	if response.StatusCode != http.StatusOK {
		return agent, statusError("get bot", response)
	}

	if err := decode(response, &agent); err != nil {
		return agent, fmt.Errorf("get bot: %w", err)
	}

	if agent.ID != botID {
		return agent, fmt.Errorf("get bot: %w: answered with bot %q instead of %q", ErrProtocol, agent.ID, botID)
	}

	return agent, nil
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

// SendMessage posts the owner's message to a bot chat. requestID makes retries idempotent.
func (c *Client) SendMessage(ctx context.Context, botID, content, requestID string) (Execution, error) {
	var execution Execution

	body, err := json.Marshal(map[string]string{"content": content, "requestId": requestID})
	if err != nil {
		return execution, fmt.Errorf("encode message: %w", err)
	}

	response, err := c.do(ctx, http.MethodPost, "/v1/agents/"+url.PathEscape(botID)+"/messages", body, "application/json")
	if err != nil {
		return execution, err
	}

	defer closeBody(response)

	// The contract answers a queued message with 202 Accepted only.
	if response.StatusCode != http.StatusAccepted {
		return execution, statusError("send message", response)
	}

	if err := decode(response, &execution); err != nil {
		return execution, fmt.Errorf("send message: %w", err)
	}

	if execution.RequestID != requestID || execution.AgentID != botID {
		return execution, fmt.Errorf("send message: %w: answered for a different request", ErrProtocol)
	}

	return execution, nil
}

// Execution fetches the current state of one execution of a bot.
func (c *Client) Execution(ctx context.Context, botID, executionID string) (Execution, error) {
	response, err := c.do(ctx, http.MethodGet, "/v1/executions?agentId="+url.QueryEscape(botID), nil, "")
	if err != nil {
		return Execution{}, err
	}

	defer closeBody(response)

	if response.StatusCode != http.StatusOK {
		return Execution{}, statusError("list executions", response)
	}

	var executions []Execution
	if err := decode(response, &executions); err != nil {
		return Execution{}, fmt.Errorf("list executions: %w", err)
	}

	for _, execution := range executions {
		if execution.ID == executionID {
			return execution, nil
		}
	}

	return Execution{}, fmt.Errorf("%w: execution %s is not listed", ErrProtocol, executionID)
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

// Reply returns the assistant messages that answer requestID, in order: those
// after the owner's message with that request ID and before the next owner message.
// found is false when the owner's message is not in the snapshot.
func (s Snapshot) Reply(requestID string) ([]string, bool) {
	var parts []string

	found := false

	for _, message := range s.Messages {
		if !found {
			found = message.Role == roleUser && message.RequestID == requestID

			continue
		}

		if message.Role == roleUser {
			break
		}

		if text := strings.TrimSpace(message.Content); message.Role == "assistant" && text != "" {
			parts = append(parts, text)
		}
	}

	return parts, found
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

// Events streams chat snapshots of a bot until ctx ends or the stream breaks.
// The first snapshot arrives right after the connection opens.
func (c *Client) Events(ctx context.Context, botID string, onSnapshot func(Snapshot) error) error {
	path := "/v1/agents/" + url.PathEscape(botID) + "/events?partial=true&limit=" + snapshotLimit

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

func (c *Client) do(ctx context.Context, method, path string, body []byte, contentType string) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		c.mu.Lock()
		token := c.token
		c.mu.Unlock()

		if token == "" {
			if err := c.SignIn(ctx); err != nil {
				return nil, err
			}

			continue
		}

		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}

		request, err := http.NewRequestWithContext(ctx, method, c.endpoint(path), reader)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}

		request.Header.Set("Authorization", "Bearer "+token)

		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}

		response, err := c.http.Do(request)
		if err != nil {
			return nil, fmt.Errorf("%s %s: %w", method, path, err)
		}

		// An expired session gets one fresh sign-in; a second refusal is final.
		if response.StatusCode == http.StatusUnauthorized && attempt == 0 {
			closeBody(response)
			c.mu.Lock()
			c.token = ""
			c.mu.Unlock()

			continue
		}

		if response.StatusCode == http.StatusUnauthorized {
			closeBody(response)

			return nil, ErrUnauthorized
		}

		return response, nil
	}
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
