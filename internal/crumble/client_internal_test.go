package crumble

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crumble-project/crumble-voice/internal/turn"
)

func TestReplyCollectsAssistantMessagesOfTheRequest(t *testing.T) {
	t.Parallel()

	snapshot := Snapshot{Messages: []Message{
		{Role: "user", RequestID: "old", Content: "earlier"},
		{Role: "assistant", Content: "earlier answer"},
		{Role: "user", RequestID: "mine", Content: "question"},
		{Role: "assistant", Content: "  "},
		{Role: "toolResult", Content: "tool output"},
		{Role: "assistant", Content: "first part"},
		{Role: "assistant", Content: "second part"},
		{Role: "user", RequestID: "later", Content: "next"},
		{Role: "assistant", Content: "not mine"},
	}}

	reply, found := snapshot.Reply("mine")
	if !found || strings.Join(reply, "|") != "first part|second part" {
		t.Fatalf("Reply = %q, %v", reply, found)
	}

	if _, found := snapshot.Reply("missing"); found {
		t.Fatal("Reply found a request that is not in the snapshot")
	}
}

func TestReadEvents(t *testing.T) {
	t.Parallel()

	stream := ": comment\nevent: heartbeat\ndata: {}\n\nevent: snapshot\ndata: {\"a\":\ndata: 1}\n\n"

	var events []Event

	err := readEvents(strings.NewReader(stream), func(event Event) error {
		events = append(events, event)

		return nil
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want io.ErrUnexpectedEOF at the end of the stream", err)
	}

	if len(events) != 2 || events[1].Name != "snapshot" || string(events[1].Data) != "{\"a\":\n1}" {
		t.Fatalf("events = %+v", events)
	}
}

// fakeCrumble answers sign-in, messages, executions, and events like the server does.
type fakeCrumble struct {
	mu        sync.Mutex
	requestID string
	status    string
	signIns   int
	snapshots chan string
}

func (f *fakeCrumble) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/auth/sign-in/email" {
		f.mu.Lock()
		f.signIns++
		f.mu.Unlock()
		_, _ = io.WriteString(w, `{"token":"session"}`)

		return
	}

	if r.Header.Get("Authorization") != "Bearer session" {
		w.WriteHeader(http.StatusUnauthorized)

		return
	}

	switch r.URL.Path {
	case "/v1/agents/bot/events":
		if r.URL.Query().Get("partial") != "true" {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: snapshot\ndata: {\"messages\":[],\"status\":\"idle\"}\n\n")
		flush(w)

		for {
			select {
			case data := <-f.snapshots:
				_, _ = fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", data)
				flush(w)
			case <-r.Context().Done():
				return
			}
		}
	case "/v1/agents/bot/messages":
		body, _ := io.ReadAll(r.Body)
		_, rest, _ := strings.Cut(string(body), `"requestId":"`)
		id, _, _ := strings.Cut(rest, `"`)

		f.mu.Lock()
		f.requestID, f.status = id, "running"
		f.mu.Unlock()

		w.WriteHeader(http.StatusAccepted)
		_, _ = fmt.Fprintf(w, `{"id":"exec","agentId":"bot","requestId":%q,"status":"pending","error":null}`, id)

		go func() {
			asked := fmt.Sprintf(`{"role":"user","requestId":%q,"content":"hi"}`, id)
			// The bot is still finishing an earlier turn; its text is not this call's reply.
			f.snapshots <- `{"messages":[{"role":"user","requestId":"other","content":"before"}],` +
				`"status":"running","partial":"Earlier answer"}`
			f.snapshots <- `{"messages":[` + asked + `],"status":"running","partial":"Hello"}`
			f.snapshots <- `{"messages":[` + asked + `],"status":"running","partial":"Hello **the"}`
			// A real turn writes for a while; without the pause the final snapshot
			// can replace the running ones before the client reads them.
			time.Sleep(50 * time.Millisecond)

			f.mu.Lock()
			f.status = "done"
			f.mu.Unlock()

			f.snapshots <- fmt.Sprintf(`{"messages":[{"role":"user","requestId":%q,"content":"hi"},`+
				`{"role":"assistant","content":"Hello **there**"}],"status":"idle"}`, id)
		}()
	case "/v1/executions":
		f.mu.Lock()
		defer f.mu.Unlock()
		_, _ = fmt.Fprintf(w, `[{"id":"exec","agentId":"bot","requestId":%q,"status":%q,"error":null}]`, f.requestID, f.status)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func flush(w http.ResponseWriter) {
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func TestTurnWaitsForTheFinishedExecution(t *testing.T) {
	t.Parallel()

	fake := &fakeCrumble{snapshots: make(chan string, 5)}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)

	base, _ := url.Parse(server.URL)
	client := NewClient(base, "owner@example.com", "password", server.Client())

	var (
		partials []string
		steps    []string
	)

	reply, err := client.Turn(context.Background(), "bot", "hi", turn.Observer{
		Accepted: func() { steps = append(steps, "accepted") },
		Running:  func() { steps = append(steps, "running") },
		Partial: func(generation int, text string) {
			partials = append(partials, fmt.Sprint(generation, ":", text))
		},
	})
	if err != nil {
		t.Fatalf("Turn: %v", err)
	}

	if len(reply) != 1 || reply[0] != "Hello **there**" {
		t.Fatalf("reply = %q", reply)
	}

	// Snapshots can be skipped when they arrive faster than they are read, so
	// only the order and the latest text are certain.
	if len(partials) == 0 || partials[len(partials)-1] != "0:Hello **the" {
		t.Fatalf("partials = %q", partials)
	}

	for _, partial := range partials {
		if strings.Contains(partial, "Earlier") {
			t.Fatalf("partials = %q, want none from another turn", partials)
		}
	}

	if strings.Join(steps, ",") != "accepted,running" {
		t.Fatalf("steps = %q", steps)
	}

	if fake.signIns != 1 {
		t.Fatalf("signed in %d times, want 1", fake.signIns)
	}
}

func TestPartialStreamCountsGenerations(t *testing.T) {
	t.Parallel()

	var got []string

	stream := partialStream{requestID: "mine", observer: turn.Observer{Partial: func(generation int, text string) {
		got = append(got, fmt.Sprint(generation, ":", text))
	}}}

	asked := []Message{{Role: "user", RequestID: "mine"}}

	for _, partial := range []string{"Let", "Let me", "Let me", "It is", "It is sunny"} {
		stream.observe(Snapshot{Messages: asked, Status: "running", Partial: partial})
	}

	stream.observe(Snapshot{Status: "idle", Partial: "ignored"})

	want := "0:Let|0:Let me|1:It is|1:It is sunny"
	if strings.Join(got, "|") != want {
		t.Fatalf("reported %q, want %q", strings.Join(got, "|"), want)
	}
}

func TestPartialStreamSkipsOtherTurns(t *testing.T) {
	t.Parallel()

	var got []string

	stream := partialStream{requestID: "mine", observer: turn.Observer{Partial: func(_ int, text string) {
		got = append(got, text)
	}}}

	stream.observe(Snapshot{Status: "running", Partial: "Not sent yet"})
	stream.observe(Snapshot{
		Messages: []Message{{Role: "user", RequestID: "earlier"}}, Status: "running", Partial: "Earlier reply",
	})
	stream.observe(Snapshot{Messages: []Message{{Role: "user", RequestID: "mine"}}, Status: "running", Partial: "Mine"})
	stream.observe(Snapshot{
		Messages: []Message{{Role: "user", RequestID: "mine"}, {Role: "user", RequestID: "later"}},
		Status:   "running", Partial: "Later reply",
	})

	if strings.Join(got, "|") != "Mine" {
		t.Fatalf("reported %q, want only this turn's partial", got)
	}
}
