package crumble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crumble-project/crumble-voice/internal/turn"
)

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

const testToken = "acct_1.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

type spoken struct {
	CallID    string `json:"callId"`
	RequestID string `json:"requestId"`
	Content   string `json:"content"`
}

// fakeCrumble answers the voice gateway's line routes like the server does.
type fakeCrumble struct {
	mu sync.Mutex
	// failures is how many submissions answer 503 before one is accepted.
	failures  int
	posted    []spoken
	status    string
	signIns   int
	snapshots chan string
}

func (f *fakeCrumble) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/auth/") {
		f.mu.Lock()
		f.signIns++
		f.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if r.Header.Get("Authorization") != "Bearer "+testToken {
		unauthorized(w)

		return
	}

	switch r.URL.Path {
	case "/v1/voice/lines/ada/events":
		f.events(w, r)
	case "/v1/voice/lines/ada/messages":
		f.message(w, r)
	case "/v1/voice/lines/ada/executions/exec":
		f.mu.Lock()
		defer f.mu.Unlock()

		// The transcript's last text differs on purpose: the reply must come from this route.
		reply := "null"
		if f.status == "done" {
			reply = `["Hello **there**"]`
		}

		_, _ = fmt.Fprintf(w, `{"execution":{"id":"exec","agentId":"bot","requestId":%q,"status":%q,"error":null},"reply":%s}`,
			f.requestID(), f.status, reply)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = io.WriteString(w, `{"error":"Voice gateway token required"}`)
}

// requestID is the server's request ID for the last submission; f.mu must be held.
func (f *fakeCrumble) requestID() string {
	last := f.posted[len(f.posted)-1]

	return "voice:" + last.CallID + ":" + last.RequestID
}

func (f *fakeCrumble) events(w http.ResponseWriter, r *http.Request) {
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
}

func (f *fakeCrumble) message(w http.ResponseWriter, r *http.Request) {
	var message spoken
	if err := json.NewDecoder(r.Body).Decode(&message); err != nil {
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	f.mu.Lock()
	f.posted = append(f.posted, message)

	if f.failures > 0 {
		f.failures--
		f.mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)

		return
	}

	id := f.requestID()
	f.status = "running"
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

		f.snapshots <- `{"messages":[` + asked + `,{"role":"assistant","content":"Not the reply"}],"status":"idle"}`
	}()
}

func flush(w http.ResponseWriter) {
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func testClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	base, _ := url.Parse(server.URL)
	client := NewClient(base, testToken, server.Client())
	client.submitRetry = time.Millisecond
	client.reconnectFirst = time.Millisecond
	client.reconnectCap = time.Millisecond

	return client
}

func TestTurnSettlesFromTheSpokenExecution(t *testing.T) {
	t.Parallel()

	fake := &fakeCrumble{snapshots: make(chan string, 5)}
	client := testClient(t, fake)

	var (
		partials []string
		steps    []string
	)

	reply, err := client.Turn(context.Background(), "ada", "call-1", "hi", turn.Observer{
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
		t.Fatalf("reply = %q, want the spoken execution's reply", reply)
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

	if len(fake.posted) != 1 || fake.posted[0].CallID != "call-1" || fake.posted[0].Content != "hi" {
		t.Fatalf("posted = %+v, want one message with the call ID", fake.posted)
	}

	if fake.signIns != 0 {
		t.Fatalf("signed in %d times, want none", fake.signIns)
	}
}

func TestRetriedSubmissionReusesItsIDs(t *testing.T) {
	t.Parallel()

	fake := &fakeCrumble{failures: 1, snapshots: make(chan string, 5)}
	client := testClient(t, fake)

	if _, err := client.Turn(context.Background(), "ada", "call-1", "hi", turn.Observer{}); err != nil {
		t.Fatalf("Turn: %v", err)
	}

	if len(fake.posted) != 2 || fake.posted[0] != fake.posted[1] || fake.posted[0].RequestID == "" {
		t.Fatalf("posted = %+v, want the retry to repeat the first attempt's IDs", fake.posted)
	}
}

func TestUnauthorizedStopsWithoutRetrying(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		requests int
	)

	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		unauthorized(w)
	}))

	err := client.Serve(context.Background(), configs(func(VoiceConfig) { t.Error("got a config without a token") }),
		slog.New(slog.DiscardHandler))
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Serve = %v, want ErrUnauthorized", err)
	}

	if _, err := client.SendSpoken(context.Background(), "ada", "call-1", "request-1", "hi"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("SendSpoken = %v, want ErrUnauthorized", err)
	}

	if requests != 2 {
		t.Fatalf("sent %d requests, want one each and no retry", requests)
	}
}

// voiceEvents serves /v1/voice/events, one script of events per connection.
func voiceEvents(t *testing.T, connections [][]string) (http.Handler, func() int) {
	t.Helper()

	var (
		mu     sync.Mutex
		opened int
	)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/voice/events" || r.URL.RawQuery != "calls=true" || r.Header.Get("Authorization") != "Bearer "+testToken {
			t.Errorf("request %s %s, want the events stream with calls=true and the token", r.Method, r.URL)
			unauthorized(w)

			return
		}

		mu.Lock()
		script := connections[min(opened, len(connections)-1)]
		opened++
		mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")

		for _, event := range script {
			_, _ = io.WriteString(w, event)
		}

		flush(w)
	})

	return handler, func() int {
		mu.Lock()
		defer mu.Unlock()

		return opened
	}
}

func config(number string) string {
	return `event: config
data: {"enabled":true,"ownerNumbers":["` + number + `"],"lines":[{"username":"ada","botId":"bot","botName":"Ada",` +
		`"displayName":"Ada","callsOwner":false}]}

`
}

// configs handles only config events and fails the test on a call or cancel.
func configs(onConfig func(VoiceConfig)) Handlers {
	return Handlers{
		Config: onConfig,
		Call:   func(VoiceCall) { panic("unexpected call event") },
		Cancel: func(string) { panic("unexpected cancel event") },
	}
}

func TestServePassesEveryEventAndIgnoresUnknownOnes(t *testing.T) {
	t.Parallel()

	call := "event: call\ndata: {\"callId\":\"c\",\"username\":\"ada\",\"to\":\"**610\",\"greeting\":\"Hi.\"}\n\n"
	handler, opened := voiceEvents(t, [][]string{
		{config("**610"), call, "event: heartbeat\ndata: {}\n\n", "event: cancel\ndata: {\"callId\":\"c\"}\n\n", config("**611")},
		{config("**612")},
	})
	client := testClient(t, handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var events []string

	err := client.Serve(ctx, Handlers{
		Config: func(config VoiceConfig) {
			events = append(events, config.OwnerNumbers...)

			if len(config.Lines) != 1 || config.Lines[0].BotName != "Ada" {
				t.Errorf("lines = %+v", config.Lines)
			}

			if len(events) == 5 {
				cancel()
			}
		},
		Call: func(call VoiceCall) {
			events = append(events, "call "+call.CallID+" from "+call.Username+" to "+call.To+": "+call.Greeting)
		},
		Cancel: func(callID string) { events = append(events, "cancel "+callID) },
	}, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("Serve = %v, want nil once ctx ends", err)
	}

	want := "**610,call c from ada to **610: Hi.,cancel c,**611,**612"
	if strings.Join(events, ",") != want || opened() != 2 {
		t.Fatalf("events = %q over %d connections, want %q and a reconnect", events, opened(), want)
	}
}

func TestReplacedEndsServe(t *testing.T) {
	t.Parallel()

	handler, opened := voiceEvents(t, [][]string{{config("**610"), "event: replaced\ndata: {}\n\n"}})
	client := testClient(t, handler)

	received := 0

	err := client.Serve(context.Background(), configs(func(VoiceConfig) { received++ }), slog.New(slog.DiscardHandler))
	if !errors.Is(err, ErrReplaced) {
		t.Fatalf("Serve = %v, want ErrReplaced", err)
	}

	if received != 1 || opened() != 1 {
		t.Fatalf("%d configs over %d connections, want one and no reconnect", received, opened())
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

func TestCallStartAndEndReports(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		requests []string
	)

	// statuses answers each "METHOD path" in turn; the last answer repeats.
	statuses := map[string][]int{
		"PUT /v1/voice/lines/ada/calls/started": {http.StatusInternalServerError, http.StatusNoContent},
		"PUT /v1/voice/lines/ada/calls/ended":   {http.StatusConflict},
		"PUT /v1/voice/lines/ada/calls/older":   {http.StatusNotFound},
		"POST /v1/voice/calls/started":          {http.StatusNoContent},
		"POST /v1/voice/calls/invalid":          {http.StatusBadRequest},
		"POST /v1/voice/lines/ada/messages":     {http.StatusConflict},
	}

	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		key := r.Method + " " + r.URL.Path

		mu.Lock()
		defer mu.Unlock()

		requests = append(requests, key+" "+string(body))
		queued := statuses[key]

		if len(queued) > 1 {
			statuses[key] = queued[1:]
		}

		w.WriteHeader(queued[0])
	}))

	ctx := context.Background()

	if err := client.StartCall(ctx, "ada", "started"); err != nil {
		t.Errorf("StartCall after a server error = %v, want it retried", err)
	}

	if err := client.StartCall(ctx, "ada", "ended"); !errors.Is(err, ErrCallEnded) {
		t.Errorf("StartCall on 409 = %v, want ErrCallEnded", err)
	}

	if err := client.StartCall(ctx, "ada", "older"); !errors.Is(err, ErrNoCallState) {
		t.Errorf("StartCall on 404 = %v, want ErrNoCallState", err)
	}

	if err := client.ReportEnded(ctx, "started", ReasonMaxDuration); err != nil {
		t.Errorf("ReportEnded = %v", err)
	}

	if err := client.ReportEnded(ctx, "invalid", ReasonHungUp); !errors.Is(err, ErrRefused) {
		t.Errorf("ReportEnded on 400 = %v, want ErrRefused", err)
	}

	if _, err := client.SendSpoken(ctx, "ada", "ended", "request-1", "hi"); !errors.Is(err, ErrCallEnded) {
		t.Errorf("SendSpoken on 409 = %v, want ErrCallEnded", err)
	}

	want := []string{
		"PUT /v1/voice/lines/ada/calls/started {}",
		"PUT /v1/voice/lines/ada/calls/started {}",
		"PUT /v1/voice/lines/ada/calls/ended {}",
		"PUT /v1/voice/lines/ada/calls/older {}",
		`POST /v1/voice/calls/started {"outcome":"ended","reason":"maxDuration"}`,
		`POST /v1/voice/calls/invalid {"outcome":"ended","reason":"hungUp"}`,
		`POST /v1/voice/lines/ada/messages {"callId":"ended","content":"hi","requestId":"request-1"}`,
	}

	if !slices.Equal(requests, want) {
		t.Errorf("requests =\n%s\nwant one retry after the server error and none after a 4xx:\n%s",
			strings.Join(requests, "\n"), strings.Join(want, "\n"))
	}
}
