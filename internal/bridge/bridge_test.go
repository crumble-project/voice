package bridge_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/emiago/diago"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/bridge"
	"github.com/crumble-project/crumble-voice/internal/call"
	"github.com/crumble-project/crumble-voice/internal/config"
	"github.com/crumble-project/crumble-voice/internal/crumble"
	"github.com/crumble-project/crumble-voice/internal/sipline"
	"github.com/crumble-project/crumble-voice/internal/speech"
	"github.com/crumble-project/crumble-voice/internal/turn"
)

// Ports apart from internal/sipline's, since go test runs packages at the same time.
const (
	registrarPort = 25160
	gatewayPort   = 25170
	token         = "acct_1.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	// The owner's phone answers, hears the greeting, and hangs up.
	answers = "**610"
	// The owner's phone rings until the caller cancels.
	rings = "**620"
	// The owner's phone answers, hears the greeting, and waits for the gateway to hang up.
	stays    = "**630"
	stranger = "999"
	callA    = "0b4d2a51-7c1e-4f3a-9d2b-6e8f1a2c3d4e"
	callB    = "1c5e3b62-8d2f-4a4b-8e3c-7f9a2b3d4e5f"
	callC    = "2d6f4c73-9e3a-4b5c-9f4d-8a0b3c4d5e6f"
	callD    = "3e7a5d84-af4b-4c6d-8a5e-9b1c4d5e6f7a"
	callE    = "4f8b6e95-ba5c-4d7e-9b6f-0c2d5e6f7a8b"
	callF    = "5a9c7fa6-cb6d-4e8f-8c7a-1d3e6f7a8b9c"
	callG    = "6bad80b7-dc7e-4f9a-9d8b-2e4f7a8b9cad"
	callH    = "7cbe91c8-ed8f-4a0b-8e9c-3f5a8b9cadbe"
	callI    = "8dcfa2d9-fe9a-4b1c-9fad-4a6b9cadbecf"
	callJ    = "9ed0b3ea-0fab-4c2d-8abe-5b7cadbecfd0"
	callK    = "afe1c4fb-1abc-4d3e-9bcf-6c8dbecfd0e1"
	callL    = "b0f2d50c-2bcd-4e4f-8cd0-7d9ecfd0e1f2"
	callM    = "c103e61d-3cde-4f5a-9de1-8eafd0e1f203"
	callN    = "d214f72e-4def-4a6b-8ef2-9fb0e1f20314"
	callO    = "e325083f-5ef0-4b7c-9f03-a0c1f2031425"
	// Longer than the second the greeting waits for the start report.
	slowStartDelay = 1500 * time.Millisecond
	// The start report, which the fake server logs among the call reports.
	start = "start"
)

type report struct {
	CallID  string `json:"-"`
	Line    string `json:"-"`
	Outcome string `json:"outcome"`
	Reason  string `json:"reason"`
	Error   string `json:"error"`
}

type lineReport struct {
	Username   string  `json:"username"`
	Registered bool    `json:"registered"`
	Error      *string `json:"error"`
}

// connection is one open /v1/voice/events stream; the test writes its events.
type connection struct {
	query  string
	events chan string
	drop   chan struct{}
}

// server plays the Crumble server's voice gateway routes.
type server struct {
	connections chan *connection
	reports     chan report
	lines       chan []lineReport

	mu sync.Mutex
	// statuses holds the answers to successive "callId outcome" reports; 204 once they run out.
	statuses map[string][]int
	// slowStart is the call whose start report takes longer than the greeting waits for it.
	slowStart string
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer "+token {
		w.WriteHeader(http.StatusUnauthorized)

		return
	}

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/voice/events":
		s.stream(w, r)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/voice/calls/"):
		var got report
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		got.CallID = strings.TrimPrefix(r.URL.Path, "/v1/voice/calls/")
		s.reports <- got

		w.WriteHeader(s.status(got.CallID + " " + got.Outcome))
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/voice/lines/") && strings.Contains(r.URL.Path, "/calls/"):
		s.start(w, r)
	case r.Method == http.MethodPut && r.URL.Path == "/v1/voice/lines":
		var got struct {
			Lines []lineReport `json:"lines"`
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil || got.Lines == nil {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		s.lines <- got.Lines

		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// start logs a start report among the call reports, before it answers.
func (s *server) start(w http.ResponseWriter, r *http.Request) {
	line, callID, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/v1/voice/lines/"), "/calls/")
	if body, _ := io.ReadAll(r.Body); string(body) != "{}" {
		w.WriteHeader(http.StatusBadRequest)

		return
	}

	if callID == s.slowStart {
		time.Sleep(slowStartDelay)
	}

	s.reports <- report{CallID: callID, Line: line, Outcome: start, Reason: "", Error: ""}

	w.WriteHeader(s.status(callID + " " + start))
}

func (s *server) status(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	queued := s.statuses[key]
	if len(queued) == 0 {
		return http.StatusNoContent
	}

	s.statuses[key] = queued[1:]

	return queued[0]
}

func (s *server) stream(w http.ResponseWriter, r *http.Request) {
	open := &connection{query: r.URL.RawQuery, events: make(chan string, 8), drop: make(chan struct{})}
	s.connections <- open

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	flush(w)

	for {
		select {
		case event := <-open.events:
			_, _ = io.WriteString(w, event)
			flush(w)
		case <-open.drop:
			return
		case <-r.Context().Done():
			return
		}
	}
}

func flush(w http.ResponseWriter) {
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func event[T map[string]string | map[string]any](name string, data T) string {
	encoded, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	return "event: " + name + "\ndata: " + string(encoded) + "\n\n"
}

func configEvent(usernames ...string) string {
	lines := make([]map[string]any, 0, len(usernames))
	for _, username := range usernames {
		lines = append(lines, map[string]any{
			"username": username, "botId": "bot_" + username, "botName": username, "displayName": username, "callsOwner": true,
		})
	}

	return event("config", map[string]any{"enabled": true, "ownerNumbers": []string{answers, rings, stays}, "lines": lines})
}

func callEvent(callID, username, to string) string {
	return event("call", map[string]string{"callId": callID, "username": username, "to": to, "greeting": "Hi, your build finished."})
}

// phone plays the registrar and the owner's phones behind it.
type phone struct {
	mu      sync.Mutex
	invites map[string]int
	// answered receives the line each answered call came from, once its greeting is heard.
	answered chan string
	// ringing receives a value when the ringing phone starts to ring.
	ringing chan struct{}
	// ended receives the number of a call the gateway cancelled or hung up.
	ended chan string
}

func (p *phone) invitesTo(number string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.invites[number]
}

func startPhone(ctx context.Context, t *testing.T) *phone {
	t.Helper()

	ua, err := sipgo.NewUA(sipgo.WithUserAgent("registrar"))
	if err != nil {
		t.Fatalf("registrar UA: %v", err)
	}

	sipServer, err := sipgo.NewServer(ua)
	if err != nil {
		t.Fatalf("registrar server: %v", err)
	}

	sipServer.OnRegister(func(request *sip.Request, tx sip.ServerTransaction) {
		_ = tx.Respond(sip.NewResponseFromRequest(request, sip.StatusOK, "OK", nil))
	})

	p := &phone{invites: map[string]int{}, answered: make(chan string, 8), ringing: make(chan struct{}, 1), ended: make(chan string, 8)}

	stack := diago.NewDiago(ua, diago.WithServer(sipServer),
		diago.WithTransport(diago.Transport{Transport: "udp", BindHost: "127.0.0.1", BindPort: registrarPort}),
		diago.WithLogger(slog.New(slog.DiscardHandler)))

	err = stack.ServeBackground(ctx, func(dialog *diago.DialogServerSession) { p.ring(ctx, dialog) })
	if err != nil {
		t.Fatalf("registrar serve: %v", err)
	}

	return p
}

func (p *phone) ring(ctx context.Context, dialog *diago.DialogServerSession) {
	number := dialog.ToUser()

	p.mu.Lock()
	p.invites[number]++
	p.mu.Unlock()

	waitForEnd := func() {
		select {
		case <-dialog.Context().Done():
			p.ended <- number
		case <-time.After(10 * time.Second):
		}
	}

	switch number {
	case rings:
		_ = dialog.Ringing()
		p.ringing <- struct{}{}

		waitForEnd()
	case answers, stays:
		session, err := dialog.Answer(diago.AnswerOptions{})
		if err != nil {
			return
		}

		reader, err := session.AudioReader()
		if err != nil {
			return
		}

		if _, err := reader.Read(make([]byte, 1500)); err == nil {
			p.answered <- dialog.FromUser()
		}

		if number == answers {
			_ = dialog.Hangup(ctx)

			return
		}

		waitForEnd()
	default:
		_ = dialog.Respond(sip.StatusNotFound, "Not Found", nil)
	}
}

type transcription struct{ events chan speech.Event }

func (t *transcription) Write([]byte) error          { return nil }
func (t *transcription) Events() <-chan speech.Event { return t.events }
func (t *transcription) Err() error                  { return nil }
func (t *transcription) Close() error                { return nil }

type transcriber struct{}

//nolint:ireturn // The fake implements speech.Transcriber.
func (transcriber) Transcribe(context.Context) (speech.Transcription, error) {
	events := make(chan speech.Event, 1)
	events <- speech.Event{Kind: speech.Utterance, Text: "hello"}

	return &transcription{events: events}, nil
}

type synthesizer struct{}

func (synthesizer) Synthesize(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(bytes.Repeat([]byte{0x10, 0x20}, audio.FrameSamples*25))), nil
}

// bot records the call ID of every spoken message, and answers one on callJ as the
// server does on a call it ended.
type bot struct {
	mu      sync.Mutex
	callIDs []string
}

func (b *bot) Turn(_ context.Context, _, callID, _ string, _ turn.Observer) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.callIDs = append(b.callIDs, callID)

	if callID == callJ {
		return nil, fmt.Errorf("send message: %w", crumble.ErrCallEnded)
	}

	return []string{"Hi."}, nil
}

func (b *bot) calls() []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	return slices.Clone(b.callIDs)
}

func testConfig() *config.Config {
	loopback := netip.MustParseAddr("127.0.0.1")
	accounts := make([]config.Account, 0, 4)

	for _, username := range []string{"ada", "bea", "cid", "dee"} {
		accounts = append(accounts, config.Account{Username: username, Password: "secret", Voice: "marin"})
	}

	return &config.Config{
		SIP: config.SIP{
			Registrar: "127.0.0.1", Transport: "udp", BindHost: "127.0.0.1", BindPort: gatewayPort,
			RTPPortStart: 26200, RTPPortEnd: 26300, RegisterExpiry: time.Minute,
			RegistrarHost: "127.0.0.1", RegistrarPort: registrarPort, RegistrarIPs: []netip.Addr{loopback},
			Accounts: accounts,
		},
		Call: config.Call{
			MaxDuration: time.Minute, TurnTimeout: 10 * time.Second, MaxConcurrent: 4,
			HoldingPhrase: "One moment.", HoldingDelay: time.Minute, TimeoutPhrase: "Later.",
			AnswerGreeting: "Hi, this is {name}.", FailurePhrase: "Sorry.", UnansweredAfter: 5 * time.Second,
		},
	}
}

type harness struct {
	t       *testing.T
	ctx     context.Context //nolint:containedctx // The test's deadline bounds every wait.
	server  *server
	reports []report
}

//nolint:ireturn // T is the channel's element type, not an interface.
func receive[T any](h *harness, from <-chan T, failure string) T {
	h.t.Helper()

	select {
	case value := <-from:
		return value
	case <-h.ctx.Done():
		h.t.Fatal(failure)

		var zero T

		return zero
	}
}

// awaitReport reads call reports until one for callID with outcome arrives.
func (h *harness) awaitReport(callID, outcome string) report {
	h.t.Helper()

	for {
		got := receive(h, h.server.reports, "no "+outcome+" report for "+callID)
		h.reports = append(h.reports, got)

		if got.CallID == callID && got.Outcome == outcome {
			return got
		}
	}
}

// awaitTotal reads call reports until n for callID with outcome arrived, counting earlier ones.
func (h *harness) awaitTotal(callID, outcome string, n int) {
	h.t.Helper()

	count := func() int {
		return len(slices.DeleteFunc(slices.Clone(h.reports), func(r report) bool { return r.CallID != callID || r.Outcome != outcome }))
	}

	for count() < n {
		h.reports = append(h.reports, receive(h, h.server.reports, fmt.Sprintf("no %s report %d for %s", outcome, n, callID)))
	}
}

// drain collects the call reports already sent.
func (h *harness) drain() {
	for {
		select {
		case got := <-h.server.reports:
			h.reports = append(h.reports, got)
		default:
			return
		}
	}
}

// outcomes lists a call's reports except ended, which the call's own end times.
func (h *harness) outcomes(callID string) []string {
	var outcomes []string

	for _, got := range h.reports {
		if got.CallID == callID && got.Outcome != crumble.OutcomeEnded {
			outcomes = append(outcomes, got.Outcome)
		}
	}

	return outcomes
}

// endings lists the reasons of a call's ended reports.
func (h *harness) endings(callID string) []string {
	var reasons []string

	for _, got := range h.reports {
		if got.CallID == callID && got.Outcome == crumble.OutcomeEnded {
			reasons = append(reasons, got.Reason)
		}
	}

	return reasons
}

// awaitLines reads line reports until one equals want, written as "username:registered:error".
func (h *harness) awaitLines(want ...string) {
	h.t.Helper()

	for {
		lines := receive(h, h.server.lines, fmt.Sprintf("no line report %q", want))
		got := make([]string, 0, len(lines))

		for _, line := range lines {
			reason := ""
			if line.Error != nil {
				reason = *line.Error
			}

			got = append(got, fmt.Sprintf("%s:%t:%s", line.Username, line.Registered, reason))
		}

		if slices.Equal(got, want) {
			return
		}
	}
}

func (h *harness) connect() *connection {
	h.t.Helper()

	open := receive(h, h.server.connections, "the gateway did not open the events stream")
	if open.query != "calls=true" {
		h.t.Fatalf("events stream query = %q, want calls=true", open.query)
	}

	return open
}

//nolint:paralleltest // Binds fixed SIP and RTP ports.
func TestCallsFromTheServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	fakeBot := &bot{}
	pipeline := call.Pipeline{Transcriber: transcriber{}, Synthesizer: synthesizer{}}
	fake := &server{
		connections: make(chan *connection, 2), reports: make(chan report, 64), lines: make(chan []lineReport, 64),
		statuses: map[string][]int{
			callD + " answered": {http.StatusNotFound},
			callE + " ringing":  {http.StatusNoContent, http.StatusNotFound},
			callF + " ringing":  {http.StatusUnauthorized},
			callG + " answered": {http.StatusServiceUnavailable, http.StatusServiceUnavailable},
			callI + " start":    {http.StatusConflict},
			callK + " start":    {http.StatusNotFound},
			callL + " start":    {http.StatusNoContent, http.StatusConflict},
			callM + " ended":    {http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusServiceUnavailable},
			callO + " answered": {http.StatusNotFound},
		},
		slowStart: callO,
	}
	httpServer := httptest.NewServer(fake)
	t.Cleanup(httpServer.Close)

	base, _ := url.Parse(httpServer.URL)
	client := crumble.NewClient(base, token, httpServer.Client())
	calls := bridge.NewCalls(client, slog.New(slog.DiscardHandler))

	// New sets the media package's port range, so it runs before any other SIP stack starts.
	gateway, err := sipline.New(testConfig(), call.Services{Speech: nil, Bot: fakeBot, Log: slog.New(slog.DiscardHandler)},
		sipline.Options{SpeechFor: func(string) call.Speech { return pipeline }, StrangerRecording: nil, Calls: calls})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// The phone outlives the gateway, as a real one does, so it answers the gateway's shutdown.
	peerCtx, stopPeer := context.WithCancel(context.Background())
	defer stopPeer()

	owner := startPhone(peerCtx, t)
	h := &harness{t: t, ctx: ctx, server: fake, reports: nil}

	gatewayDone := make(chan error, 1)
	bridgeDone := make(chan error, 1)

	go func() { gatewayDone <- gateway.Run(ctx) }()
	go func() { bridgeDone <- bridge.Run(ctx, gateway, client, calls, slog.New(slog.DiscardHandler)) }()

	first := h.connect()

	t.Run("line registration is reported, and again after a config change", func(*testing.T) {
		first.events <- configEvent("ada", "bea", "cid", "dee", "bob")
		h.awaitLines("ada:true:", "bea:true:", "cid:true:", "dee:true:", "bob:false:no SIP account with this username in sip.accounts")

		first.events <- configEvent("ada", "bea", "cid", "dee")
		h.awaitLines("ada:true:", "bea:true:", "cid:true:", "dee:true:")
	})

	t.Run("a call is acknowledged, dialed, started before its greeting, answered, and ended", func(t *testing.T) {
		placedCall(t, h, owner, first)
	})

	var second *connection

	t.Run("a duplicate or re-sent call does not dial again, and a stranger's number is refused", func(t *testing.T) {
		first.events <- callEvent(callA, "ada", answers)
		close(first.drop)

		second = h.connect()
		second.events <- configEvent("ada", "bea", "cid", "dee")
		second.events <- callEvent(callA, "ada", answers)
		second.events <- callEvent(callB, "ada", stranger)

		// Events are handled in order, so B's report comes after both copies of A were seen.
		refused := h.awaitReport(callB, crumble.OutcomeFailed)
		if !strings.Contains(refused.Error, "owner's numbers") {
			t.Errorf("refusal error = %q, want it to name the owner's numbers", refused.Error)
		}

		// The gateway's own owner check would acknowledge first; this one refuses before ringing.
		if got := h.outcomes(callB); !slices.Equal(got, []string{crumble.OutcomeFailed}) {
			t.Errorf("call B reports = %q, want only failed", got)
		}

		if dialed := owner.invitesTo(answers) + owner.invitesTo(stranger); dialed != 1 {
			t.Errorf("dialed %d times, want call A once and never the stranger", dialed)
		}

		if got := h.outcomes(callA); !slices.Equal(got, []string{crumble.OutcomeRinging, start, crumble.OutcomeAnswered}) {
			t.Errorf("call A reports = %q, want ringing, the start, and answered once", got)
		}
	})

	t.Run("cancel hangs up a ringing call", func(t *testing.T) {
		second.events <- callEvent(callC, "bea", rings)
		h.awaitReport(callC, crumble.OutcomeRinging)
		receive(h, owner.ringing, "the owner's phone never rang")

		second.events <- event("cancel", map[string]string{"callId": callC})

		if ended := receive(h, owner.ended, "the ringing call was not cancelled"); ended != rings {
			t.Errorf("ended the call to %q, want the ringing one", ended)
		}
	})

	t.Run("a 404 on the report hangs up", func(t *testing.T) {
		second.events <- callEvent(callD, "cid", stays)
		h.awaitReport(callD, crumble.OutcomeAnswered)

		if ended := receive(h, owner.ended, "the gateway kept the call the server dropped"); ended != stays {
			t.Errorf("ended the call to %q, want the answered one", ended)
		}

		if got := h.outcomes(callC); !slices.Equal(got, []string{crumble.OutcomeRinging}) {
			t.Errorf("call C reports = %q, want only ringing: a cancelled call is not reported", got)
		}
	})

	t.Run("after a reconnect, ringing calls are checked, active calls confirmed, and a lost end report sent again", func(t *testing.T) {
		second = recheckAfterReconnect(t, h, owner, second)
	})
	t.Run("an outcome report is retried after a server error", func(t *testing.T) { retriedReport(t, h, owner, second) })
	t.Run("a refused token on the ringing report does not dial", func(t *testing.T) { refusedToken(t, h, owner, second) })
	t.Run("a 409 on the start or on a spoken message hangs up; a 404 on the start does not", func(t *testing.T) {
		endedByServer(t, h, owner, second)
	})

	t.Run("a slow start report still reaches the server before answered", func(t *testing.T) {
		slowStart(t, h, owner, second)
	})

	t.Run("calls at shutdown are reported as failed when ringing and as stopping when talking", func(t *testing.T) {
		shutdownWhileRinging(t, h, owner, second, func() error {
			cancel()

			return errors.Join(<-bridgeDone, <-gatewayDone)
		})
	})

	checkCallIDs(t, h, fakeBot)
}

func checkCallIDs(t *testing.T, h *harness, fakeBot *bot) {
	t.Helper()

	for _, callID := range []string{callI, callJ} {
		if got := h.endings(callID); len(got) != 0 {
			t.Errorf("call %s ended = %q, want no report for a call the server ended", callID, got)
		}
	}

	known := []string{callA, callD, callG, callJ, callK, callL, callM, callN, callO}
	for _, callID := range fakeBot.calls() {
		if !slices.Contains(known, callID) {
			t.Errorf("spoken message with call ID %q, want the call event's ID", callID)
		}
	}
}

func placedCall(t *testing.T, h *harness, owner *phone, open *connection) {
	t.Helper()

	open.events <- callEvent(callA, "ada", answers)
	if from := receive(h, owner.answered, "the owner heard no greeting"); from != "ada" {
		t.Errorf("call came from %q, want the line ada", from)
	}

	// The server logs a start before answering it, and the greeting waits for the answer.
	h.drain()

	if got := h.outcomes(callA); len(got) < 2 || !slices.Equal(got[:2], []string{crumble.OutcomeRinging, start}) {
		t.Errorf("call A reports when the owner heard the greeting = %q, want ringing and the start", got)
	}

	h.awaitReport(callA, crumble.OutcomeEnded)

	if got := h.outcomes(callA); !slices.Equal(got, []string{crumble.OutcomeRinging, start, crumble.OutcomeAnswered}) {
		t.Errorf("call A reports = %q, want ringing, the start, then answered", got)
	}

	if got := h.endings(callA); !slices.Equal(got, []string{crumble.ReasonHungUp}) {
		t.Errorf("call A ended = %q, want hung up once", got)
	}

	if line := h.reports[slices.IndexFunc(h.reports, func(r report) bool { return r.Outcome == start })].Line; line != "ada" {
		t.Errorf("start reported for the line %q, want ada", line)
	}
}

// drainAnswered drops greetings heard on calls whose end the test already saw.
func drainAnswered(owner *phone) {
	for {
		select {
		case <-owner.answered:
		default:
			return
		}
	}
}

func endedByServer(t *testing.T, h *harness, owner *phone, open *connection) {
	t.Helper()

	drainAnswered(owner)
	open.events <- callEvent(callI, "bea", stays)

	if ended := receive(h, owner.ended, "the gateway kept a call whose start got 409"); ended != stays {
		t.Errorf("ended the call to %q, want the answered one", ended)
	}

	// The owner's phone signals a heard greeting before the call's end.
	select {
	case <-owner.answered:
		t.Error("the owner heard the greeting on a call the server ended at its start")
	default:
	}

	open.events <- callEvent(callJ, "cid", stays)

	if ended := receive(h, owner.ended, "the gateway kept a call whose spoken message got 409"); ended != stays {
		t.Errorf("ended the call to %q, want the answered one", ended)
	}

	drainAnswered(owner)
	open.events <- callEvent(callK, "ada", answers)
	receive(h, owner.answered, "the owner heard no greeting after a 404 on the start")
	h.awaitReport(callK, crumble.OutcomeEnded)

	if got := h.endings(callK); !slices.Equal(got, []string{crumble.ReasonHungUp}) {
		t.Errorf("call K ended = %q, want the owner's hang-up: a 404 on the start keeps the call", got)
	}
}

func slowStart(t *testing.T, h *harness, owner *phone, open *connection) {
	t.Helper()

	drainAnswered(owner)
	open.events <- callEvent(callO, "ada", stays)
	receive(h, owner.answered, "the owner heard no greeting while the start report was slow")

	// The 404 on answered hangs up, which ends the call for the next test.
	h.awaitReport(callO, crumble.OutcomeAnswered)

	if got := h.outcomes(callO); !slices.Equal(got, []string{crumble.OutcomeRinging, start, crumble.OutcomeAnswered}) {
		t.Errorf("call O reports = %q, want ringing, the start, then answered", got)
	}

	if ended := receive(h, owner.ended, "the gateway kept the call the server dropped"); ended != stays {
		t.Errorf("ended the call to %q, want the answered one", ended)
	}
}

func recheckAfterReconnect(t *testing.T, h *harness, owner *phone, open *connection) *connection {
	t.Helper()

	open.events <- callEvent(callE, "bea", rings)
	open.events <- callEvent(callL, "cid", stays)
	open.events <- callEvent(callM, "dee", answers)
	h.awaitTotal(callE, crumble.OutcomeRinging, 1)
	receive(h, owner.ringing, "the owner's phone never rang")
	h.awaitTotal(callL, start, 1)
	h.awaitTotal(callM, crumble.OutcomeEnded, 3)

	close(open.drop)

	next := h.connect()
	next.events <- configEvent("ada", "bea", "cid", "dee")
	h.awaitTotal(callE, crumble.OutcomeRinging, 2)
	h.awaitTotal(callL, start, 2)
	h.awaitTotal(callM, crumble.OutcomeEnded, 4)

	// The ringing call gets 404 on its ringing report, the active one 409 on its start.
	ended := []string{
		receive(h, owner.ended, "the gateway kept a call the server dropped while disconnected"),
		receive(h, owner.ended, "the gateway kept a call the server dropped while disconnected"),
	}
	slices.Sort(ended)

	if want := []string{rings, stays}; !slices.Equal(ended, want) {
		t.Errorf("ended the calls to %q, want %q", ended, want)
	}

	if got := h.endings(callM); !slices.Equal(got, slices.Repeat([]string{crumble.ReasonHungUp}, 4)) {
		t.Errorf("call M ended = %q, want three attempts and one more after the reconnect", got)
	}

	drainAnswered(owner)

	return next
}

func retriedReport(t *testing.T, h *harness, owner *phone, open *connection) {
	t.Helper()

	open.events <- callEvent(callG, "ada", answers)

	for range 3 {
		h.awaitReport(callG, crumble.OutcomeAnswered)
	}

	want := []string{crumble.OutcomeRinging, start, crumble.OutcomeAnswered, crumble.OutcomeAnswered, crumble.OutcomeAnswered}
	if got := h.outcomes(callG); !slices.Equal(got, want) {
		t.Errorf("call G reports = %q, want %q", got, want)
	}

	receive(h, owner.answered, "the owner heard no greeting")
}

func refusedToken(t *testing.T, h *harness, owner *phone, open *connection) {
	t.Helper()

	before := owner.invitesTo(answers)
	open.events <- callEvent(callF, "ada", answers)
	h.awaitReport(callF, crumble.OutcomeRinging)

	// Long enough for a dial and its answered report; nothing should follow.
	time.Sleep(time.Second)
	h.drain()

	if dialed := owner.invitesTo(answers) - before; dialed != 0 || !slices.Equal(h.outcomes(callF), []string{crumble.OutcomeRinging}) {
		t.Errorf("dialed %d times with reports %q, want no dial after a 401", dialed, h.outcomes(callF))
	}
}

func shutdownWhileRinging(t *testing.T, h *harness, owner *phone, open *connection, stop func() error) {
	t.Helper()

	open.events <- callEvent(callN, "ada", stays)
	h.awaitReport(callN, crumble.OutcomeAnswered)
	receive(h, owner.answered, "the owner heard no greeting")

	open.events <- callEvent(callH, "dee", rings)
	h.awaitReport(callH, crumble.OutcomeRinging)
	receive(h, owner.ringing, "the owner's phone never rang")

	started := time.Now()

	if err := stop(); err != nil {
		t.Errorf("Run: %v", err)
	}

	if took := time.Since(started); took > 3*time.Second {
		t.Errorf("shutdown took %s with a ringing call, want under 3s", took.Round(100*time.Millisecond))
	}

	for range 2 {
		select {
		case <-owner.ended:
		case <-time.After(3 * time.Second):
			t.Error("the owner's phone kept ringing or talking after the gateway stopped")
		}
	}

	h.drain()

	if got := h.outcomes(callH); !slices.Equal(got, []string{crumble.OutcomeRinging, crumble.OutcomeFailed}) {
		t.Errorf("call H reports = %q, want ringing and failed", got)
	}

	if got := h.endings(callN); !slices.Equal(got, []string{crumble.ReasonStopping}) {
		t.Errorf("call N ended = %q, want stopping, reported before Run returned", got)
	}
}
