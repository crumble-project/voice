package sipline_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/diago"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/call"
	"github.com/crumble-project/crumble-voice/internal/config"
	"github.com/crumble-project/crumble-voice/internal/sipline"
	"github.com/crumble-project/crumble-voice/internal/speech"
	"github.com/crumble-project/crumble-voice/internal/turn"
)

const (
	registrarPort = 25060
	gatewayPort   = 25070
	callerPort    = 25080
	owner         = "**610"
)

type transcription struct{ events chan speech.Event }

func (t *transcription) Write([]byte) error          { return nil }
func (t *transcription) Events() <-chan speech.Event { return t.events }
func (t *transcription) Err() error                  { return nil }
func (t *transcription) Close() error                { return nil }

// transcriber hears "hello" as soon as a call starts, or fails to start while fail is set.
type transcriber struct{ fail *atomic.Bool }

var errTranscriber = errors.New("transcription is down")

//nolint:ireturn // The fake implements speech.Transcriber.
func (t transcriber) Transcribe(context.Context) (speech.Transcription, error) {
	if t.fail != nil && t.fail.Load() {
		return nil, errTranscriber
	}

	events := make(chan speech.Event, 1)
	events <- speech.Event{Kind: speech.Utterance, Text: "hello"}

	return &transcription{events: events}, nil
}

// synthesizer records what the gateway says.
type synthesizer struct {
	mu   sync.Mutex
	said []string
}

func (s *synthesizer) Synthesize(_ context.Context, text string) (io.ReadCloser, error) {
	s.mu.Lock()
	s.said = append(s.said, text)
	s.mu.Unlock()

	return io.NopCloser(bytes.NewReader(bytes.Repeat([]byte{0x10, 0x20}, audio.FrameSamples*25))), nil
}

func (s *synthesizer) spoken() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string(nil), s.said...)
}

type bot struct {
	mu       sync.Mutex
	messages []string
}

func (b *bot) Turn(_ context.Context, _, _, content string, _ turn.Observer) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.messages = append(b.messages, content)

	return []string{"Hi."}, nil
}

func (b *bot) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.messages)
}

// calls records each call's start with how much was said before it, and each end.
type calls struct {
	voice   *synthesizer
	started chan started
	ended   chan sipline.EndReason

	mu sync.Mutex
	// refuse answers the next start as the server would a call it ended.
	refuse bool
}

type started struct {
	line   string
	callID string
	said   int
}

func (c *calls) Started(_ context.Context, username, callID string, hangUp func()) {
	c.mu.Lock()
	refuse := c.refuse
	c.refuse = false
	c.mu.Unlock()

	c.started <- started{line: username, callID: callID, said: len(c.voice.spoken())}

	if refuse {
		hangUp()
	}
}

func (c *calls) Ended(_ string, reason sipline.EndReason) { c.ended <- reason }

func (c *calls) refuseNext() {
	c.mu.Lock()
	c.refuse = true
	c.mu.Unlock()
}

//nolint:ireturn // T is the channel's element type, not an interface.
func next[T any](ctx context.Context, t *testing.T, from <-chan T, failure string) T {
	t.Helper()

	select {
	case value := <-from:
		return value
	case <-ctx.Done():
		t.Fatal(failure)

		var zero T

		return zero
	}
}

// saidFirst checks that the greeting was the first thing said after the call started.
func saidFirst(t *testing.T, voice *synthesizer, start started, greeting string) {
	t.Helper()

	if said := voice.spoken(); len(said) <= start.said || said[start.said] != greeting {
		t.Errorf("said after the start report = %q, want the greeting %q first", said[min(start.said, len(said)):], greeting)
	}
}

func testConfig() *config.Config {
	loopback := netip.MustParseAddr("127.0.0.1")

	return &config.Config{
		SIP: config.SIP{
			Registrar: "127.0.0.1", Transport: "udp", BindHost: "127.0.0.1", BindPort: gatewayPort,
			RTPPortStart: 26000, RTPPortEnd: 26100, RegisterExpiry: time.Minute,
			RegistrarHost: "127.0.0.1", RegistrarPort: registrarPort, RegistrarIPs: []netip.Addr{loopback},
			Accounts: []config.Account{
				{Username: "ada", Password: "secret", Voice: "marin"},
				{Username: "eve", Password: "wrong", Voice: "marin"},
			},
		},
		Call: config.Call{
			MaxDuration: time.Second, TurnTimeout: 10 * time.Second, MaxConcurrent: 1,
			HoldingPhrase: "One moment.", HoldingDelay: time.Minute, TimeoutPhrase: "Later.",
			AnswerGreeting: "Hi, this is {name}.",
			FailurePhrase:  "Sorry.", UnansweredAfter: 5 * time.Second,
		},
	}
}

// served is what the server sends: the line ada and the owner's number.
func served() sipline.Setup {
	return sipline.Setup{
		OwnerNumbers: []string{owner},
		Lines:        []sipline.Line{{Username: "ada", BotID: "bot_ada", BotName: "Ada", DisplayName: "Ada"}},
	}
}

// registrar accepts every REGISTER, like a telephone system with the right
// password, and plays the owner's phone for calls to the owner's number.
type registrar struct {
	// registered receives each REGISTER whose From and Contact name the line, as a FRITZ!Box requires.
	registered chan struct{}
	// unregistered receives each REGISTER with Expires: 0.
	unregistered chan struct{}
	// answered receives the caller ID of each call the owner's phone took and heard audio on.
	answered chan string
}

// register answers like a FRITZ!Box: ada registers, eve has a wrong password.
func (r *registrar) register(request *sip.Request, tx sip.ServerTransaction) {
	signal := func(events chan struct{}) {
		select {
		case events <- struct{}{}:
		default:
		}
	}

	if expires := request.GetHeader("Expires"); expires != nil && expires.Value() == "0" {
		_ = tx.Respond(sip.NewResponseFromRequest(request, sip.StatusOK, "OK", nil))
		signal(r.unregistered)

		return
	}

	if request.From().Address.User == "eve" {
		_ = tx.Respond(sip.NewResponseFromRequest(request, sip.StatusForbidden, "Forbidden", nil))

		return
	}

	if request.From().Address.User != "ada" || request.Contact() == nil || request.Contact().Address.User != "ada" {
		return // A FRITZ!Box sends no answer at all.
	}

	_ = tx.Respond(sip.NewResponseFromRequest(request, sip.StatusOK, "OK", nil))
	signal(r.registered)
}

func startRegistrar(ctx context.Context, t *testing.T) *registrar {
	t.Helper()

	ua, err := sipgo.NewUA(sipgo.WithUserAgent("registrar"))
	if err != nil {
		t.Fatalf("registrar UA: %v", err)
	}

	server, err := sipgo.NewServer(ua)
	if err != nil {
		t.Fatalf("registrar server: %v", err)
	}

	r := &registrar{
		registered: make(chan struct{}, 16), unregistered: make(chan struct{}, 16), answered: make(chan string, 1),
	}

	server.OnRegister(r.register)

	phone := diago.NewDiago(ua, diago.WithServer(server),
		diago.WithTransport(diago.Transport{Transport: "udp", BindHost: "127.0.0.1", BindPort: registrarPort}),
		diago.WithLogger(slog.New(slog.DiscardHandler)))

	err = phone.ServeBackground(ctx, func(dialog *diago.DialogServerSession) {
		if dialog.ToUser() != owner {
			_ = dialog.Respond(sip.StatusNotFound, "Not Found", nil)

			return
		}

		session, answerErr := dialog.Answer(diago.AnswerOptions{})
		if answerErr != nil {
			return
		}

		reader, readerErr := session.AudioReader()
		if readerErr != nil {
			return
		}

		// The greeting is the first audio the owner hears.
		if _, readErr := reader.Read(make([]byte, 1500)); readErr == nil {
			r.answered <- dialog.FromUser()
		}

		_ = dialog.Hangup(ctx)
	})
	if err != nil {
		t.Fatalf("registrar serve: %v", err)
	}

	return r
}

func caller(t *testing.T) *diago.Diago {
	t.Helper()

	ua, err := sipgo.NewUA(sipgo.WithUserAgent("caller"))
	if err != nil {
		t.Fatalf("caller UA: %v", err)
	}

	t.Cleanup(func() { _ = ua.Close() })

	return diago.NewDiago(ua,
		diago.WithTransport(diago.Transport{Transport: "udp", BindHost: "127.0.0.1", BindPort: callerPort}),
		diago.WithLogger(slog.New(slog.DiscardHandler)))
}

func invite(ctx context.Context, phone *diago.Diago, from string) (*diago.DialogClientSession, *diago.DialogMedia, error) {
	header := &sip.FromHeader{Address: sip.Uri{User: from, Host: "127.0.0.1"}, Params: sip.NewParams()}

	dialog, session, err := phone.Invite(ctx, sip.Uri{User: "ada", Host: "127.0.0.1", Port: gatewayPort},
		diago.InviteOptions{Headers: []sip.Header{header}})
	if err != nil {
		return nil, nil, fmt.Errorf("invite: %w", err)
	}

	return dialog, session, nil
}

// inviteOwner calls the line as the owner once the previous call released the bot.
func inviteOwner(ctx context.Context, t *testing.T, phone *diago.Diago) *diago.DialogClientSession {
	t.Helper()

	for {
		dialog, _, err := invite(ctx, phone, owner)

		var refused *sipgo.ErrDialogResponse
		if errors.As(err, &refused) && refused.Res.StatusCode == sip.StatusBusyHere && ctx.Err() == nil {
			time.Sleep(50 * time.Millisecond)

			continue
		}

		if err != nil {
			t.Fatalf("invite: %v", err)
		}

		t.Cleanup(func() { _ = dialog.Close() })

		return dialog
	}
}

func hungUpByGateway(ctx context.Context, t *testing.T, dialog *diago.DialogClientSession) {
	t.Helper()

	select {
	case <-dialog.Context().Done():
	case <-ctx.Done():
		t.Fatal("the gateway did not hang up")
	}
}

//nolint:paralleltest // Binds fixed SIP and RTP ports.
func TestCalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fake := &bot{}
	voice := &synthesizer{}
	broken := &atomic.Bool{}
	reports := &calls{voice: voice, started: make(chan started, 16), ended: make(chan sipline.EndReason, 16)}

	// New sets the media package's port range, so it runs before any other SIP stack starts.
	gateway, err := sipline.New(testConfig(), call.Services{Speech: nil, Bot: fake, Log: slog.New(slog.DiscardHandler)},
		sipline.Options{
			SpeechFor: func(string) call.Speech {
				return call.Pipeline{Transcriber: transcriber{fail: broken}, Synthesizer: voice}
			},
			// Half a second of a recognisable non-silent pattern.
			StrangerRecording: bytes.Repeat([]byte{0x00, 0x30}, audio.TelephoneRate/2),
			Calls:             reports,
		})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// The registrar outlives the gateway, as a real one does, so it answers the gateway's shutdown.
	peerCtx, stopPeer := context.WithCancel(context.Background())
	defer stopPeer()

	fakeRegistrar := startRegistrar(peerCtx, t)
	stopped := make(chan error, 1)

	go func() { stopped <- gateway.Run(ctx) }()

	gateway.Apply(served())
	waitFor(ctx, t, fakeRegistrar.registered, "the line never registered")

	phone := caller(t)

	t.Run("stranger hears the recording, not the bot", func(t *testing.T) { strangerHearsRecording(ctx, t, phone, fake) })
	t.Run("owner reaches the bot and hears the reply", func(t *testing.T) {
		ownerReachesBot(ctx, t, phone, fake)

		if said := voice.spoken(); len(said) == 0 || said[0] != "Hi, this is Ada." {
			t.Errorf("said = %q, want the greeting with the bot's name first", said)
		}

		start := next(ctx, t, reports.started, "the call start was not reported")
		if start.line != "ada" || start.callID == "" {
			t.Errorf("start = %+v, want the line ada with a call ID", start)
		}

		saidFirst(t, voice, start, "Hi, this is Ada.")

		if reason := next(ctx, t, reports.ended, "the call end was not reported"); reason != sipline.EndHungUp {
			t.Errorf("end reason = %d, want hung up", reason)
		}
	})
	t.Run("bot calls the owner", func(t *testing.T) { botCallsOwner(ctx, t, gateway, fakeRegistrar, reports) })
	t.Run("bot cannot call strangers", func(t *testing.T) {
		if _, dialErr := gateway.Dial(ctx, outbound("ada", "999")); !errors.Is(dialErr, sipline.ErrNotOwner) {
			t.Fatalf("err = %v, want ErrNotOwner", dialErr)
		}
	})
	t.Run("a new config drops and adds lines", func(t *testing.T) { reconfigure(ctx, t, gateway, fakeRegistrar) })
	t.Run("a refused line leaves the others running", func(t *testing.T) { refusedLine(ctx, t, gateway, fakeRegistrar, reports) })
	t.Run("the server ending the call at its start hangs up before the greeting", func(t *testing.T) {
		endedAtStart(ctx, t, phone, reports)
	})
	t.Run("a failed call and a call at its maximum length report why they ended", func(t *testing.T) {
		broken.Store(true)
		hungUpByGateway(ctx, t, inviteOwner(ctx, t, phone))
		broken.Store(false)
		next(ctx, t, reports.started, "the call start was not reported")

		if reason := next(ctx, t, reports.ended, "the failed call's end was not reported"); reason != sipline.EndFailed {
			t.Errorf("end reason = %d, want failed", reason)
		}

		hungUpByGateway(ctx, t, inviteOwner(ctx, t, phone))
		next(ctx, t, reports.started, "the call start was not reported")

		if reason := next(ctx, t, reports.ended, "the long call's end was not reported"); reason != sipline.EndMaxDuration {
			t.Errorf("end reason = %d, want maximum duration", reason)
		}
	})

	stopDuringACall(ctx, t, phone, reports, func() error {
		cancel()

		return <-stopped
	})
}

func endedAtStart(ctx context.Context, t *testing.T, phone *diago.Diago, reports *calls) {
	t.Helper()

	said := len(reports.voice.spoken())

	reports.refuseNext()
	hungUpByGateway(ctx, t, inviteOwner(ctx, t, phone))
	next(ctx, t, reports.started, "the call start was not reported")

	if reason := next(ctx, t, reports.ended, "the call end was not reported"); reason != sipline.EndByServer {
		t.Errorf("end reason = %d, want ended by the server", reason)
	}

	if after := reports.voice.spoken(); len(after) != said {
		t.Errorf("said %q on a call the server ended", after[said:])
	}
}

func stopDuringACall(ctx context.Context, t *testing.T, phone *diago.Diago, reports *calls, stop func() error) {
	t.Helper()

	// The owner stays on this call until the gateway stops.
	dialog := inviteOwner(ctx, t, phone)
	next(ctx, t, reports.started, "the call start was not reported")

	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	select {
	case reason := <-reports.ended:
		if reason != sipline.EndStopping {
			t.Errorf("end reason = %d, want stopping", reason)
		}
	default:
		t.Error("Run returned before the call's end was reported")
	}

	// stop ended ctx, so the hang-up gets a bound of its own.
	hangupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	hungUpByGateway(hangupCtx, t, dialog)
}

func strangerHearsRecording(ctx context.Context, t *testing.T, phone *diago.Diago, fake *bot) {
	t.Helper()

	dialog, session, err := invite(ctx, phone, "999")
	if err != nil {
		t.Fatalf("invite: %v", err)
	}

	defer func() { _ = dialog.Close() }()

	reader, err := session.AudioReader()
	if err != nil {
		t.Fatalf("audio reader: %v", err)
	}

	if _, err := reader.Read(make([]byte, 1500)); err != nil {
		t.Fatalf("no recording audio: %v", err)
	}

	// The gateway hangs up once the half-second recording ends.
	select {
	case <-dialog.Context().Done():
	case <-time.After(5 * time.Second):
		t.Error("the gateway did not hang up after the recording")
	}

	if fake.count() != 0 {
		t.Errorf("a stranger reached the bot with %d messages", fake.count())
	}
}

func ownerReachesBot(ctx context.Context, t *testing.T, phone *diago.Diago, fake *bot) {
	t.Helper()

	dialog, session, err := invite(ctx, phone, owner)
	if err != nil {
		t.Fatalf("invite: %v", err)
	}

	defer func() { _ = dialog.Close() }()

	reader, err := session.AudioReader()
	if err != nil {
		t.Fatalf("audio reader: %v", err)
	}

	if _, err := reader.Read(make([]byte, 1500)); err != nil {
		t.Fatalf("no reply audio: %v", err)
	}

	if fake.count() != 1 {
		t.Errorf("bot got %d messages, want 1", fake.count())
	}

	_ = dialog.Hangup(ctx)
}

func botCallsOwner(ctx context.Context, t *testing.T, gateway *sipline.Gateway, fakeRegistrar *registrar, reports *calls) {
	t.Helper()

	// The previous call releases the bot once its hangup completes.
	deadline := time.Now().Add(5 * time.Second)

	hangup, err := gateway.Dial(ctx, outbound("ada", owner))
	for errors.Is(err, sipline.ErrBusy) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)

		hangup, err = gateway.Dial(ctx, outbound("ada", owner))
	}

	if err != nil || hangup == nil {
		t.Fatalf("Dial = %v", err)
	}

	// Dial returns only once the start is reported, so the server hears it before the answer.
	var start started

	select {
	case start = <-reports.started:
	default:
		t.Fatal("Dial returned before the call start was reported")
	}

	if want := outbound("ada", owner); start.callID != want.CallID {
		t.Errorf("start call ID = %q, want the call event's %q", start.callID, want.CallID)
	}

	select {
	case from := <-fakeRegistrar.answered:
		if from != "ada" {
			t.Errorf("call came from %q, want the line ada", from)
		}
	case <-ctx.Done():
		t.Fatal("the owner's phone heard nothing")
	}

	if reason := next(ctx, t, reports.ended, "the call end was not reported"); reason != sipline.EndHungUp {
		t.Errorf("end reason = %d, want hung up", reason)
	}

	saidFirst(t, reports.voice, start, "Hello, this is Ada.")
}

func outbound(username, number string) sipline.Outbound {
	return sipline.Outbound{
		CallID: "6f0c7f2e-3a52-4c1e-9b8a-2d5f7c1e9a40", Username: username, Number: number, Greeting: "Hello, this is Ada.",
	}
}

func waitFor(ctx context.Context, t *testing.T, events <-chan struct{}, failure string) {
	t.Helper()

	select {
	case <-events:
	case <-ctx.Done():
		t.Fatal(failure)
	}
}

func reconfigure(ctx context.Context, t *testing.T, gateway *sipline.Gateway, fakeRegistrar *registrar) {
	t.Helper()

	// The outbound call may still be hanging up; the new config waits for it.
	gateway.Apply(sipline.Setup{OwnerNumbers: []string{owner}, Lines: nil})
	waitFor(ctx, t, fakeRegistrar.unregistered, "the dropped line was not unregistered")

	if _, err := gateway.Dial(ctx, outbound("ada", owner)); !errors.Is(err, sipline.ErrUnknownLine) {
		t.Fatalf("Dial after the line was dropped: err = %v, want ErrUnknownLine", err)
	}

	for len(fakeRegistrar.registered) > 0 {
		<-fakeRegistrar.registered
	}

	// bob has no local SIP account, so only ada registers.
	withBob := served()
	withBob.Lines = append(withBob.Lines, sipline.Line{Username: "bob", BotID: "bot_bob", BotName: "Bob"})
	gateway.Apply(withBob)
	waitFor(ctx, t, fakeRegistrar.registered, "the line did not register again")

	if _, err := gateway.Dial(ctx, outbound("bob", owner)); !errors.Is(err, sipline.ErrUnknownLine) {
		t.Fatalf("Dial from a line without a SIP account: err = %v, want ErrUnknownLine", err)
	}
}

func refusedLine(ctx context.Context, t *testing.T, gateway *sipline.Gateway, fakeRegistrar *registrar, reports *calls) {
	t.Helper()

	withEve := served()
	withEve.Lines = append(withEve.Lines, sipline.Line{Username: "eve", BotID: "bot_eve", BotName: "Eve", DisplayName: "Eve"})
	gateway.Apply(withEve)

	for {
		lines := gateway.Lines()
		if len(lines) == 2 && lines[0].Registered && !lines[1].Registered && strings.Contains(lines[1].Error, "403") {
			break
		}

		select {
		case <-gateway.LinesChanged():
		case <-ctx.Done():
			t.Fatalf("lines = %+v, want ada registered and eve refused with the registrar's answer", lines)
		}
	}

	botCallsOwner(ctx, t, gateway, fakeRegistrar, reports)
}

// silentRegistrar rings for every INVITE and answers nothing else, like a registrar
// that went away while a call was ringing: REGISTER and CANCEL get no answer.
func silentRegistrar(ctx context.Context, t *testing.T, port int) <-chan struct{} {
	t.Helper()

	conn, err := (&net.ListenConfig{}).ListenPacket(ctx, "udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	ringing := make(chan struct{}, 1)

	go func() {
		buffer := make([]byte, 65535)

		for {
			n, from, readErr := conn.ReadFrom(buffer)
			if readErr != nil {
				return
			}

			message, parseErr := sip.ParseMessage(buffer[:n])

			request, ok := message.(*sip.Request)
			if parseErr != nil || !ok || request.Method != sip.INVITE {
				continue
			}

			response := sip.NewResponseFromRequest(request, sip.StatusRinging, "Ringing", nil)
			response.AppendHeader(sip.NewHeader("Contact", fmt.Sprintf("<sip:%s@127.0.0.1:%d>", owner, port)))
			_, _ = conn.WriteTo([]byte(response.String()), from)

			select {
			case ringing <- struct{}{}:
			default:
			}
		}
	}()

	return ringing
}

//nolint:paralleltest // Binds fixed SIP and RTP ports.
func TestShutdownWhileACancelGoesUnanswered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cfg := testConfig()
	cfg.SIP.BindPort, cfg.SIP.RegistrarPort = gatewayPort+2, registrarPort+2

	gateway, err := sipline.New(cfg, call.Services{Speech: nil, Bot: &bot{}, Log: slog.New(slog.DiscardHandler)},
		sipline.Options{SpeechFor: func(string) call.Speech { return nil }, StrangerRecording: nil})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ringing := silentRegistrar(ctx, t, registrarPort+2)
	runCtx, stop := context.WithCancel(ctx)
	stopped := make(chan error, 1)

	go func() { stopped <- gateway.Run(runCtx) }()

	gateway.Apply(served())

	for len(gateway.Lines()) == 0 {
		select {
		case <-gateway.LinesChanged():
		case <-ctx.Done():
			t.Fatal("the line was never applied")
		}
	}

	go func() { _, _ = gateway.Dial(runCtx, outbound("ada", owner)) }()

	select {
	case <-ringing:
	case <-ctx.Done():
		t.Fatal("the call never rang")
	}

	stop()

	started := time.Now()

	if err := <-stopped; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if took := time.Since(started); took > 4*time.Second {
		t.Fatalf("Run took %s to return with a ringing call, want a few seconds", took.Round(time.Second))
	}
}
