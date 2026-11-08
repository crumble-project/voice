package sipline_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"sync"
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

// transcriber hears "hello" as soon as a call starts.
type transcriber struct{}

//nolint:ireturn // The fake implements speech.Transcriber.
func (transcriber) Transcribe(context.Context) (speech.Transcription, error) {
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

func (b *bot) Turn(_ context.Context, _, content string, _ turn.Observer) ([]string, error) {
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

func testConfig() *config.Config {
	loopback := netip.MustParseAddr("127.0.0.1")

	return &config.Config{
		SIP: config.SIP{
			Registrar: "127.0.0.1", Transport: "udp", BindHost: "127.0.0.1", BindPort: gatewayPort,
			RTPPortStart: 26000, RTPPortEnd: 26100, RegisterExpiry: time.Minute,
			RegistrarHost: "127.0.0.1", RegistrarPort: registrarPort, RegistrarIPs: []netip.Addr{loopback},
		},
		Owner: config.Owner{Numbers: []string{owner}},
		Lines: []config.Line{{Username: "ada", Password: "secret", DisplayName: "Ada", BotID: "bot_ada"}},
		Call: config.Call{
			MaxDuration: time.Minute, TurnTimeout: 10 * time.Second, MaxConcurrent: 1,
			HoldingPhrase: "One moment.", HoldingDelay: time.Minute, TimeoutPhrase: "Later.",
			AnswerGreeting: "Hi, this is {name}.",
			FailurePhrase:  "Sorry.", UnansweredAfter: 5 * time.Second,
		},
	}
}

// registrar accepts every REGISTER, like a telephone system with the right
// password, and plays the owner's phone for calls to the owner's number.
type registrar struct {
	// registered closes on the first REGISTER whose From and Contact name the line, as a FRITZ!Box requires.
	registered chan struct{}
	// answered receives the caller ID of each call the owner's phone took and heard audio on.
	answered chan string
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

	r := &registrar{registered: make(chan struct{}), answered: make(chan string, 1)}

	var once sync.Once

	server.OnRegister(func(request *sip.Request, tx sip.ServerTransaction) {
		if request.From().Address.User != "ada" || request.Contact().Address.User != "ada" {
			return // A FRITZ!Box sends no answer at all.
		}

		_ = tx.Respond(sip.NewResponseFromRequest(request, sip.StatusOK, "OK", nil))

		once.Do(func() { close(r.registered) })
	})

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

//nolint:paralleltest // Binds fixed SIP and RTP ports.
func TestCalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	fake := &bot{}
	voice := &synthesizer{}

	// New sets the media package's port range, so it runs before any other SIP stack starts.
	gateway, err := sipline.New(testConfig(), call.Services{Speech: nil, Bot: fake, Log: slog.New(slog.DiscardHandler)},
		sipline.Options{
			SpeechFor: func(string) call.Speech {
				return call.Pipeline{Transcriber: transcriber{}, Synthesizer: voice}
			},
			BotNames: map[string]string{"bot_ada": "Ada"},
			// Half a second of a recognisable non-silent pattern.
			StrangerRecording: bytes.Repeat([]byte{0x00, 0x30}, audio.TelephoneRate/2),
		})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	fakeRegistrar := startRegistrar(ctx, t)
	stopped := make(chan error, 1)

	go func() { stopped <- gateway.Run(ctx) }()

	select {
	case <-fakeRegistrar.registered:
	case <-ctx.Done():
		t.Fatal("the line never registered")
	}

	phone := caller(t)

	t.Run("stranger hears the recording, not the bot", func(t *testing.T) { strangerHearsRecording(ctx, t, phone, fake) })
	t.Run("owner reaches the bot and hears the reply", func(t *testing.T) {
		ownerReachesBot(ctx, t, phone, fake)

		if said := voice.spoken(); len(said) == 0 || said[0] != "Hi, this is Ada." {
			t.Errorf("said = %q, want the greeting with the bot's name first", said)
		}
	})
	t.Run("bot calls the owner", func(t *testing.T) { botCallsOwner(ctx, t, gateway, fakeRegistrar) })
	t.Run("bot cannot call strangers", func(t *testing.T) {
		if _, dialErr := gateway.Dial(ctx, "bot_ada", "999", "Hi."); !errors.Is(dialErr, sipline.ErrNotOwner) {
			t.Fatalf("err = %v, want ErrNotOwner", dialErr)
		}
	})

	cancel()

	if err := <-stopped; err != nil {
		t.Fatalf("Run: %v", err)
	}
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

func botCallsOwner(ctx context.Context, t *testing.T, gateway *sipline.Gateway, fakeRegistrar *registrar) {
	t.Helper()

	// The previous call releases the bot once its hangup completes.
	deadline := time.Now().Add(5 * time.Second)

	id, err := gateway.Dial(ctx, "bot_ada", owner, "Hello, this is Ada.")
	for errors.Is(err, sipline.ErrBusy) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)

		id, err = gateway.Dial(ctx, "bot_ada", owner, "Hello, this is Ada.")
	}

	if err != nil || id == "" {
		t.Fatalf("Dial = %q, %v", id, err)
	}

	select {
	case from := <-fakeRegistrar.answered:
		if from != "ada" {
			t.Errorf("call came from %q, want the line ada", from)
		}
	case <-ctx.Done():
		t.Fatal("the owner's phone heard nothing")
	}
}
