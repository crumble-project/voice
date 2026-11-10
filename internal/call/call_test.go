package call_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/call"
	"github.com/crumble-project/crumble-voice/internal/speech"
	"github.com/crumble-project/crumble-voice/internal/turn"
)

var errBoom = errors.New("boom")

type fakeTranscription struct {
	events chan speech.Event
}

func (f *fakeTranscription) Write([]byte) error          { return nil }
func (f *fakeTranscription) Events() <-chan speech.Event { return f.events }
func (f *fakeTranscription) Err() error                  { return nil }
func (f *fakeTranscription) Close() error                { return nil }

type fakeTranscriber struct{ session *fakeTranscription }

//nolint:ireturn // The fake implements speech.Transcriber.
func (f fakeTranscriber) Transcribe(context.Context) (speech.Transcription, error) {
	return f.session, nil
}

// fakeSynthesizer returns one frame per character, so tests can count what was spoken.
type fakeSynthesizer struct {
	mu     sync.Mutex
	spoken []string
}

func (f *fakeSynthesizer) Synthesize(_ context.Context, text string) (io.ReadCloser, error) {
	f.mu.Lock()
	f.spoken = append(f.spoken, text)
	f.mu.Unlock()

	return io.NopCloser(bytes.NewReader(make([]byte, audio.FrameBytes*len(text)))), nil
}

func (f *fakeSynthesizer) said() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.spoken...)
}

type fakeBot struct {
	mu       sync.Mutex
	contents []string
	calls    []string
	reply    func(content string) (string, error)
	// stream, when set, replaces reply and may report partial text.
	stream func(content string, partial func(generation int, text string)) ([]string, error)
	// hangUp makes every reply ask to end the call.
	hangUp bool
}

func (f *fakeBot) Turn(_ context.Context, _, callID, content string, observer turn.Observer) ([]string, error) {
	f.mu.Lock()
	f.contents = append(f.contents, content)
	f.calls = append(f.calls, callID)
	f.mu.Unlock()

	observer.NotifyAccepted()
	observer.NotifyRunning()

	if f.stream != nil {
		return f.stream(content, observer.NotifyPartial)
	}

	reply, err := f.reply(content)
	if err != nil {
		return nil, err
	}

	if f.hangUp {
		observer.NotifyHangUp()
	}

	return []string{reply}, nil
}

// pacedWriter blocks like an RTP writer, 1 ms per frame instead of 20.
type pacedWriter struct{}

func (pacedWriter) Write(p []byte) (int, error) {
	time.Sleep(time.Millisecond)

	return len(p), nil
}

func options() call.Options {
	return call.Options{
		Line: "ada", BotID: "bot", CallID: "call-1", Greeting: "",
		HoldingPhrase: "One moment.", HoldingDelay: time.Hour,
		TimeoutPhrase: "Later in the chat.", FailurePhrase: "Something went wrong.",
		TurnTimeout: time.Second, MaxDuration: time.Minute,
	}
}

func run(t *testing.T, bot *fakeBot, opts call.Options, script func(events chan<- speech.Event, synth *fakeSynthesizer)) *fakeSynthesizer {
	t.Helper()

	session := &fakeTranscription{events: make(chan speech.Event)}
	synth := &fakeSynthesizer{}
	reader, writer := io.Pipe()

	done := make(chan error, 1)

	go func() {
		done <- call.Run(context.Background(), call.Media{In: reader, Out: pacedWriter{}}, call.Services{
			Speech: call.Pipeline{Transcriber: fakeTranscriber{session: session}, Synthesizer: synth},
			Bot:    bot,
			Log:    slog.New(slog.DiscardHandler),
		}, opts)
	}()

	script(session.events, synth)
	_ = writer.Close()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	return synth
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func TestReplyIsSpokenAndMessagesCarryTheCallID(t *testing.T) {
	t.Parallel()

	bot := &fakeBot{reply: func(string) (string, error) { return "**Hi** there", nil }}

	synth := run(t, bot, options(), func(events chan<- speech.Event, synth *fakeSynthesizer) {
		events <- speech.Event{Kind: speech.Utterance, Text: "hello"}
		eventually(t, func() bool { return len(synth.said()) == 1 })
		events <- speech.Event{Kind: speech.Utterance, Text: "again"}
		eventually(t, func() bool { return len(synth.said()) == 2 })
	})

	if got := synth.said(); got[0] != "Hi there" {
		t.Errorf("spoken = %q, want Markdown removed", got)
	}

	if bot.contents[0] != "hello" || bot.contents[1] != "again" {
		t.Errorf("messages = %q, want what the caller said", bot.contents)
	}

	if bot.calls[0] != "call-1" || bot.calls[1] != "call-1" {
		t.Errorf("call IDs = %q, want the call's ID on every message", bot.calls)
	}
}

func TestSpeechWhileTheBotThinksIsSentAsOneMessage(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	bot := &fakeBot{reply: func(content string) (string, error) {
		if strings.HasSuffix(content, "first") {
			<-release
		}

		return "ok", nil
	}}

	run(t, bot, options(), func(events chan<- speech.Event, synth *fakeSynthesizer) {
		events <- speech.Event{Kind: speech.Utterance, Text: "first"}
		events <- speech.Event{Kind: speech.Utterance, Text: "second"}
		events <- speech.Event{Kind: speech.Utterance, Text: "third"}
		close(release)
		eventually(t, func() bool { return len(synth.said()) == 2 })
	})

	if len(bot.contents) != 2 || bot.contents[1] != "second third" {
		t.Errorf("messages = %q", bot.contents)
	}
}

func TestBargeInStopsSpeech(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("word ", 2000)
	bot := &fakeBot{reply: func(string) (string, error) { return long, nil }}

	synth := run(t, bot, options(), func(events chan<- speech.Event, synth *fakeSynthesizer) {
		events <- speech.Event{Kind: speech.Utterance, Text: "tell me a lot"}
		eventually(t, func() bool { return len(synth.said()) >= 1 })
		events <- speech.Event{Kind: speech.SpeechStarted, Text: ""}
		time.Sleep(50 * time.Millisecond)
	})

	// The reply splits into seven chunks; barge-in drops all but the one playing
	// and the one synthesized ahead of it.
	if got := len(synth.said()); got > 2 {
		t.Errorf("synthesized %d chunks after barge-in, want at most 2", got)
	}
}

func TestFailedTurnSpeaksFailurePhrase(t *testing.T) {
	t.Parallel()

	bot := &fakeBot{reply: func(string) (string, error) { return "", errBoom }}

	synth := run(t, bot, options(), func(events chan<- speech.Event, synth *fakeSynthesizer) {
		events <- speech.Event{Kind: speech.Utterance, Text: "hello"}
		eventually(t, func() bool { return len(synth.said()) == 1 })
	})

	if got := synth.said()[0]; got != "Something went wrong." {
		t.Errorf("spoken = %q", got)
	}
}

func TestServerEndingTheCallEndsIt(t *testing.T) {
	t.Parallel()

	bot := &fakeBot{reply: func(string) (string, error) { return "", fmt.Errorf("send message: %w", turn.ErrCallEnded) }}
	session := &fakeTranscription{events: make(chan speech.Event, 1)}
	synth := &fakeSynthesizer{}
	reader, writer := io.Pipe()

	defer func() { _ = writer.Close() }()

	session.events <- speech.Event{Kind: speech.Utterance, Text: "hello"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := call.Run(ctx, call.Media{In: reader, Out: pacedWriter{}}, call.Services{
		Speech: call.Pipeline{Transcriber: fakeTranscriber{session: session}, Synthesizer: synth},
		Bot:    bot,
		Log:    slog.New(slog.DiscardHandler),
	}, options())
	if !errors.Is(err, turn.ErrCallEnded) {
		t.Fatalf("Run = %v, want the call ended by the server", err)
	}

	if said := synth.said(); len(said) != 0 {
		t.Errorf("said %q, want no failure phrase on a call the server ended", said)
	}
}

func TestBotHangsUpAfterItsGoodbyeWasPlayed(t *testing.T) {
	t.Parallel()

	const goodbye = "Goodbye for now."

	bot := &fakeBot{reply: func(string) (string, error) { return goodbye, nil }, hangUp: true}
	session := &fakeTranscription{events: make(chan speech.Event, 1)}
	synth := &fakeSynthesizer{}
	out := &countingWriter{}
	reader, writer := io.Pipe()

	defer func() { _ = writer.Close() }()

	session.events <- speech.Event{Kind: speech.Utterance, Text: "please hang up"}

	// What the caller says after the bot hung up starts no further turn.
	go func() {
		time.Sleep(20 * time.Millisecond)

		session.events <- speech.Event{Kind: speech.SpeechStarted}
		session.events <- speech.Event{Kind: speech.Utterance, Text: "hello?"}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := options()
	opts.HangUpQuiet = 100 * time.Millisecond

	err := call.Run(ctx, call.Media{In: reader, Out: out}, call.Services{
		Speech: call.Pipeline{Transcriber: fakeTranscriber{session: session}, Synthesizer: synth},
		Bot:    bot,
		Log:    slog.New(slog.DiscardHandler),
	}, opts)
	if !errors.Is(err, call.ErrBotHungUp) {
		t.Fatalf("Run = %v, want the bot's hang-up", err)
	}

	bot.mu.Lock()
	turns := len(bot.contents)
	bot.mu.Unlock()

	if turns != 1 {
		t.Errorf("the bot got %d turns, want only the one that hung up", turns)
	}

	// The fake synthesizer makes one frame per character.
	if played := out.frames.Load(); played != int64(len(goodbye)) {
		t.Errorf("played %d frames before hanging up, want the whole goodbye of %d", played, len(goodbye))
	}
}

func TestSlowTurnSpeaksHoldingThenTimeoutPhrase(t *testing.T) {
	t.Parallel()

	opts := options()
	opts.HoldingDelay = 10 * time.Millisecond
	opts.TurnTimeout = 100 * time.Millisecond

	bot := &fakeBot{reply: func(string) (string, error) {
		time.Sleep(time.Second)

		return "", context.DeadlineExceeded
	}}

	synth := run(t, bot, opts, func(events chan<- speech.Event, synth *fakeSynthesizer) {
		events <- speech.Event{Kind: speech.Utterance, Text: "hello"}
		eventually(t, func() bool { return len(synth.said()) == 2 })
	})

	if got := synth.said(); got[0] != "One moment." || got[1] != "Later in the chat." {
		t.Errorf("spoken = %q", got)
	}
}

func TestSpeakable(t *testing.T) {
	t.Parallel()

	input := "# Plan\n\n- **Buy** [milk](https://shop.example)\n- see https://x.example/a\n\n```go\nfmt.Println()\n```\nDone."
	want := "Plan\nBuy milk\nsee (link in the chat)\n(code is in the chat)\nDone."

	if got := call.Speakable(input); got != want {
		t.Errorf("Speakable = %q", got)
	}
}

func TestChunksPreferSentenceEnds(t *testing.T) {
	t.Parallel()

	chunks := call.Chunks("One two. Three four. Five six.", 20)

	want := []string{"One two. Three four.", "Five six."}
	if len(chunks) != len(want) || chunks[0] != want[0] || chunks[1] != want[1] {
		t.Errorf("Chunks = %q", chunks)
	}
}

func TestFirstSentenceIsSpokenWhileTheBotWrites(t *testing.T) {
	t.Parallel()

	var synthRef *fakeSynthesizer

	ready := make(chan *fakeSynthesizer, 1)
	bot := &fakeBot{stream: func(_ string, partial func(int, string)) ([]string, error) {
		synthRef = <-ready

		partial(0, "Hello there. How")
		// The turn is still running, yet the first sentence is already spoken.
		eventually(t, func() bool { return len(synthRef.said()) == 1 })
		partial(0, "Hello there. How are you?")

		return []string{"Hello there. How are you? Fine."}, nil
	}}

	synth := run(t, bot, options(), func(events chan<- speech.Event, synth *fakeSynthesizer) {
		ready <- synth
		events <- speech.Event{Kind: speech.Utterance, Text: "hi"}
		eventually(t, func() bool { return len(synth.said()) == 2 })
	})

	want := []string{"Hello there.", "How are you? Fine."}
	if got := synth.said(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("spoken = %q, want %q", got, want)
	}
}

func TestNewGenerationDropsTheUnseenTailOfTheLastOne(t *testing.T) {
	t.Parallel()

	bot := &fakeBot{stream: func(_ string, partial func(int, string)) ([]string, error) {
		partial(0, "Let me check. One")
		partial(1, "It is sunny. Very")

		return []string{"Let me check. One second.", "It is sunny. Very warm."}, nil
	}}

	synth := run(t, bot, options(), func(events chan<- speech.Event, synth *fakeSynthesizer) {
		events <- speech.Event{Kind: speech.Utterance, Text: "weather?"}
		eventually(t, func() bool { return len(synth.said()) == 3 })
	})

	want := []string{"Let me check.", "It is sunny.", "Very warm."}
	if got := synth.said(); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("spoken = %q, want %q", got, want)
	}
}

func TestHoldingPhraseIsSkippedOnceTheBotSpeaks(t *testing.T) {
	t.Parallel()

	opts := options()
	opts.HoldingDelay = 50 * time.Millisecond

	bot := &fakeBot{stream: func(_ string, partial func(int, string)) ([]string, error) {
		partial(0, "Sure. ")
		time.Sleep(150 * time.Millisecond)

		return []string{"Sure. Done."}, nil
	}}

	synth := run(t, bot, opts, func(events chan<- speech.Event, synth *fakeSynthesizer) {
		events <- speech.Event{Kind: speech.Utterance, Text: "go"}
		eventually(t, func() bool { return len(synth.said()) == 2 })
	})

	if got := synth.said(); got[0] != "Sure." || got[1] != "Done." {
		t.Errorf("spoken = %q, want no holding phrase", got)
	}
}
