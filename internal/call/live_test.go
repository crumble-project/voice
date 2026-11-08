package call_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/call"
	"github.com/crumble-project/crumble-voice/internal/speech"
)

type fakeDuplex struct {
	events chan speech.Event
	audio  chan []byte

	mu   sync.Mutex
	said []string
}

func (f *fakeDuplex) Write([]byte) error          { return nil }
func (f *fakeDuplex) Events() <-chan speech.Event { return f.events }
func (f *fakeDuplex) Err() error                  { return nil }
func (f *fakeDuplex) Close() error                { return nil }
func (f *fakeDuplex) Audio() <-chan []byte        { return f.audio }

func (f *fakeDuplex) Say(text string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.said = append(f.said, text)

	return nil
}

func (f *fakeDuplex) spoken() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.said...)
}

type fakeLive struct{ duplex *fakeDuplex }

//nolint:ireturn // The fake implements speech.Live.
func (f fakeLive) Open(context.Context) (speech.Duplex, error) { return f.duplex, nil }

type countingWriter struct{ frames atomic.Int64 }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.frames.Add(1)

	return len(p), nil
}

func TestLiveModeHandsTheReplyToTheModel(t *testing.T) {
	t.Parallel()

	duplex := &fakeDuplex{events: make(chan speech.Event), audio: make(chan []byte, 4)}
	out := &countingWriter{}
	reader, writer := io.Pipe()

	opts := options()
	opts.HoldingDelay = 10 * time.Millisecond

	bot := &fakeBot{reply: func(string) (string, error) {
		time.Sleep(50 * time.Millisecond)

		return "It is sunny.", nil
	}}

	done := make(chan error, 1)

	go func() {
		done <- call.Run(context.Background(), call.Media{In: reader, Out: out}, call.Services{
			Speech: call.Live{Model: fakeLive{duplex: duplex}, Acknowledging: true},
			Bot:    bot,
			Log:    slog.New(slog.DiscardHandler),
		}, opts)
	}()

	duplex.audio <- make([]byte, audio.FrameBytes*2)
	duplex.events <- speech.Event{Kind: speech.Utterance, Text: "weather?"}

	eventually(t, func() bool { return len(duplex.spoken()) == 1 })
	eventually(t, func() bool { return out.frames.Load() == 2 })

	_ = writer.Close()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The live model acknowledges by itself, so no holding phrase is sent.
	if got := duplex.spoken(); len(got) != 1 || got[0] != "It is sunny." {
		t.Errorf("said = %q", got)
	}
}

func TestLiveModeLetsTheModelWordTheGreeting(t *testing.T) {
	t.Parallel()

	duplex := &fakeDuplex{events: make(chan speech.Event), audio: make(chan []byte)}
	reader, writer := io.Pipe()

	opts := options()
	opts.Greeting = "Hi, this is Frien."

	done := make(chan error, 1)

	go func() {
		done <- call.Run(context.Background(), call.Media{In: reader, Out: &countingWriter{}}, call.Services{
			Speech: call.Live{Model: fakeLive{duplex: duplex}},
			Bot:    &fakeBot{reply: func(string) (string, error) { return "", nil }},
			Log:    slog.New(slog.DiscardHandler),
		}, opts)
	}()

	eventually(t, func() bool { return len(duplex.spoken()) == 1 })
	_ = writer.Close()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	// fakeDuplex has no Greet of its own, so the greeting arrives as plain text.
	if got := duplex.spoken()[0]; got != "Hi, this is Frien." {
		t.Errorf("greeting = %q", got)
	}
}

func TestRealtimeModeSaysTheHoldingPhrase(t *testing.T) {
	t.Parallel()

	duplex := &fakeDuplex{events: make(chan speech.Event), audio: make(chan []byte)}
	reader, writer := io.Pipe()

	opts := options()
	opts.HoldingDelay = 10 * time.Millisecond

	done := make(chan error, 1)

	go func() {
		done <- call.Run(context.Background(), call.Media{In: reader, Out: &countingWriter{}}, call.Services{
			Speech: call.Live{Model: fakeLive{duplex: duplex}, Acknowledging: false},
			Bot: &fakeBot{reply: func(string) (string, error) {
				time.Sleep(80 * time.Millisecond)

				return "Done.", nil
			}},
			Log: slog.New(slog.DiscardHandler),
		}, opts)
	}()

	duplex.events <- speech.Event{Kind: speech.Utterance, Text: "go"}

	eventually(t, func() bool { return len(duplex.spoken()) == 2 })
	_ = writer.Close()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if got := duplex.spoken(); got[0] != "One moment." || got[1] != "Done." {
		t.Errorf("said = %q, want the holding phrase, then the reply", got)
	}
}
