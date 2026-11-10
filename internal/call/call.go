// Package call runs one phone conversation between a caller and a bot chat.
package call

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/speech"
	"github.com/crumble-project/crumble-voice/internal/turn"
)

// Leaves room under the 4096-character synthesis limit for multi-byte text.
const synthesisChunk = 1500

var (
	// ErrMaxDuration ends a call that reached its configured length.
	ErrMaxDuration = errors.New("call reached its maximum duration")
	// ErrBotHungUp ends a call the bot asked to end, once its goodbye was said.
	ErrBotHungUp = errors.New("the bot hung up")
)

// Speech has no end signal shared by every speech mode, so a goodbye counts as said
// once no audio went out for this long. It covers the wait for the first audio too.
const defaultHangUpQuiet = 3 * time.Second

// Bot answers one spoken message on a line's bot chat, reporting progress to
// observer; the result holds the reply's messages in order.
type Bot interface {
	Turn(ctx context.Context, username, callID, content string, observer turn.Observer) ([]string, error)
}

// Options describe one call.
type Options struct {
	// Line is the SIP username of the line the call is on.
	Line string
	// BotID names the line's bot in logs.
	BotID string
	// CallID groups the call's spoken messages on the server.
	CallID        string
	Greeting      string
	HoldingPhrase string
	HoldingDelay  time.Duration
	TimeoutPhrase string
	FailurePhrase string
	TurnTimeout   time.Duration
	MaxDuration   time.Duration
	// HangUpQuiet is how long no audio must go out before a call the bot ends hangs up; zero means 3 seconds.
	HangUpQuiet time.Duration
}

// Media is the call's audio as telephone PCM. Out must pace writes in real time.
type Media struct {
	In  io.Reader
	Out io.Writer
}

// Services are the dependencies of a call.
type Services struct {
	Speech Speech
	Bot    Bot
	Log    *slog.Logger
}

// quietWriter notes when audio last went out.
type quietWriter struct {
	out  io.Writer
	last atomic.Int64
}

func (w *quietWriter) Write(pcm []byte) (int, error) {
	n, err := w.out.Write(pcm)
	w.last.Store(time.Now().UnixNano())

	//nolint:wrapcheck // The writer only observes; callers wrap the error.
	return n, err
}

type turnResult struct {
	parts   []string
	err     error
	hangUp  bool
	speaker *streamSpeaker
	heard   time.Duration
	timing  *turnTiming
}

// turnTiming records when each step of a turn happened, to find what makes a call slow.
type turnTiming struct {
	mu        sync.Mutex
	sent      time.Time
	accepted  time.Time
	running   time.Time
	firstText time.Time
}

func (t *turnTiming) mark(at *time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if at.IsZero() {
		*at = time.Now()
	}
}

// since returns milliseconds from sending to at, or -1 when the step never happened.
func (t *turnTiming) since(at time.Time) int64 {
	if at.IsZero() {
		return -1
	}

	return at.Sub(t.sent).Milliseconds()
}

// Run holds the conversation until the caller hangs up, ctx ends, or a service fails.
// The caller's audio is read until In returns io.EOF, which means the call ended.
func Run(ctx context.Context, media Media, services Services, options Options) error {
	ctx, cancel := context.WithTimeoutCause(ctx, options.MaxDuration, ErrMaxDuration)
	defer cancel()

	if options.HangUpQuiet == 0 {
		options.HangUpQuiet = defaultHangUpQuiet
	}

	out := &quietWriter{out: media.Out, last: atomic.Int64{}}

	session, err := services.Speech.Start(ctx, out, services.Log)
	if err != nil {
		return fmt.Errorf("start speech: %w", err)
	}

	defer session.Close()

	listened := make(chan error, 1)

	go func() { listened <- listen(media.In, session) }()

	if options.Greeting != "" {
		session.Greet(options.Greeting)
	}

	if services.Speech.Acknowledges() {
		options.HoldingPhrase = ""
	}

	conversation := &conversation{
		services: services, options: options, session: session, out: out,
		results: make(chan turnResult, 1),
	}

	return conversation.loop(ctx, listened)
}

type conversation struct {
	services Services
	options  Options
	session  Session
	out      *quietWriter
	results  chan turnResult
	// leaving fires when a call the bot ends may have gone quiet; leftAt is when the bot asked.
	leaving <-chan time.Time
	leftAt  time.Time
	queued  []string
	heard   time.Duration
	busy    bool
}

func (c *conversation) loop(ctx context.Context, listened <-chan error) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("call ended: %w", context.Cause(ctx))
		case err := <-listened:
			return err
		case err := <-c.session.Failed():
			return err
		case event, ok := <-c.session.Events():
			if !ok {
				return fmt.Errorf("hearing ended: %w", c.session.Err())
			}

			c.handle(ctx, event)
		case result := <-c.results:
			if errors.Is(result.err, turn.ErrCallEnded) {
				return fmt.Errorf("bot turn: %w", result.err)
			}

			c.busy = false
			c.answer(ctx, result)

			if result.hangUp && result.err == nil && c.leaving == nil {
				c.leaving, c.leftAt = time.After(c.options.HangUpQuiet), time.Now()
			}

			c.next(ctx)
		case <-c.leaving:
			quietSince := c.leftAt
			if last := time.Unix(0, c.out.last.Load()); last.After(quietSince) {
				quietSince = last
			}

			quiet := time.Since(quietSince)
			if quiet >= c.options.HangUpQuiet {
				return ErrBotHungUp
			}

			c.leaving = time.After(c.options.HangUpQuiet - quiet)
		}
	}
}

func (c *conversation) handle(ctx context.Context, event speech.Event) {
	// The bot hung up: the goodbye plays to its end and nothing the caller says starts a turn.
	if c.leaving != nil {
		return
	}

	switch event.Kind {
	case speech.SpeechStarted:
		c.session.Stop()
	case speech.Utterance:
		if !event.Stopped.IsZero() {
			c.heard = time.Since(event.Stopped)
		}

		c.services.Log.Debug("caller said", "chars", len(event.Text))
		c.queued = append(c.queued, event.Text)
		c.next(ctx)
	default:
	}
}

// next sends everything the caller said since the last turn as one message;
// a bot chat runs one turn at a time.
func (c *conversation) next(ctx context.Context) {
	if c.busy || len(c.queued) == 0 || c.leaving != nil {
		return
	}

	content := strings.Join(c.queued, " ")
	c.queued = nil
	c.busy = true

	go c.turn(ctx, content, c.heard)
}

func (c *conversation) turn(callCtx context.Context, content string, heard time.Duration) {
	ctx, cancel := context.WithTimeout(callCtx, c.options.TurnTimeout)
	defer cancel()

	var spoke, hangUp atomic.Bool

	speaker := &streamSpeaker{say: func(text string) {
		spoke.Store(true)
		c.session.Say(text)
	}}

	holding := time.AfterFunc(c.options.HoldingDelay, func() {
		if c.options.HoldingPhrase != "" && !spoke.Load() {
			c.session.Say(c.options.HoldingPhrase)
		}
	})

	timing := &turnTiming{sent: time.Now()}
	observer := turn.Observer{
		Accepted: func() { timing.mark(&timing.accepted) },
		Running:  func() { timing.mark(&timing.running) },
		Partial: func(generation int, text string) {
			timing.mark(&timing.firstText)
			speaker.partial(generation, text)
		},
		HangUp: func() { hangUp.Store(true) },
	}

	parts, err := c.services.Bot.Turn(ctx, c.options.Line, c.options.CallID, content, observer)

	holding.Stop()

	if errors.Is(err, context.DeadlineExceeded) && callCtx.Err() == nil {
		err = errTurnTimeout
	}

	c.results <- turnResult{parts: parts, err: err, hangUp: hangUp.Load(), speaker: speaker, heard: heard, timing: timing}
}

var errTurnTimeout = errors.New("the bot did not reply in time")

func (c *conversation) answer(ctx context.Context, result turnResult) {
	switch {
	case result.err == nil:
		c.logTiming(result)

		for _, chunk := range Chunks(Speakable(result.speaker.remainder(result.parts)), synthesisChunk) {
			c.session.Say(chunk)
		}
	case errors.Is(result.err, errTurnTimeout):
		// The turn keeps running in Crumble; its reply lands in the bot chat.
		c.services.Log.Warn("bot turn timed out", "bot", c.options.BotID)
		c.session.Say(c.options.TimeoutPhrase)
	case ctx.Err() != nil:
	default:
		c.services.Log.Error("bot turn failed", "bot", c.options.BotID, "error", result.err)
		c.session.Say(c.options.FailurePhrase)
	}
}

// logTiming records where a turn's time went. All but transcribe_ms count from
// sending the message; -1 means the step was not seen.
func (c *conversation) logTiming(result turnResult) {
	timing := result.timing

	timing.mu.Lock()
	defer timing.mu.Unlock()

	c.services.Log.Info("turn timing",
		"transcribe_ms", result.heard.Milliseconds(),
		"accepted_ms", timing.since(timing.accepted),
		"running_ms", timing.since(timing.running),
		"first_text_ms", timing.since(timing.firstText),
		"first_sentence_ms", timing.since(result.speaker.started),
		"reply_ms", time.Since(timing.sent).Milliseconds(),
	)
}

func listen(in io.Reader, session Session) error {
	frame := make([]byte, audio.FrameBytes)

	for {
		n, err := io.ReadFull(in, frame)
		if n > 0 {
			if writeErr := session.Write(append([]byte(nil), frame[:n]...)); writeErr != nil {
				return fmt.Errorf("send caller audio: %w", writeErr)
			}
		}

		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}

		if err != nil {
			return fmt.Errorf("read caller audio: %w", err)
		}
	}
}
