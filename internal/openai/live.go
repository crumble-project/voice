package openai

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/speech"
)

const (
	// The caller counts as done when no transcript arrived for this long and the model has not answered yet.
	utteranceQuiet = 800 * time.Millisecond
	// A few seconds of model speech; the call drains it in real time.
	liveAudioBacklog = 256
)

// Live opens GPT-Live sessions in client mode: the model hears the caller,
// acknowledges, and speaks the text the gateway gives it.
type Live struct {
	log          *slog.Logger
	base         *url.URL
	apiKey       string
	model        string
	voice        string
	instructions string
}

// NewLive returns a GPT-Live client for an API base such as https://api.openai.com.
func NewLive(base *url.URL, apiKey, model, voice, instructions string, log *slog.Logger) *Live {
	return &Live{log: log, base: base, apiKey: apiKey, model: model, voice: voice, instructions: instructions}
}

var _ speech.Live = (*Live)(nil)

type liveStart struct {
	Type    string      `json:"type"`
	Session liveSession `json:"session"`
}

type liveSession struct {
	Model        string    `json:"model"`
	Instructions string    `json:"instructions"`
	Audio        liveAudio `json:"audio"`
}

type liveAudio struct {
	// G.711 μ-law at 8 kHz is the telephone format, so no resampling is needed.
	Format inputFormat `json:"format"`
	Output struct {
		Voice string `json:"voice"`
	} `json:"output"`
}

type liveEvent struct {
	Type  string `json:"type"`
	Delta string `json:"delta"`
	Error *struct {
		Type    string `json:"type"`
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	Reason string `json:"reason"`
}

// Open connects, starts the session, and returns once the server confirmed it.
//
//nolint:ireturn // Open implements speech.Live, whose sessions are provider-neutral.
func (l *Live) Open(ctx context.Context) (speech.Duplex, error) {
	dialCtx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()

	conn, err := dial(dialCtx, l.base, "/v1/live/sessions", url.Values{}, l.apiKey)
	if err != nil {
		return nil, fmt.Errorf("GPT-Live: %w", err)
	}

	start := liveStart{Type: "session.start", Session: liveSession{
		Model:        l.model,
		Instructions: l.instructions,
		Audio:        liveAudio{Format: inputFormat{Type: formatPCMU, Rate: audio.TelephoneRate}},
	}}
	start.Session.Audio.Output.Voice = l.voice

	if err := sendJSON(dialCtx, conn, start); err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "start failed")

		return nil, fmt.Errorf("start GPT-Live session: %w", err)
	}

	for {
		var event liveEvent
		if err := readJSON(dialCtx, conn, &event); err != nil {
			_ = conn.Close(websocket.StatusPolicyViolation, "start failed")

			return nil, fmt.Errorf("wait for GPT-Live session: %w", err)
		}

		if event.Type == eventTypeError {
			_ = conn.Close(websocket.StatusPolicyViolation, "start failed")

			return nil, liveError(event)
		}

		if event.Type == "session.started" {
			break
		}
	}

	session := &duplex{
		ctx: ctx, log: l.log, conn: conn,
		input:  make(chan []byte, audioBacklog),
		events: make(chan speech.Event, eventBacklog),
		output: make(chan []byte, liveAudioBacklog),
		done:   make(chan struct{}),
	}
	session.start(ctx)

	return session, nil
}

func liveError(event liveEvent) error {
	if event.Error == nil {
		return fmt.Errorf("%w: %s without details", ErrRealtime, event.Type)
	}

	return fmt.Errorf("%w: %s (%s %s)", ErrRealtime, event.Error.Message, event.Error.Type, event.Error.Code)
}

type duplex struct {
	ctx    context.Context //nolint:containedctx // Say and Close have no context of their own; the call's bounds them.
	log    *slog.Logger
	conn   *websocket.Conn
	input  chan []byte
	events chan speech.Event
	output chan []byte
	done   chan struct{}

	closeOnce sync.Once
	mu        sync.Mutex
	err       error
}

var _ speech.Duplex = (*duplex)(nil)

func (d *duplex) Write(pcm []byte) error {
	select {
	case <-d.done:
		if err := d.Err(); err != nil {
			return err
		}

		return ErrClosed
	default:
	}

	select {
	case d.input <- pcm:
		return nil
	default:
		d.finish(d.ctx, ErrBacklog)

		return ErrBacklog
	}
}

func (d *duplex) Events() <-chan speech.Event { return d.events }
func (d *duplex) Audio() <-chan []byte        { return d.output }

func (d *duplex) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.err
}

type commentary struct {
	Type string `json:"type"`
	// DelegationID is null for text meant for the whole session.
	DelegationID *string `json:"delegation_id"`
	Content      string  `json:"content"`
}

type closeSession struct {
	Type string `json:"type"`
}

// Greet words the opening line as an instruction, the form OpenAI's own greeting
// example uses; plain commentary at the start of a call may go unspoken.
func (d *duplex) Greet(text string) error {
	// A bot writes the greeting, so quoting keeps a quote or newline in it from ending the string.
	return d.Say("Greet the caller now by saying exactly: " + strconv.Quote(text))
}

// Say hands text to the model as commentary, which it speaks and may rephrase.
func (d *duplex) Say(text string) error {
	return sendJSON(d.ctx, d.conn, commentary{Type: "session.commentary.append", DelegationID: nil, Content: text})
}

func (d *duplex) Close() error {
	d.shutdown(d.ctx)

	return nil
}

func (d *duplex) shutdown(ctx context.Context) {
	d.closeOnce.Do(func() {
		// The goodbye goes out even when the call's context already ended.
		goodbyeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), writeTimeout)
		defer cancel()

		_ = sendJSON(goodbyeCtx, d.conn, closeSession{Type: "session.close"})

		close(d.done)
		_ = d.conn.Close(websocket.StatusNormalClosure, "")
	})
}

func (d *duplex) finish(ctx context.Context, err error) {
	d.mu.Lock()
	if d.err == nil && err != nil {
		d.err = err
	}
	d.mu.Unlock()

	d.shutdown(ctx)
}

func (d *duplex) start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	raw := make(chan liveEvent)

	go func() {
		defer cancel()
		d.finish(ctx, d.writeLoop(ctx))
	}()

	go func() {
		defer cancel()
		defer close(raw)

		for {
			var event liveEvent
			if err := readJSON(ctx, d.conn, &event); err != nil {
				select {
				case <-d.done:
				default:
					d.finish(ctx, err)
				}

				return
			}

			select {
			case raw <- event:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		defer cancel()
		defer close(d.events)
		defer close(d.output)
		d.finish(ctx, d.eventLoop(ctx, raw))
	}()
}

func (d *duplex) writeLoop(ctx context.Context) error {
	return sendUlaw(ctx, d.conn, d.input, d.done, "session.input_audio.append")
}

// eventLoop turns transcript deltas into utterances. GPT-Live marks no turn end,
// so an utterance ends when the model starts answering or the transcript goes quiet.
func (d *duplex) eventLoop(ctx context.Context, raw <-chan liveEvent) error {
	turn := &hearing{quiet: time.NewTimer(time.Hour)}
	turn.quiet.Stop()

	defer d.logSaid(turn)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-turn.quiet.C:
			if !d.flush(ctx, turn) {
				return nil
			}
		case event, ok := <-raw:
			if !ok {
				return nil
			}

			keepGoing, err := d.handle(ctx, turn, event)
			if err != nil || !keepGoing {
				return err
			}
		}
	}
}

// hearing collects the caller's words until the turn ends, and the model's
// words until the caller speaks again.
type hearing struct {
	text strings.Builder
	said strings.Builder
	// saidAt is when the model started the words in said.
	saidAt time.Time
	last   time.Time
	quiet  *time.Timer
}

func (d *duplex) handle(ctx context.Context, turn *hearing, event liveEvent) (bool, error) {
	switch event.Type {
	case "session.input_transcript.delta":
		d.logSaid(turn)

		if turn.text.Len() == 0 && !d.emit(ctx, speech.Event{Kind: speech.SpeechStarted, Text: "", Stopped: time.Time{}}) {
			return false, nil
		}

		turn.text.WriteString(event.Delta)
		turn.last = time.Now()
		turn.quiet.Reset(utteranceQuiet)
	case "session.output_transcript.delta":
		if turn.said.Len() == 0 {
			turn.saidAt = time.Now()
		}

		turn.said.WriteString(event.Delta)

		if turn.text.Len() > 0 {
			return d.flush(ctx, turn), nil
		}
	case "session.output_audio.delta":
		return true, d.play(ctx, event.Delta)
	case eventTypeError:
		d.logSaid(turn)

		return false, liveError(event)
	case "session.closed":
		d.logSaid(turn)

		return false, fmt.Errorf("%w: GPT-Live closed the session: %s", ErrClosed, event.Reason)
	default:
	}

	return true, nil
}

// logSaid logs what the model said since the caller last spoke, the only record
// of how it worded the bot's text.
func (d *duplex) logSaid(turn *hearing) {
	if said := strings.TrimSpace(turn.said.String()); said != "" {
		// Debug only: the text is conversation content, which the bot chat already keeps.
		d.log.Debug("live voice said", "text", said, "started", turn.saidAt.Format(time.RFC3339Nano))
	}

	turn.said.Reset()
}

// flush reports what the caller said as one utterance; false means the call ended.
func (d *duplex) flush(ctx context.Context, turn *hearing) bool {
	turn.quiet.Stop()

	text := strings.TrimSpace(turn.text.String())
	turn.text.Reset()

	if text == "" {
		return true
	}

	return d.emit(ctx, speech.Event{Kind: speech.Utterance, Text: text, Stopped: turn.last})
}

func (d *duplex) emit(ctx context.Context, event speech.Event) bool {
	select {
	case d.events <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func (d *duplex) play(ctx context.Context, delta string) error {
	pcm, err := decodeUlaw(delta)
	if err != nil {
		return err
	}

	select {
	case d.output <- pcm:
	case <-ctx.Done():
	}

	return nil
}
