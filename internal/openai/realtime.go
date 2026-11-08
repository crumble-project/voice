package openai

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/crumble-project/crumble-voice/internal/speech"
)

// Realtime opens Realtime API conversation sessions that never answer by
// themselves: they report what the caller says and speak only the text given.
type Realtime struct {
	log           *slog.Logger
	base          *url.URL
	apiKey        string
	model         string
	transcription string
	language      string
	voice         string
	style         string
}

// RealtimeOptions configure a Realtime client.
type RealtimeOptions struct {
	Model              string
	TranscriptionModel string
	Language           string
	Voice              string
	// Style is how to speak, such as "calmly and clearly"; the words always come from the bot.
	Style string
}

// NewRealtime returns a Realtime client for an API base such as https://api.openai.com.
func NewRealtime(base *url.URL, apiKey string, options RealtimeOptions, log *slog.Logger) *Realtime {
	return &Realtime{
		log: log, base: base, apiKey: apiKey, model: options.Model, transcription: options.TranscriptionModel,
		language: options.Language, voice: options.Voice, style: options.Style,
	}
}

var _ speech.Live = (*Realtime)(nil)

type realtimeUpdate struct {
	Type    string          `json:"type"`
	Session realtimeSession `json:"session"`
}

type realtimeSession struct {
	Type             string        `json:"type"`
	Model            string        `json:"model"`
	OutputModalities []string      `json:"output_modalities"`
	Audio            realtimeAudio `json:"audio"`
}

type realtimeAudio struct {
	Input struct {
		Format        audioFormat         `json:"format"`
		Transcription transcriptionConfig `json:"transcription"`
		TurnDetection realtimeTurns       `json:"turn_detection"`
	} `json:"input"`
	Output struct {
		Format audioFormat `json:"format"`
		Voice  string      `json:"voice"`
	} `json:"output"`
}

type audioFormat struct {
	Type string `json:"type"`
}

type realtimeTurns struct {
	Type              string  `json:"type"`
	Threshold         float64 `json:"threshold"`
	PrefixPaddingMS   int     `json:"prefix_padding_ms"`
	SilenceDurationMS int     `json:"silence_duration_ms"`
	// The model must never answer by itself: every word it says comes from the bot.
	CreateResponse bool `json:"create_response"`
	// The server cancels what the model is saying when the caller starts talking.
	InterruptResponse bool `json:"interrupt_response"`
}

type realtimeEvent struct {
	Type       string `json:"type"`
	Delta      string `json:"delta"`
	Transcript string `json:"transcript"`
	ResponseID string `json:"response_id"`
	Response   *struct {
		ID string `json:"id"`
	} `json:"response"`
	Error *struct {
		Type    string `json:"type"`
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// Open connects and configures the session, returning once the server accepted it.
//
//nolint:ireturn // Open implements speech.Live, whose sessions are provider-neutral.
func (r *Realtime) Open(ctx context.Context) (speech.Duplex, error) {
	dialCtx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()

	conn, err := dial(dialCtx, r.base, "/v1/realtime", url.Values{"model": {r.model}}, r.apiKey)
	if err != nil {
		return nil, fmt.Errorf("realtime: %w", err)
	}

	update := realtimeUpdate{Type: "session.update", Session: realtimeSession{
		Type: "realtime", Model: r.model, OutputModalities: []string{"audio"},
	}}

	// G.711 μ-law in both directions: the telephone's own format, no resampling.
	update.Session.Audio.Input.Format = audioFormat{Type: formatPCMU}
	update.Session.Audio.Input.Transcription = transcriptionConfig{Model: r.transcription, Language: r.language}
	update.Session.Audio.Input.TurnDetection = realtimeTurns{
		Type: "server_vad", Threshold: vadThreshold, PrefixPaddingMS: vadPrefixPadding, SilenceDurationMS: vadSilenceSeconds,
		CreateResponse: false, InterruptResponse: true,
	}
	update.Session.Audio.Output.Format = audioFormat{Type: formatPCMU}
	update.Session.Audio.Output.Voice = r.voice

	if err := r.configure(dialCtx, conn, update); err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "configuration failed")

		return nil, err
	}

	session := &realtimeDuplex{
		ctx: ctx, log: r.log, conn: conn, style: r.style,
		input:     make(chan []byte, audioBacklog),
		events:    make(chan speech.Event, eventBacklog),
		output:    make(chan []byte, liveAudioBacklog),
		says:      make(chan string, eventBacklog),
		interrupt: make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
	session.start(ctx)

	return session, nil
}

func (r *Realtime) configure(ctx context.Context, conn *websocket.Conn, update realtimeUpdate) error {
	if err := sendJSON(ctx, conn, update); err != nil {
		return fmt.Errorf("configure realtime session: %w", err)
	}

	for {
		var event realtimeEvent
		if err := readJSON(ctx, conn, &event); err != nil {
			return fmt.Errorf("wait for realtime session: %w", err)
		}

		switch event.Type {
		case "session.updated":
			return nil
		case eventTypeError:
			return realtimeError(event)
		default:
		}
	}
}

func realtimeError(event realtimeEvent) error {
	if event.Error == nil {
		return fmt.Errorf("%w: %s without details", ErrRealtime, event.Type)
	}

	return fmt.Errorf("%w: %s (%s %s)", ErrRealtime, event.Error.Message, event.Error.Type, event.Error.Code)
}

type realtimeDuplex struct {
	ctx       context.Context //nolint:containedctx // Say and Close have no context of their own; the call's bounds them.
	log       *slog.Logger
	conn      *websocket.Conn
	style     string
	input     chan []byte
	events    chan speech.Event
	output    chan []byte
	says      chan string
	interrupt chan struct{}
	done      chan struct{}

	closeOnce sync.Once
	mu        sync.Mutex
	err       error
}

var (
	_ speech.Duplex      = (*realtimeDuplex)(nil)
	_ speech.Interrupter = (*realtimeDuplex)(nil)
)

func (d *realtimeDuplex) Write(pcm []byte) error {
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

func (d *realtimeDuplex) Events() <-chan speech.Event { return d.events }
func (d *realtimeDuplex) Audio() <-chan []byte        { return d.output }

func (d *realtimeDuplex) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.err
}

// Say queues text; the session speaks one text at a time, in order.
func (d *realtimeDuplex) Say(text string) error {
	select {
	case d.says <- text:
		return nil
	case <-d.done:
		return ErrClosed
	default:
		return ErrBacklog
	}
}

// Interrupt drops text not yet spoken; the server already cancels the current reply.
func (d *realtimeDuplex) Interrupt() {
	select {
	case d.interrupt <- struct{}{}:
	default:
	}
}

func (d *realtimeDuplex) Close() error {
	d.shutdown()

	return nil
}

func (d *realtimeDuplex) shutdown() {
	d.closeOnce.Do(func() {
		close(d.done)
		_ = d.conn.Close(websocket.StatusNormalClosure, "")
	})
}

func (d *realtimeDuplex) finish(_ context.Context, err error) {
	d.mu.Lock()
	if d.err == nil && err != nil {
		d.err = err
	}
	d.mu.Unlock()

	d.shutdown()
}

func (d *realtimeDuplex) start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	raw := make(chan realtimeEvent)

	go func() {
		defer cancel()
		d.finish(ctx, d.writeLoop(ctx))
	}()

	go func() {
		defer cancel()
		defer close(raw)

		for {
			var event realtimeEvent
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

func (d *realtimeDuplex) writeLoop(ctx context.Context) error {
	return sendUlaw(ctx, d.conn, d.input, d.done, "input_audio_buffer.append")
}

// speaking tracks the replies the session asked for: one at a time, in order.
type speaking struct {
	queue  []string
	active bool
	// current is the reply being spoken; ignored is one the caller interrupted.
	current string
	ignored string
	// cancelPending is an interruption that came before the server named the reply.
	cancelPending bool
	said          strings.Builder
	saidAt        time.Time
	stopped       time.Time
}

func (d *realtimeDuplex) eventLoop(ctx context.Context, raw <-chan realtimeEvent) error {
	state := &speaking{}

	for {
		select {
		case <-ctx.Done():
			return nil
		case text := <-d.says:
			state.queue = append(state.queue, text)
		case <-d.interrupt:
			if err := d.cancel(ctx, state); err != nil {
				return err
			}
		case event, ok := <-raw:
			if !ok {
				return nil
			}

			if err := d.handle(ctx, state, event); err != nil {
				return err
			}
		}

		if err := d.speakNext(ctx, state); err != nil {
			return err
		}
	}
}

func (d *realtimeDuplex) handle(ctx context.Context, state *speaking, event realtimeEvent) error {
	switch event.Type {
	case "input_audio_buffer.speech_started":
		d.emit(ctx, speech.Event{Kind: speech.SpeechStarted, Text: "", Stopped: time.Time{}})
	case "input_audio_buffer.speech_stopped":
		state.stopped = time.Now()
	case "conversation.item.input_audio_transcription.completed":
		if text := strings.TrimSpace(event.Transcript); text != "" {
			d.emit(ctx, speech.Event{Kind: speech.Utterance, Text: text, Stopped: state.stopped})
		}
	case eventTypeError:
		// The session survives most errors, such as cancelling a reply that already ended.
		d.log.Warn("realtime error", "error", realtimeError(event))
	default:
		return d.handleResponse(ctx, state, event)
	}

	return nil
}

// handleResponse follows the reply the session is speaking.
func (d *realtimeDuplex) handleResponse(ctx context.Context, state *speaking, event realtimeEvent) error {
	switch event.Type {
	case "response.created":
		if event.Response == nil {
			return nil
		}

		state.current = event.Response.ID

		if state.cancelPending {
			state.cancelPending = false

			return d.cancelCurrent(ctx, state)
		}
	case "response.output_audio.delta":
		if event.ResponseID != "" && event.ResponseID == state.ignored {
			return nil
		}

		return d.play(ctx, event.Delta)
	case "response.output_audio_transcript.delta":
		if state.said.Len() == 0 {
			state.saidAt = time.Now()
		}

		state.said.WriteString(event.Delta)
	case "response.done":
		state.active = false
		state.current = ""
		state.cancelPending = false

		if said := strings.TrimSpace(state.said.String()); said != "" {
			// Debug only: the text is conversation content, which the bot chat already keeps.
			d.log.Debug("realtime voice said", "text", said, "started", state.saidAt.Format(time.RFC3339Nano))
		}

		state.said.Reset()
	default:
	}

	return nil
}

type responseCancel struct {
	Type       string `json:"type"`
	ResponseID string `json:"response_id"`
}

// cancel drops queued text and stops the reply being spoken. The reply sits
// outside the conversation, so it is cancelled by ID, and its late audio is dropped.
func (d *realtimeDuplex) cancel(ctx context.Context, state *speaking) error {
	state.queue = nil

	if !state.active {
		return nil
	}

	if state.current == "" {
		state.cancelPending = true

		return nil
	}

	return d.cancelCurrent(ctx, state)
}

func (d *realtimeDuplex) cancelCurrent(ctx context.Context, state *speaking) error {
	state.ignored = state.current

	return sendJSON(ctx, d.conn, responseCancel{Type: "response.cancel", ResponseID: state.current})
}

// speakNext asks for the next queued text once the previous reply is done.
func (d *realtimeDuplex) speakNext(ctx context.Context, state *speaking) error {
	if state.active || len(state.queue) == 0 {
		return nil
	}

	text := state.queue[0]
	state.queue = state.queue[1:]
	state.active = true

	return sendJSON(ctx, d.conn, sayRequest(d.style, text))
}

type responseCreate struct {
	Type     string `json:"type"`
	Response struct {
		// "none" keeps the reply out of the conversation, so the model never builds on its own words.
		Conversation     string   `json:"conversation"`
		OutputModalities []string `json:"output_modalities"`
		Instructions     string   `json:"instructions"`
		// Empty input: the reply sees only its instructions, not the call so far.
		Input []any `json:"input"`
	} `json:"response"`
}

// sayRequest asks for text spoken word for word; OpenAI does not guarantee it.
func sayRequest(style, text string) responseCreate {
	var request responseCreate

	request.Type = "response.create"
	request.Response.Conversation = "none"
	request.Response.OutputModalities = []string{"audio"}
	request.Response.Input = []any{}

	instructions := "Say exactly the following text, word for word, and nothing else. Do not answer it or comment on it.\n\n" + text
	if style != "" {
		instructions = style + "\n\n" + instructions
	}

	request.Response.Instructions = instructions

	return request
}

func (d *realtimeDuplex) emit(ctx context.Context, event speech.Event) {
	select {
	case d.events <- event:
	case <-ctx.Done():
	}
}

func (d *realtimeDuplex) play(ctx context.Context, delta string) error {
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
