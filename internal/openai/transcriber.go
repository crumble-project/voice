// Package openai implements speech services with the OpenAI Realtime and Audio APIs.
package openai

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/speech"
)

const (
	sessionTimeout = 10 * time.Second
	writeTimeout   = 5 * time.Second
	frameDuration  = 20 * time.Millisecond
	// Five seconds of 20 ms frames; a longer backlog means the connection cannot keep up.
	audioBacklog = 250
	// At most one second of audio per append message.
	maxBatchBytes = 50 * audio.FrameBytes
	slowSend      = 200 * time.Millisecond
	maxEvent      = 1 << 20
	eventBacklog  = 16
)

var (
	// ErrBacklog means audio arrived faster than the transcription connection sent it.
	ErrBacklog = errors.New("transcription connection is not keeping up with the call audio")
	// ErrClosed means the transcription session already ended.
	ErrClosed = errors.New("transcription is closed")
	// ErrRealtime is an error event from the Realtime API.
	ErrRealtime = errors.New("realtime API error")
	// ErrBinaryMessage means the server sent a frame the protocol does not use.
	ErrBinaryMessage = errors.New("unexpected binary realtime message")
)

// Server VAD settings; the silence leaves room for a short pause mid-sentence.
const (
	vadThreshold      = 0.5
	vadPrefixPadding  = 300
	vadSilenceSeconds = 600
)

// Transcriber opens Realtime transcription sessions.
type Transcriber struct {
	log      *slog.Logger
	base     *url.URL
	apiKey   string
	model    string
	language string
}

// NewTranscriber returns a transcriber for an API base such as https://api.openai.com.
func NewTranscriber(base *url.URL, apiKey, model, language string, log *slog.Logger) *Transcriber {
	return &Transcriber{log: log, base: base, apiKey: apiKey, model: model, language: language}
}

var _ speech.Transcriber = (*Transcriber)(nil)

// Transcribe connects and configures a session, returning once the server accepted the configuration.
//
//nolint:ireturn // Transcribe implements speech.Transcriber, whose sessions are provider-neutral.
func (t *Transcriber) Transcribe(ctx context.Context) (speech.Transcription, error) {
	dialCtx, cancel := context.WithTimeout(ctx, sessionTimeout)
	defer cancel()

	conn, err := dial(dialCtx, t.base, "/v1/realtime", url.Values{"intent": {"transcription"}}, t.apiKey)
	if err != nil {
		return nil, fmt.Errorf("realtime transcription: %w", err)
	}

	session := &transcription{
		log:    t.log,
		conn:   conn,
		audio:  make(chan []byte, audioBacklog),
		events: make(chan speech.Event, eventBacklog),
		done:   make(chan struct{}),
	}

	if err := session.configure(dialCtx, t.model, t.language); err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "configuration failed")

		return nil, err
	}

	session.start(ctx)

	return session, nil
}

type transcription struct {
	log    *slog.Logger
	conn   *websocket.Conn
	audio  chan []byte
	events chan speech.Event
	done   chan struct{}

	upsampler audio.Upsampler

	closeOnce sync.Once
	mu        sync.Mutex
	err       error
}

type inputFormat struct {
	Type string `json:"type"`
	Rate int    `json:"rate"`
}

type transcriptionConfig struct {
	Model    string `json:"model"`
	Language string `json:"language,omitempty"`
}

type turnDetection struct {
	Type              string  `json:"type"`
	Threshold         float64 `json:"threshold"`
	PrefixPaddingMS   int     `json:"prefix_padding_ms"`
	SilenceDurationMS int     `json:"silence_duration_ms"`
}

type noiseReduction struct {
	Type string `json:"type"`
}

type audioInput struct {
	Format         inputFormat         `json:"format"`
	Transcription  transcriptionConfig `json:"transcription"`
	TurnDetection  turnDetection       `json:"turn_detection"`
	NoiseReduction noiseReduction      `json:"noise_reduction"`
}

type sessionUpdate struct {
	Type    string `json:"type"`
	Session struct {
		Type  string `json:"type"`
		Audio struct {
			Input audioInput `json:"input"`
		} `json:"audio"`
	} `json:"session"`
}

type serverEvent struct {
	Type       string `json:"type"`
	Transcript string `json:"transcript"`
	Error      *struct {
		Type    string `json:"type"`
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (s *transcription) Write(pcm []byte) error {
	select {
	case <-s.done:
		if err := s.Err(); err != nil {
			return err
		}

		return ErrClosed
	default:
	}

	select {
	case s.audio <- pcm:
		return nil
	default:
		s.finish(ErrBacklog)

		return ErrBacklog
	}
}

func (s *transcription) Events() <-chan speech.Event { return s.events }

func (s *transcription) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.err
}

func (s *transcription) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
		_ = s.conn.Close(websocket.StatusNormalClosure, "")
	})

	return nil
}

func (s *transcription) configure(ctx context.Context, model, language string) error {
	var update sessionUpdate

	update.Type = "session.update"
	update.Session.Type = "transcription"
	update.Session.Audio.Input = audioInput{
		// The documented PCM format is 24 kHz, so the call audio is upsampled from 8 kHz.
		Format:        inputFormat{Type: "audio/pcm", Rate: audio.TelephoneRate * audio.Factor},
		Transcription: transcriptionConfig{Model: model, Language: language},
		TurnDetection: turnDetection{
			Type: "server_vad", Threshold: vadThreshold, PrefixPaddingMS: vadPrefixPadding, SilenceDurationMS: vadSilenceSeconds,
		},
		// A telephone handset is a close-talking microphone.
		NoiseReduction: noiseReduction{Type: "near_field"},
	}

	if err := s.send(ctx, update); err != nil {
		return err
	}

	for {
		event, err := s.read(ctx)
		if err != nil {
			return fmt.Errorf("wait for transcription session: %w", err)
		}

		switch event.Type {
		case "session.updated":
			return nil
		case eventTypeError:
			return eventError(event)
		default:
		}
	}
}

func (s *transcription) start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer cancel()
		s.finish(s.writeLoop(ctx))
	}()

	go func() {
		defer cancel()
		defer close(s.events)
		s.finish(s.readLoop(ctx))
	}()
}

func (s *transcription) writeLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.done:
			return nil
		case pcm := <-s.audio:
			// Everything queued while the last send was in flight goes out as one
			// message, so a slow link costs latency instead of dropping the call.
			batch := append([]byte(nil), pcm...)
			for drained := false; !drained && len(batch) < maxBatchBytes; {
				select {
				case more := <-s.audio:
					batch = append(batch, more...)
				default:
					drained = true
				}
			}

			if err := s.sendAudio(ctx, batch); err != nil {
				return err
			}
		}
	}
}

func (s *transcription) sendAudio(ctx context.Context, pcm []byte) error {
	upsampled := audio.Bytes(s.upsampler.Process(audio.Samples(pcm)))
	message := audioAppend{Type: "input_audio_buffer.append", Audio: base64.StdEncoding.EncodeToString(upsampled)}

	started := time.Now()

	if err := s.send(ctx, message); err != nil {
		return err
	}

	if elapsed := time.Since(started); elapsed > slowSend {
		s.log.Warn("slow transcription upload", "took", elapsed.String(), "audio", time.Duration(len(pcm)/audio.FrameBytes)*frameDuration)
	}

	return nil
}

func (s *transcription) readLoop(ctx context.Context) error {
	var stopped time.Time

	for {
		event, err := s.read(ctx)
		if err != nil {
			select {
			case <-s.done:
				return nil
			default:
				return err
			}
		}

		var out speech.Event

		switch event.Type {
		case "input_audio_buffer.speech_started":
			out = speech.Event{Kind: speech.SpeechStarted, Text: "", Stopped: time.Time{}}
		case "input_audio_buffer.speech_stopped":
			stopped = time.Now()

			continue
		case "conversation.item.input_audio_transcription.completed":
			text := strings.TrimSpace(event.Transcript)
			if text == "" {
				continue
			}

			out = speech.Event{Kind: speech.Utterance, Text: text, Stopped: stopped}
		case "conversation.item.input_audio_transcription.failed", eventTypeError:
			return eventError(event)
		default:
			continue
		}

		select {
		case s.events <- out:
		case <-ctx.Done():
			return nil
		}
	}
}

func (s *transcription) send(ctx context.Context, message any) error {
	return sendJSON(ctx, s.conn, message)
}

func (s *transcription) read(ctx context.Context) (serverEvent, error) {
	var event serverEvent

	err := readJSON(ctx, s.conn, &event)

	return event, err
}

func eventError(event serverEvent) error {
	if event.Error == nil {
		return fmt.Errorf("%w: %s without details", ErrRealtime, event.Type)
	}

	return fmt.Errorf("%w: %s: %s (%s %s)", ErrRealtime, event.Type, event.Error.Message, event.Error.Type, event.Error.Code)
}

func (s *transcription) finish(err error) {
	s.mu.Lock()
	if s.err == nil && err != nil {
		s.err = err
	}
	s.mu.Unlock()

	_ = s.Close()
}
