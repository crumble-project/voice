package call

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/speech"
)

// Speech connects a call's audio to speech services.
type Speech interface {
	// Start begins hearing the caller and speaking into out, which paces writes in real time.
	Start(ctx context.Context, out io.Writer, log *slog.Logger) (Session, error)
	// Acknowledges reports whether the speech side answers the caller by itself while
	// the bot thinks, which makes a holding phrase unnecessary.
	Acknowledges() bool
}

// Session is one call's speech: it hears the caller and says text.
type Session interface {
	// Write sends the caller's telephone PCM.
	Write(pcm []byte) error
	// Events closes when hearing ends; Err then tells why.
	Events() <-chan speech.Event
	Err() error
	// Say queues text to speak; Stop drops whatever is queued or playing.
	Say(text string)
	// Greet says the opening line of a call.
	Greet(text string)
	Stop()
	// Failed reports an error that ends the call.
	Failed() <-chan error
	Close()
}

// Pipeline transcribes the caller and speaks with a separate synthesizer.
type Pipeline struct {
	Transcriber speech.Transcriber
	Synthesizer speech.Synthesizer
}

// Start opens a transcription and a player.
//
//nolint:ireturn // Start implements Speech, whose sessions are interchangeable.
func (p Pipeline) Start(ctx context.Context, out io.Writer, log *slog.Logger) (Session, error) {
	transcription, err := p.Transcriber.Transcribe(ctx)
	if err != nil {
		return nil, fmt.Errorf("start transcription: %w", err)
	}

	return &pipelineSession{transcription: transcription, player: newPlayer(ctx, out, p.Synthesizer, log)}, nil
}

// Acknowledges is false: the pipeline only speaks what it is given.
func (Pipeline) Acknowledges() bool { return false }

type pipelineSession struct {
	transcription speech.Transcription
	player        *player
}

func (s *pipelineSession) Write(pcm []byte) error {
	if err := s.transcription.Write(pcm); err != nil {
		return fmt.Errorf("transcribe: %w", err)
	}

	return nil
}

func (s *pipelineSession) Events() <-chan speech.Event { return s.transcription.Events() }

func (s *pipelineSession) Err() error {
	if err := s.transcription.Err(); err != nil {
		return fmt.Errorf("transcription: %w", err)
	}

	return nil
}

func (s *pipelineSession) Say(text string)      { s.player.say(text) }
func (s *pipelineSession) Greet(text string)    { s.player.say(text) }
func (s *pipelineSession) Stop()                { s.player.stop() }
func (s *pipelineSession) Failed() <-chan error { return s.player.failed }
func (s *pipelineSession) Close()               { s.player.close(); _ = s.transcription.Close() }

// Live uses one full-duplex voice model for hearing and speaking.
type Live struct {
	Model speech.Live
	// Acknowledging is true when the model answers the caller by itself while the
	// bot works, which makes the holding phrase unnecessary.
	Acknowledging bool
}

// Start opens the live session and plays its audio into out.
//
//nolint:ireturn // Start implements Speech, whose sessions are interchangeable.
func (l Live) Start(ctx context.Context, out io.Writer, log *slog.Logger) (Session, error) {
	duplex, err := l.Model.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open live voice: %w", err)
	}

	session := &liveSession{
		duplex: duplex, out: out, log: log,
		// The model sends audio faster than real time; a minute of frames covers a long answer.
		frames: make(chan []byte, liveBuffer), failed: make(chan error, 1), done: make(chan struct{}),
	}

	session.ended.Go(func() { session.split(ctx) })
	session.ended.Go(func() { session.play(ctx) })

	return session, nil
}

// Acknowledges reports whether the model fills the wait by itself.
func (l Live) Acknowledges() bool { return l.Acknowledging }

// A minute of 20 ms frames.
const liveBuffer = 3000

type liveSession struct {
	duplex speech.Duplex
	out    io.Writer
	log    *slog.Logger
	frames chan []byte
	failed chan error
	done   chan struct{}
	ended  sync.WaitGroup
	once   sync.Once
}

func (s *liveSession) Write(pcm []byte) error {
	if err := s.duplex.Write(pcm); err != nil {
		return fmt.Errorf("send to live voice: %w", err)
	}

	return nil
}

func (s *liveSession) Events() <-chan speech.Event { return s.duplex.Events() }

func (s *liveSession) Err() error {
	if err := s.duplex.Err(); err != nil {
		return fmt.Errorf("live voice: %w", err)
	}

	return nil
}

func (s *liveSession) Say(text string) {
	if err := s.duplex.Say(text); err != nil {
		s.fail(fmt.Errorf("say through live voice: %w", err))
	}
}

// Greet lets a model that needs it word the opening line its own way.
func (s *liveSession) Greet(text string) {
	greeter, ok := s.duplex.(speech.Greeter)
	if !ok {
		s.Say(text)

		return
	}

	if err := greeter.Greet(text); err != nil {
		s.fail(fmt.Errorf("greet through live voice: %w", err))
	}
}

// Stop drops the model's buffered speech and, where the model supports it, the
// text it has not said yet.
func (s *liveSession) Stop() {
	if interrupter, ok := s.duplex.(speech.Interrupter); ok {
		interrupter.Interrupt()
	}

	for {
		select {
		case <-s.frames:
		default:
			return
		}
	}
}

func (s *liveSession) Failed() <-chan error { return s.failed }

func (s *liveSession) Close() {
	s.once.Do(func() {
		close(s.done)
		_ = s.duplex.Close()
		s.ended.Wait()
	})
}

func (s *liveSession) fail(err error) {
	select {
	case s.failed <- err:
	default:
	}
}

// split cuts the model's audio into 20 ms frames.
func (s *liveSession) split(ctx context.Context) {
	var pending []byte

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case chunk, ok := <-s.duplex.Audio():
			if !ok {
				return
			}

			pending = append(pending, chunk...)

			for len(pending) >= audio.FrameBytes {
				select {
				case s.frames <- pending[:audio.FrameBytes:audio.FrameBytes]:
				default:
					s.log.Warn("live voice buffer full, dropping audio")
				}

				pending = pending[audio.FrameBytes:]
			}
		}
	}
}

func (s *liveSession) play(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case frame := <-s.frames:
			if _, err := s.out.Write(frame); err != nil {
				s.fail(fmt.Errorf("play live voice: %w", err))

				return
			}
		}
	}
}
