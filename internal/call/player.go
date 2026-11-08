package call

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/speech"
)

// Enough for a long reply split into sentences plus a holding phrase.
const playerQueue = 64

// player speaks queued text in order through a synthesizer. While one sentence
// plays, the synthesizer holds the next one ready, so sentences follow without a gap.
type player struct {
	out         io.Writer
	synthesizer speech.Synthesizer
	log         *slog.Logger

	queue  chan utterance
	ready  chan prepared
	failed chan error
	done   chan struct{}
	ended  sync.WaitGroup

	mu         sync.Mutex
	parent     context.Context //nolint:containedctx // Each generation's context derives from the call's.
	generation uint64
	genCtx     context.Context //nolint:containedctx // Cancelled as a whole when the caller interrupts.
	genCancel  context.CancelFunc
}

// utterance is queued text tagged with the generation it was queued in,
// so text queued before a stop is never spoken after it.
type utterance struct {
	text       string
	generation uint64
	queued     time.Time
}

type prepared struct {
	utterance

	stream io.ReadCloser
}

func newPlayer(ctx context.Context, out io.Writer, synthesizer speech.Synthesizer, log *slog.Logger) *player {
	genCtx, genCancel := context.WithCancel(ctx)

	p := &player{
		out: out, synthesizer: synthesizer, log: log,
		queue: make(chan utterance, playerQueue), ready: make(chan prepared),
		failed: make(chan error, 1), done: make(chan struct{}),
		parent: ctx, genCtx: genCtx, genCancel: genCancel,
	}

	p.ended.Go(func() { p.synthesize(ctx) })
	p.ended.Go(func() { p.play(ctx) })

	return p
}

// say queues text. A full queue drops the text, since the caller could not follow it anyway.
func (p *player) say(text string) {
	p.mu.Lock()
	item := utterance{text: text, generation: p.generation, queued: time.Now()}
	p.mu.Unlock()

	select {
	case p.queue <- item:
	default:
		p.log.Warn("speech queue full, dropping text", "chars", len(text))
	}
}

// stop silences current speech and drops queued text, for when the caller starts talking.
func (p *player) stop() {
	p.mu.Lock()
	p.generation++
	p.genCancel()
	p.genCtx, p.genCancel = context.WithCancel(p.parent)
	p.mu.Unlock()

	for {
		select {
		case <-p.queue:
		case item := <-p.ready:
			_ = item.stream.Close()
		default:
			return
		}
	}
}

func (p *player) close() {
	close(p.done)
	p.stop()
	p.ended.Wait()

	p.mu.Lock()
	p.genCancel()
	p.mu.Unlock()
}

// current returns the context for generation, or nil when the generation was stopped.
func (p *player) current(generation uint64) context.Context {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.generation != generation {
		return nil
	}

	return p.genCtx
}

func (p *player) fail(err error) {
	select {
	case p.failed <- err:
	default:
	}
}

func (p *player) synthesize(ctx context.Context) {
	for {
		var item utterance

		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case item = <-p.queue:
		}

		genCtx := p.current(item.generation)
		if genCtx == nil {
			continue
		}

		// genCtx derives from the call's context and also ends when the caller interrupts.
		stream, err := p.synthesizer.Synthesize(genCtx, item.text) //nolint:contextcheck // See above.
		if err != nil {
			if genCtx.Err() != nil {
				continue
			}

			p.fail(fmt.Errorf("synthesize speech: %w", err))

			return
		}

		select {
		case p.ready <- prepared{utterance: item, stream: stream}:
		case <-p.done:
			_ = stream.Close()

			return
		case <-ctx.Done():
			_ = stream.Close()

			return
		}
	}
}

func (p *player) play(ctx context.Context) {
	frame := make([]byte, audio.FrameBytes)

	for {
		var item prepared

		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case item = <-p.ready:
		}

		err := p.playOne(item, frame)
		_ = item.stream.Close()

		if err != nil {
			p.fail(err)

			return
		}
	}
}

func (p *player) playOne(item prepared, frame []byte) error {
	first := true

	for {
		n, readErr := io.ReadFull(item.stream, frame)
		if p.current(item.generation) == nil {
			return nil
		}

		if n > 0 {
			if first {
				first = false

				// waited_ms covers synthesis and any sentence still playing before this one.
				p.log.Info("speaking", "chars", len(item.text), "waited_ms", time.Since(item.queued).Milliseconds())
			}

			clear(frame[n:])

			if _, err := p.out.Write(frame); err != nil {
				return fmt.Errorf("play speech: %w", err)
			}
		}

		switch {
		case readErr == nil:
		case errors.Is(readErr, io.EOF), errors.Is(readErr, io.ErrUnexpectedEOF):
			return nil
		case p.current(item.generation) == nil:
			return nil
		default:
			return fmt.Errorf("read speech: %w", readErr)
		}
	}
}
