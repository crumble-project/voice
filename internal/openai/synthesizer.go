package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/speech"
)

const (
	// The speech endpoint accepts at most 4096 characters of input.
	maxSpeechInput = 4096
	maxErrorBody   = 4096
	// 100 ms of 24 kHz PCM per read.
	readChunk = 4800
)

var (
	// ErrInputTooLong means the text exceeds what one speech request accepts.
	ErrInputTooLong = errors.New("text is longer than one speech request accepts")
	// ErrStatus is an HTTP status the speech endpoint should not return.
	ErrStatus = errors.New("unexpected status")
)

// Synthesizer calls POST /v1/audio/speech.
//
// OpenAI lists gpt-4o-mini-tts and tts-1 as shutting down on 2027-01-06; the
// replacement runs through the Realtime API, so this type is the part to swap.
type Synthesizer struct {
	endpoint     string
	apiKey       string
	model        string
	voice        string
	instructions string
	http         *http.Client
}

// NewSynthesizer returns a synthesizer for an API base such as https://api.openai.com.
func NewSynthesizer(base *url.URL, apiKey, model, voice, instructions string, client *http.Client) *Synthesizer {
	return &Synthesizer{
		endpoint:     strings.TrimSuffix(base.String(), "/") + "/v1/audio/speech",
		apiKey:       apiKey,
		model:        model,
		voice:        voice,
		instructions: instructions,
		http:         client,
	}
}

var _ speech.Synthesizer = (*Synthesizer)(nil)

type speechRequest struct {
	Model          string `json:"model"`
	Input          string `json:"input"`
	Voice          string `json:"voice"`
	Instructions   string `json:"instructions,omitempty"`
	ResponseFormat string `json:"response_format"`
}

// Synthesize streams the spoken text as telephone PCM while the response arrives.
func (s *Synthesizer) Synthesize(ctx context.Context, text string) (io.ReadCloser, error) {
	if len([]rune(text)) > maxSpeechInput {
		return nil, ErrInputTooLong
	}

	body, err := json.Marshal(speechRequest{
		Model:        s.model,
		Input:        text,
		Voice:        s.voice,
		Instructions: s.instructions,
		// Raw 24 kHz 16-bit little-endian samples without a header.
		ResponseFormat: "pcm",
	})
	if err != nil {
		return nil, fmt.Errorf("encode speech request: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build speech request: %w", err)
	}

	request.Header.Set("Authorization", "Bearer "+s.apiKey)
	request.Header.Set("Content-Type", "application/json")

	response, err := s.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request speech: %w", err)
	}

	if response.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, maxErrorBody))
		_ = response.Body.Close()

		return nil, fmt.Errorf("request speech: %w %d: %s", ErrStatus, response.StatusCode, strings.TrimSpace(string(detail)))
	}

	return &telephonePCM{body: response.Body, downsampler: audio.NewDownsampler()}, nil
}

// telephonePCM downsamples a 24 kHz PCM body to telephone PCM as it is read.
type telephonePCM struct {
	body        io.ReadCloser
	downsampler *audio.Downsampler
	carry       []byte
	pending     []byte
	eof         bool
}

func (t *telephonePCM) Read(p []byte) (int, error) {
	for len(t.pending) == 0 {
		if t.eof {
			return 0, io.EOF
		}

		buffer := make([]byte, readChunk)

		n, err := t.body.Read(buffer)
		chunk := append(t.carry, buffer[:n]...) //nolint:gocritic // carry is reset just below.

		// Keep whole samples only; an odd trailing byte waits for the next read.
		whole := len(chunk) &^ 1
		t.carry = append([]byte(nil), chunk[whole:]...)
		t.pending = audio.Bytes(t.downsampler.Process(audio.Samples(chunk[:whole])))

		if errors.Is(err, io.EOF) {
			t.eof = true
		} else if err != nil {
			return 0, fmt.Errorf("read speech: %w", err)
		}
	}

	n := copy(p, t.pending)
	t.pending = t.pending[n:]

	return n, nil
}

func (t *telephonePCM) Close() error {
	if err := t.body.Close(); err != nil {
		return fmt.Errorf("close speech: %w", err)
	}

	return nil
}
