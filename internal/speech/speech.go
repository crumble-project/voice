// Package speech defines the speech services a call uses, independent of any provider.
//
// All audio is 16-bit little-endian mono PCM at audio.TelephoneRate.
package speech

import (
	"context"
	"io"
	"time"
)

// EventKind tells what a transcription event reports.
type EventKind int

const (
	// SpeechStarted means the caller began talking; playback should stop.
	SpeechStarted EventKind = iota + 1
	// Utterance carries the final text of one stretch of the caller's speech.
	Utterance
)

// Event is one result from a transcription session.
type Event struct {
	Kind EventKind
	Text string
	// Stopped is when the caller stopped speaking, for an Utterance; zero when unknown.
	Stopped time.Time
}

// Transcriber opens one streaming transcription per call.
type Transcriber interface {
	Transcribe(ctx context.Context) (Transcription, error)
}

// Transcription receives the caller's audio and reports speech and text.
type Transcription interface {
	// Write sends telephone PCM. It must not block for long; a call writes every 20 ms.
	Write(pcm []byte) error
	// Events closes when the session ends; Err then tells why.
	Events() <-chan Event
	Err() error
	Close() error
}

// Synthesizer turns text into speech.
type Synthesizer interface {
	// Synthesize streams telephone PCM for text. Closing the reader stops synthesis.
	Synthesize(ctx context.Context, text string) (io.ReadCloser, error)
}

// Live opens a full-duplex voice model that hears the caller and speaks itself.
type Live interface {
	Open(ctx context.Context) (Duplex, error)
}

// Duplex is one live voice session: a Transcription that also talks.
type Duplex interface {
	Transcription
	// Say asks the model to speak text to the caller. The model may rephrase it.
	Say(text string) error
	// Audio carries the model's speech as telephone PCM, faster than real time.
	Audio() <-chan []byte
}

// Greeter is a Duplex that needs the opening line of a call worded its own way.
type Greeter interface {
	Greet(text string) error
}

// Interrupter is a Duplex that drops speech it has not said yet when the caller talks.
type Interrupter interface {
	Interrupt()
}
