package audio

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	diagoaudio "github.com/emiago/diago/audio"
)

// MaxRecording bounds a recording played to callers.
const MaxRecording = 2 * time.Minute

// ErrRecordingFormat means a recording is not 8 kHz mono 16-bit PCM WAV.
var ErrRecordingFormat = errors.New("recording must be a WAV file of 8 kHz mono 16-bit PCM")

// ConvertHint is the macOS command that turns any audio file into a playable recording.
const ConvertHint = "convert it with: afconvert -f WAVE -d LEI16@8000 -c 1 input.m4a recording.wav"

const (
	wavPCM        = 1
	bitsPerSample = 16
)

// LoadRecording reads a WAV file into telephone PCM, checking its format and length.
func LoadRecording(path string) ([]byte, error) {
	file, err := os.Open(path) //nolint:gosec // The path is the operator's own configuration.
	if err != nil {
		return nil, fmt.Errorf("open recording: %w", err)
	}

	defer func() { _ = file.Close() }()

	reader := diagoaudio.NewWavReader(file)

	err = reader.ReadHeaders()
	if err != nil {
		return nil, fmt.Errorf("read recording %s: %w; %s", path, err, ConvertHint)
	}

	if reader.WavAudioFormat != wavPCM || reader.SampleRate != TelephoneRate || reader.NumChannels != 1 ||
		reader.BitsPerSample != bitsPerSample {
		return nil, fmt.Errorf("%w: %s is %d Hz, %d channel(s), %d-bit, format %d; %s", ErrRecordingFormat, path,
			reader.SampleRate, reader.NumChannels, reader.BitsPerSample, reader.WavAudioFormat, ConvertHint)
	}

	limit := int64(MaxRecording.Seconds()) * TelephoneRate * BytesPerSample

	pcm, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read recording %s: %w", path, err)
	}

	if int64(len(pcm)) > limit {
		return nil, fmt.Errorf("%w: %s is longer than %s", ErrRecordingFormat, path, MaxRecording)
	}

	if len(pcm) < FrameBytes {
		return nil, fmt.Errorf("%w: %s holds no audio", ErrRecordingFormat, path)
	}

	return pcm[:len(pcm)&^1], nil
}
