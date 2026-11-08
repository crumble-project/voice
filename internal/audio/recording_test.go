package audio_test

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/crumble-project/crumble-voice/internal/audio"
)

// writeWAV writes a PCM WAV file with the given format and sample count.
func writeWAV(t *testing.T, rate uint32, channels, bits uint16, samples uint32) string {
	t.Helper()

	data := samples * uint32(channels) * uint32(bits/8)
	header := []any{
		[4]byte{'R', 'I', 'F', 'F'},
		36 + data,
		[4]byte{'W', 'A', 'V', 'E'},
		[4]byte{'f', 'm', 't', ' '},
		uint32(16), uint16(1), channels, rate,
		rate * uint32(channels) * uint32(bits/8), channels * bits / 8, bits,
		[4]byte{'d', 'a', 't', 'a'},
		data,
	}

	path := filepath.Join(t.TempDir(), "recording.wav")

	file, err := os.Create(path) //nolint:gosec // The path is inside the test's own temporary directory.
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = file.Close() }()

	for _, field := range header {
		if err := binary.Write(file, binary.LittleEndian, field); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := file.Write(make([]byte, data)); err != nil {
		t.Fatal(err)
	}

	return path
}

func TestLoadRecording(t *testing.T) {
	t.Parallel()

	pcm, err := audio.LoadRecording(writeWAV(t, audio.TelephoneRate, 1, 16, audio.TelephoneRate))
	if err != nil {
		t.Fatalf("LoadRecording: %v", err)
	}

	if len(pcm) != audio.TelephoneRate*audio.BytesPerSample {
		t.Errorf("one second became %d bytes", len(pcm))
	}

	for name, path := range map[string]string{
		"16 kHz": writeWAV(t, 16000, 1, 16, 16000),
		"stereo": writeWAV(t, audio.TelephoneRate, 2, 16, audio.TelephoneRate),
		"8-bit":  writeWAV(t, audio.TelephoneRate, 1, 8, audio.TelephoneRate),
		"empty":  writeWAV(t, audio.TelephoneRate, 1, 16, 0),
		"long":   writeWAV(t, audio.TelephoneRate, 1, 16, audio.TelephoneRate*uint32(audio.MaxRecording.Seconds()+1)),
	} {
		if _, err := audio.LoadRecording(path); !errors.Is(err, audio.ErrRecordingFormat) {
			t.Errorf("%s: err = %v, want ErrRecordingFormat", name, err)
		}
	}
}
