package openai_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/openai"
	"github.com/crumble-project/crumble-voice/internal/speech"
)

type liveReceived struct {
	path       string
	start      map[string]any
	audioBytes int
	commentary map[string]any
}

// liveServer plays GPT-Live: it confirms the session, transcribes, and speaks.
func liveServer(t *testing.T, got chan<- liveReceived) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		defer func() { _ = conn.CloseNow() }()

		ctx := r.Context()
		result := liveReceived{path: r.URL.Path}

		read := func() map[string]any {
			_, data, readErr := conn.Read(ctx)
			if readErr != nil {
				return nil
			}

			var event map[string]any

			_ = json.Unmarshal(data, &event)

			return event
		}

		write := func(event string) { _ = conn.Write(ctx, websocket.MessageText, []byte(event)) }

		result.start = read()
		write(`{"type":"session.started","session":{"id":"live_1","status":"active","model":"gpt-live-1"}}`)

		appended := read()
		encoded, _ := appended["audio"].(string)
		decoded, _ := base64.StdEncoding.DecodeString(encoded)
		result.audioBytes = len(decoded)

		write(`{"type":"session.input_transcript.delta","delta":"Wie ","start_ms":0,"end_ms":200}`)
		write(`{"type":"session.input_transcript.delta","delta":"geht's?","start_ms":200,"end_ms":600}`)
		write(`{"type":"session.output_transcript.delta","delta":"Mhm","start_ms":700,"end_ms":900}`)
		write(`{"type":"session.output_audio.delta","delta":"` +
			base64.StdEncoding.EncodeToString(make([]byte, audio.FrameSamples)) + `"}`)

		result.commentary = read()
		got <- result

		// Read until the client closes the session.
		for event := read(); event != nil; event = read() {
			_ = event
		}
	}))
}

func TestLiveSession(t *testing.T) {
	t.Parallel()

	got := make(chan liveReceived, 1)
	server := liveServer(t, got)
	t.Cleanup(server.Close)

	base, _ := url.Parse(server.URL)
	live := openai.NewLive(base, "sk-test", "gpt-live-1", "marin", "Only acknowledge.", slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := live.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	defer func() { _ = session.Close() }()

	if err := session.Write(make([]byte, audio.FrameBytes)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	started, heard := <-session.Events(), <-session.Events()
	if started.Kind != speech.SpeechStarted || heard.Kind != speech.Utterance || heard.Text != "Wie geht's?" {
		t.Fatalf("events = %+v, %+v", started, heard)
	}

	if pcm := <-session.Audio(); len(pcm) != audio.FrameBytes {
		t.Errorf("audio = %d bytes, want one telephone frame of %d", len(pcm), audio.FrameBytes)
	}

	if err := session.Say("Es ist sonnig."); err != nil {
		t.Fatalf("Say: %v", err)
	}

	checkLiveWire(t, <-got)
}

func checkLiveWire(t *testing.T, result liveReceived) {
	t.Helper()

	if result.path != "/v1/live/sessions" {
		t.Errorf("path = %q", result.path)
	}

	inner := field(t, result.start, "session")
	format := field(t, field(t, inner, "audio"), "format")

	if result.start["type"] != "session.start" || inner["model"] != "gpt-live-1" || inner["instructions"] != "Only acknowledge." ||
		format["type"] != "audio/pcmu" || format["rate"] != float64(audio.TelephoneRate) {
		t.Errorf("start = %v", result.start)
	}

	// One 20 ms frame of μ-law is one byte per sample.
	if result.audioBytes != audio.FrameSamples {
		t.Errorf("appended %d bytes of μ-law, want %d", result.audioBytes, audio.FrameSamples)
	}

	if result.commentary["type"] != "session.commentary.append" || result.commentary["content"] != "Es ist sonnig." {
		t.Errorf("commentary = %v", result.commentary)
	}

	if value, present := result.commentary["delegation_id"]; !present || value != nil {
		t.Errorf("delegation_id must be present and null, got %v", result.commentary)
	}
}
