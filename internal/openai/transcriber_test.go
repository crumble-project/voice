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

type received struct {
	query  string
	auth   string
	update map[string]any
	audio  []byte
}

// realtimeServer accepts one transcription session and replies like the Realtime API.
func realtimeServer(t *testing.T, got chan<- received) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)

			return
		}

		defer func() { _ = conn.CloseNow() }()

		ctx := r.Context()
		result := received{query: r.URL.RawQuery, auth: r.Header.Get("Authorization")}

		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Errorf("read update: %v", err)

			return
		}

		if decodeErr := json.Unmarshal(data, &result.update); decodeErr != nil {
			t.Errorf("decode update: %v", decodeErr)
		}

		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"session.updated","session":{}}`))

		_, data, err = conn.Read(ctx)
		if err != nil {
			t.Errorf("read append: %v", err)

			return
		}

		var appended struct {
			Type  string `json:"type"`
			Audio string `json:"audio"`
		}

		_ = json.Unmarshal(data, &appended)
		result.audio, _ = base64.StdEncoding.DecodeString(appended.Audio)

		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"input_audio_buffer.speech_started","audio_start_ms":0}`))
		_ = conn.Write(ctx, websocket.MessageText,
			[]byte(`{"type":"conversation.item.input_audio_transcription.completed","transcript":"  hallo  "}`))

		got <- result

		// Returns once the client closes the session.
		_, _, _ = conn.Read(ctx)
	}))
}

func field(t *testing.T, object map[string]any, name string) map[string]any {
	t.Helper()

	value, ok := object[name].(map[string]any)
	if !ok {
		t.Fatalf("%q is not an object in %v", name, object)
	}

	return value
}

func TestTranscriptionSession(t *testing.T) {
	t.Parallel()

	got := make(chan received, 1)
	server := realtimeServer(t, got)
	t.Cleanup(server.Close)

	base, _ := url.Parse(server.URL)
	transcriber := openai.NewTranscriber(base, "sk-test", "gpt-4o-transcribe", "de", slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := transcriber.Transcribe(ctx)
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}

	defer func() { _ = session.Close() }()

	if err := session.Write(make([]byte, audio.FrameBytes)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	first, second := <-session.Events(), <-session.Events()
	if first.Kind != speech.SpeechStarted || second.Kind != speech.Utterance || second.Text != "hallo" {
		t.Fatalf("events = %+v, %+v", first, second)
	}

	result := <-got

	if result.query != "intent=transcription" || result.auth != "Bearer sk-test" {
		t.Errorf("query = %q, auth = %q", result.query, result.auth)
	}

	format := field(t, field(t, field(t, field(t, result.update, "session"), "audio"), "input"), "format")

	if result.update["type"] != "session.update" || format["type"] != "audio/pcm" || format["rate"] != float64(24000) {
		t.Errorf("update = %v", result.update)
	}

	// 20 ms at 8 kHz becomes 20 ms at 24 kHz.
	if len(result.audio) != audio.FrameBytes*audio.Factor {
		t.Errorf("appended %d bytes, want %d", len(result.audio), audio.FrameBytes*audio.Factor)
	}
}

// TestAudioBurstIsBatched covers a call whose audio arrives faster than single
// messages go out, which ended calls with ErrBacklog after about a second.
func TestAudioBurstIsBatched(t *testing.T) {
	t.Parallel()

	const frames = 200

	type upload struct{ bytes, messages int }

	uploaded := make(chan upload, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		defer func() { _ = conn.CloseNow() }()

		conn.SetReadLimit(1 << 20)

		ctx := r.Context()
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}

		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"session.updated","session":{}}`))

		var total upload

		for total.bytes < frames*audio.FrameBytes*audio.Factor {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}

			var appended struct {
				Audio string `json:"audio"`
			}

			_ = json.Unmarshal(data, &appended)
			decoded, _ := base64.StdEncoding.DecodeString(appended.Audio)
			total.bytes += len(decoded)
			total.messages++
		}

		uploaded <- total

		_, _, _ = conn.Read(ctx)
	}))
	t.Cleanup(server.Close)

	base, _ := url.Parse(server.URL)
	transcriber := openai.NewTranscriber(base, "sk-test", "gpt-4o-transcribe", "", slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := transcriber.Transcribe(ctx)
	if err != nil {
		t.Fatalf("Transcribe: %v", err)
	}

	defer func() { _ = session.Close() }()

	for i := range frames {
		if err := session.Write(make([]byte, audio.FrameBytes)); err != nil {
			t.Fatalf("Write frame %d: %v", i, err)
		}
	}

	select {
	case got := <-uploaded:
		if got.messages >= frames {
			t.Errorf("sent %d messages for %d frames, want batching", got.messages, frames)
		}
	case <-ctx.Done():
		t.Fatal("not all audio arrived")
	}
}
