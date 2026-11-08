package openai_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/openai"
	"github.com/crumble-project/crumble-voice/internal/speech"
)

type realtimeReceived struct {
	model    string
	update   map[string]any
	creates  []map[string]any
	cancels  []map[string]any
	appended int
}

// realtimeConversation plays the Realtime API through script and reports what it received.
func realtimeConversation(
	t *testing.T, got chan<- realtimeReceived, script func(read func() map[string]any, write func(string)) realtimeReceived,
) *httptest.Server {
	t.Helper()

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}

		defer func() { _ = conn.CloseNow() }()

		ctx := r.Context()

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

		result := script(read, write)
		result.model = r.URL.Query().Get("model")
		got <- result

		for event := read(); event != nil; event = read() {
			_ = event
		}
	}))
}

func TestRealtimeSession(t *testing.T) {
	t.Parallel()

	got := make(chan realtimeReceived, 1)
	server := realtimeConversation(t, got, func(read func() map[string]any, write func(string)) realtimeReceived {
		var result realtimeReceived

		result.update = read()
		write(`{"type":"session.updated","session":{}}`)

		appended := read()
		encoded, _ := appended["audio"].(string)
		decoded, _ := base64.StdEncoding.DecodeString(encoded)
		result.appended = len(decoded)

		write(`{"type":"input_audio_buffer.speech_started","audio_start_ms":0,"item_id":"item_1"}`)
		write(`{"type":"input_audio_buffer.speech_stopped","audio_end_ms":900,"item_id":"item_1"}`)
		write(`{"type":"conversation.item.input_audio_transcription.completed",` +
			`"item_id":"item_1","content_index":0,"transcript":" How are you? "}`)

		// The client asks for one reply at a time.
		result.creates = append(result.creates, read())
		write(`{"type":"response.created","response":{"id":"resp_1"}}`)
		write(`{"type":"response.output_audio.delta","response_id":"resp_1","delta":"` +
			base64.StdEncoding.EncodeToString(make([]byte, audio.FrameSamples)) + `"}`)
		write(`{"type":"error","error":{"type":"invalid_request_error","code":"some_code","message":"not fatal"}}`)
		write(`{"type":"response.done","response":{"id":"resp_1","status":"completed"}}`)

		result.creates = append(result.creates, read())
		write(`{"type":"response.created","response":{"id":"resp_2"}}`)

		// The caller interrupts: the client cancels resp_2 by ID.
		result.cancels = append(result.cancels, read())

		return result
	})
	t.Cleanup(server.Close)

	base, _ := url.Parse(server.URL)
	realtime := openai.NewRealtime(base, "sk-test", openai.RealtimeOptions{
		Model: "gpt-realtime-2.1-mini", TranscriptionModel: "gpt-4o-mini-transcribe", Language: "en",
		Voice: "marin", Style: "Speak calmly.",
	}, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := realtime.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	defer func() { _ = session.Close() }()

	if err := session.Write(make([]byte, audio.FrameBytes)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	started, heard := <-session.Events(), <-session.Events()
	if started.Kind != speech.SpeechStarted || heard.Kind != speech.Utterance || heard.Text != "How are you?" || heard.Stopped.IsZero() {
		t.Fatalf("events = %+v, %+v", started, heard)
	}

	for _, text := range []string{"I'm fine.", "And you?"} {
		if err := session.Say(text); err != nil {
			t.Fatalf("Say: %v", err)
		}
	}

	if pcm := <-session.Audio(); len(pcm) != audio.FrameBytes {
		t.Errorf("audio = %d bytes, want %d", len(pcm), audio.FrameBytes)
	}

	// Wait until the second reply started, then interrupt it.
	time.Sleep(100 * time.Millisecond)
	interrupter, ok := session.(speech.Interrupter)
	if !ok {
		t.Fatal("the realtime session cannot be interrupted")
	}

	interrupter.Interrupt()

	checkRealtimeWire(t, <-got)
}

func checkRealtimeWire(t *testing.T, result realtimeReceived) {
	t.Helper()

	if result.model != "gpt-realtime-2.1-mini" {
		t.Errorf("model = %q", result.model)
	}

	inner := field(t, result.update, "session")
	input := field(t, field(t, inner, "audio"), "input")
	turns := field(t, input, "turn_detection")

	if turns["create_response"] != false || turns["interrupt_response"] != true || turns["type"] != "server_vad" {
		t.Errorf("turn_detection = %v, want the model never to answer by itself", turns)
	}

	if field(t, input, "format")["type"] != "audio/pcmu" || field(t, input, "transcription")["model"] != "gpt-4o-mini-transcribe" {
		t.Errorf("input = %v", input)
	}

	if result.appended != audio.FrameSamples {
		t.Errorf("appended %d bytes of μ-law, want %d", result.appended, audio.FrameSamples)
	}

	if len(result.creates) != 2 {
		t.Fatalf("creates = %v", result.creates)
	}

	first := field(t, result.creates[0], "response")
	instructions, _ := first["instructions"].(string)

	if first["conversation"] != "none" || !strings.HasPrefix(instructions, "Speak calmly.") || !strings.HasSuffix(instructions, "I'm fine.") {
		t.Errorf("first response.create = %v", first)
	}

	if len(result.cancels) != 1 || result.cancels[0]["type"] != "response.cancel" || result.cancels[0]["response_id"] != "resp_2" {
		t.Errorf("cancels = %v, want resp_2 cancelled", result.cancels)
	}
}

func TestRealtimeInterruptBeforeTheReplyIsNamed(t *testing.T) {
	t.Parallel()

	requested, interrupted := make(chan struct{}), make(chan struct{})
	got := make(chan realtimeReceived, 1)
	server := realtimeConversation(t, got, func(read func() map[string]any, write func(string)) realtimeReceived {
		var result realtimeReceived

		result.update = read()
		write(`{"type":"session.updated","session":{}}`)

		result.creates = append(result.creates, read())
		close(requested)
		<-interrupted

		write(`{"type":"response.created","response":{"id":"resp_1"}}`)
		result.cancels = append(result.cancels, read())

		return result
	})
	t.Cleanup(server.Close)

	base, _ := url.Parse(server.URL)
	realtime := openai.NewRealtime(base, "sk-test", openai.RealtimeOptions{Model: "gpt-realtime-2.1-mini"},
		slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	session, err := realtime.Open(ctx)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	defer func() { _ = session.Close() }()

	if err := session.Say("Hello."); err != nil {
		t.Fatalf("Say: %v", err)
	}

	<-requested

	interrupter, ok := session.(speech.Interrupter)
	if !ok {
		t.Fatal("the realtime session cannot be interrupted")
	}

	interrupter.Interrupt()
	// The interruption must reach the session before the server names the reply.
	time.Sleep(100 * time.Millisecond)
	close(interrupted)

	select {
	case result := <-got:
		if len(result.cancels) != 1 || result.cancels[0]["response_id"] != "resp_1" {
			t.Fatalf("cancels = %v, want resp_1 cancelled", result.cancels)
		}
	case <-ctx.Done():
		t.Fatal("the interrupted reply was never cancelled")
	}
}
