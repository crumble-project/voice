package config_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/crumble-project/crumble-voice/internal/config"
)

const validConfig = `{
  "crumble": {"baseUrl": "http://127.0.0.1:3000", "email": "owner@example.com", "passwordEnv": "CRUMBLE_PASSWORD"},
  "sip": {
    "registrar": "192.168.1.1:5060", "transport": "udp", "bindHost": "0.0.0.0", "bindPort": 5070,
    "externalHost": "", "rtpPortStart": 20000, "rtpPortEnd": 20100, "registerExpiry": "5m"
  },
  "owner": {"numbers": ["**610", "+4930123456"]},
  "lines": [{"username": "ada", "passwordEnv": "SIP_PASSWORD_ADA", "displayName": "Ada", "botId": "bot_ada"}],
  "speech": {
    "mode": "pipeline", "provider": "openai", "apiKeyEnv": "OPENAI_API_KEY", "baseUrl": "https://api.openai.com",
    "transcriptionModel": "gpt-4o-transcribe", "language": "de",
    "synthesisModel": "gpt-4o-mini-tts", "voice": "marin", "voiceInstructions": "",
    "liveModel": "", "liveInstructions": "", "realtimeModel": ""
  },
  "control": {"listen": "127.0.0.1:8790", "tokenEnv": "CRUMBLE_VOICE_TOKEN"},
  "call": {
    "maxDuration": "30m", "turnTimeout": "2m", "maxConcurrent": 2,
    "callNote": "This message comes from a phone call.", "holdingPhrase": "One moment.", "holdingDelay": "3s",
    "timeoutPhrase": "That takes longer; the answer will be in the chat.", "failurePhrase": "Something went wrong.",
    "unansweredAfter": "30s", "answerGreeting": "Hi, this is {name}.",
    "strangerRecording": ""
  }
}`

func environment(overrides map[string]string) config.Environment {
	values := map[string]string{
		"CRUMBLE_PASSWORD":    "correct horse battery staple",
		"SIP_PASSWORD_ADA":    "line-secret",
		"OPENAI_API_KEY":      "sk-test",
		"CRUMBLE_VOICE_TOKEN": strings.Repeat("t", 32),
	}

	for name, value := range overrides {
		if value == "" {
			delete(values, name)
		} else {
			values[name] = value
		}
	}

	return func(name string) (string, bool) {
		value, ok := values[name]

		return value, ok
	}
}

func TestParseValid(t *testing.T) {
	t.Parallel()

	cfg, err := config.Parse(context.Background(), []byte(validConfig), environment(nil))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if cfg.SIP.RegistrarHost != "192.168.1.1" || cfg.SIP.RegistrarPort != 5060 {
		t.Errorf("registrar = %s:%d", cfg.SIP.RegistrarHost, cfg.SIP.RegistrarPort)
	}

	if cfg.Lines[0].Password != "line-secret" {
		t.Errorf("line password not read from the environment")
	}

	if cfg.Call.TurnTimeout != 2*time.Minute {
		t.Errorf("turnTimeout = %v", cfg.Call.TurnTimeout)
	}

	if cfg.Lines[0].Voice != "marin" {
		t.Errorf("a line without its own voice uses speech.voice, got %q", cfg.Lines[0].Voice)
	}

	if !cfg.Owner.IsOwner("**610") || cfg.Owner.IsOwner("610") {
		t.Errorf("owner numbers must match exactly")
	}
}

func TestParseLiveModeNeedsNoPipelineModels(t *testing.T) {
	t.Parallel()

	data := strings.NewReplacer(
		`"mode": "pipeline"`, `"mode": "live"`,
		`"transcriptionModel": "gpt-4o-transcribe"`, `"transcriptionModel": ""`,
		`"synthesisModel": "gpt-4o-mini-tts"`, `"synthesisModel": ""`,
		`"liveModel": ""`, `"liveModel": "gpt-live-1"`,
		`"liveInstructions": ""`, `"liveInstructions": "Only acknowledge."`,
	).Replace(validConfig)

	cfg, err := config.Parse(context.Background(), []byte(data), environment(nil))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if cfg.Speech.Mode != config.ModeLive || cfg.Speech.LiveModel != "gpt-live-1" {
		t.Fatalf("speech = %+v", cfg.Speech)
	}
}

func TestParseRealtimeMode(t *testing.T) {
	t.Parallel()

	data := strings.NewReplacer(
		`"mode": "pipeline"`, `"mode": "realtime"`,
		`"synthesisModel": "gpt-4o-mini-tts"`, `"synthesisModel": ""`,
		`"realtimeModel": ""`, `"realtimeModel": "gpt-realtime-2.1-mini"`,
	).Replace(validConfig)

	cfg, err := config.Parse(context.Background(), []byte(data), environment(nil))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if cfg.Speech.RealtimeModel != "gpt-realtime-2.1-mini" {
		t.Fatalf("speech = %+v", cfg.Speech)
	}

	// fable is a text-to-speech voice the Realtime API does not offer.
	_, err = config.Parse(context.Background(), []byte(strings.Replace(data, `"voice": "marin"`, `"voice": "fable"`, 1)),
		environment(nil))
	if err == nil || !strings.Contains(err.Error(), `"fable" is not a realtime voice`) {
		t.Fatalf("error = %v, want fable rejected in realtime mode", err)
	}
}

func TestLoadResolvesTheRecordingNextToTheConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "voice.json")
	data := strings.Replace(validConfig, `"strangerRecording": ""`, `"strangerRecording": "sounds/fun.wav"`, 1)

	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := config.Load(context.Background(), path, environment(nil))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if want := filepath.Join(dir, "sounds", "fun.wav"); cfg.Call.StrangerRecording != want {
		t.Errorf("strangerRecording = %q, want %q", cfg.Call.StrangerRecording, want)
	}
}

func TestParseRejects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		edit    func(string) string
		env     map[string]string
		message string
	}{
		{
			name:    "unknown field",
			edit:    func(s string) string { return strings.Replace(s, `"owner": {`, `"owner": {"pin": "1", `, 1) },
			message: `unknown field "pin"`,
		},
		{
			name:    "missing secret",
			env:     map[string]string{"OPENAI_API_KEY": ""},
			message: "OPENAI_API_KEY is not set",
		},
		{
			name:    "short control token",
			env:     map[string]string{"CRUMBLE_VOICE_TOKEN": "short"},
			message: "at least 32 characters",
		},
		{
			name:    "plain http to a public host",
			edit:    func(s string) string { return strings.Replace(s, "http://127.0.0.1:3000", "http://example.com", 1) },
			message: "plain http is only allowed",
		},
		{
			name:    "public control address",
			edit:    func(s string) string { return strings.Replace(s, "127.0.0.1:8790", "8.8.8.8:8790", 1) },
			message: "control.listen",
		},
		{
			name: "two lines for one bot",
			edit: func(s string) string {
				return strings.Replace(s, `"lines": [`,
					`"lines": [{"username": "bea", "passwordEnv": "SIP_PASSWORD_ADA", "displayName": "", "botId": "bot_ada"}, `, 1)
			},
			message: "already answers another line",
		},
		{
			name:    "unknown speech mode",
			edit:    func(s string) string { return strings.Replace(s, `"mode": "pipeline"`, `"mode": "fast"`, 1) },
			message: "speech.mode",
		},
		{
			name:    "live mode without instructions",
			edit:    func(s string) string { return strings.Replace(s, `"mode": "pipeline"`, `"mode": "live"`, 1) },
			message: "speech.liveModel: required",
		},
		{
			name:    "unknown voice",
			edit:    func(s string) string { return strings.Replace(s, `"voice": "marin"`, `"voice": "maryn"`, 1) },
			message: `speech.voice: "maryn" is not a pipeline voice`,
		},
		{
			name: "live-only voice in pipeline mode",
			edit: func(s string) string {
				return strings.Replace(s, `"botId": "bot_ada"`, `"botId": "bot_ada", "voice": "quartz"`, 1)
			},
			message: `lines[0].voice: "quartz" is not a pipeline voice`,
		},
		{
			name:    "unknown placeholder",
			edit:    func(s string) string { return strings.Replace(s, "Hi, this is {name}.", "Hi, {bot} here.", 1) },
			message: "the only placeholder is {name}",
		},
		{
			name: "recording that is not a WAV file",
			edit: func(s string) string {
				return strings.Replace(s, `"strangerRecording": ""`, `"strangerRecording": "fun.mp3"`, 1)
			},
			message: "call.strangerRecording: must be the path of a .wav file",
		},
		{
			name:    "trailing data",
			edit:    func(s string) string { return s + "{}" },
			message: "trailing data",
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			data := validConfig
			if test.edit != nil {
				data = test.edit(data)
			}

			_, err := config.Parse(context.Background(), []byte(data), environment(test.env))
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("error = %v, want it to mention %q", err, test.message)
			}
		})
	}
}
