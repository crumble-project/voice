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
  "crumble": {"baseUrl": "http://127.0.0.1:3000", "tokenEnv": "CRUMBLE_VOICE_GATEWAY_TOKEN"},
  "sip": {
    "registrar": "192.168.1.1:5060", "transport": "udp", "bindHost": "0.0.0.0", "bindPort": 5070,
    "externalHost": "", "rtpPortStart": 20000, "rtpPortEnd": 20100, "registerExpiry": "5m",
    "accounts": [{"username": "ada", "passwordEnv": "SIP_PASSWORD_ADA"}]
  },
  "speech": {
    "mode": "pipeline", "provider": "openai", "apiKeyEnv": "OPENAI_API_KEY", "baseUrl": "https://api.openai.com",
    "transcriptionModel": "gpt-4o-transcribe", "language": "de",
    "synthesisModel": "gpt-4o-mini-tts", "voice": "marin", "voiceInstructions": "",
    "liveModel": "", "liveInstructions": "", "realtimeModel": ""
  },
  "call": {
    "maxDuration": "30m", "turnTimeout": "2m", "maxConcurrent": 2,
    "holdingPhrase": "One moment.", "holdingDelay": "3s",
    "timeoutPhrase": "That takes longer; the answer will be in the chat.", "failurePhrase": "Something went wrong.",
    "unansweredAfter": "30s", "answerGreeting": "Hi, this is {name}.",
    "strangerRecording": ""
  }
}`

const voiceToken = "acct_1.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func environment(overrides map[string]string) config.Environment {
	values := map[string]string{
		"CRUMBLE_VOICE_GATEWAY_TOKEN": voiceToken,
		"SIP_PASSWORD_ADA":            "line-secret",
		"OPENAI_API_KEY":              "sk-test",
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

	if cfg.SIP.Accounts[0].Password != "line-secret" {
		t.Errorf("SIP account password not read from the environment")
	}

	if cfg.Crumble.Token != voiceToken {
		t.Errorf("voice gateway token not read from the environment")
	}

	if cfg.Call.TurnTimeout != 2*time.Minute {
		t.Errorf("turnTimeout = %v", cfg.Call.TurnTimeout)
	}

	if cfg.SIP.Accounts[0].Voice != "marin" {
		t.Errorf("an account without its own voice uses speech.voice, got %q", cfg.SIP.Accounts[0].Voice)
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

func TestParseTurnDetection(t *testing.T) {
	t.Parallel()

	// A file written before turn detection was configurable gets semantic turn detection.
	cfg, err := config.Parse(context.Background(), []byte(validConfig), environment(nil))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if s := cfg.Speech; s.TurnDetection != config.TurnSemantic || s.Eagerness != "medium" || s.SilenceDuration != 600*time.Millisecond {
		t.Errorf("defaults = %q, %q, %v", s.TurnDetection, s.Eagerness, s.SilenceDuration)
	}

	data := strings.Replace(validConfig, `"realtimeModel": ""`,
		`"realtimeModel": "", "turnDetection": "silence", "eagerness": "low", "silenceDuration": "1.2s"`, 1)

	cfg, err = config.Parse(context.Background(), []byte(data), environment(nil))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if s := cfg.Speech; s.TurnDetection != config.TurnSilence || s.Eagerness != "low" || s.SilenceDuration != 1200*time.Millisecond {
		t.Errorf("speech = %q, %q, %v", s.TurnDetection, s.Eagerness, s.SilenceDuration)
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
			edit:    func(s string) string { return strings.Replace(s, `"crumble": {`, `"crumble": {"pin": "1", `, 1) },
			message: `unknown field "pin"`,
		},
		{
			name: "an old config with lines",
			edit: func(s string) string {
				return strings.Replace(s, `"speech": {`, `"lines": [], "speech": {`, 1)
			},
			message: `unknown field "lines"`,
		},
		{
			name:    "a password instead of a voice gateway token",
			env:     map[string]string{"CRUMBLE_VOICE_GATEWAY_TOKEN": "correct horse battery staple"},
			message: "does not hold a voice gateway token",
		},
		{
			name:    "no voice gateway token",
			env:     map[string]string{"CRUMBLE_VOICE_GATEWAY_TOKEN": ""},
			message: "CRUMBLE_VOICE_GATEWAY_TOKEN is not set",
		},
		{
			name: "no SIP accounts",
			edit: func(s string) string {
				return strings.Replace(s, `{"username": "ada", "passwordEnv": "SIP_PASSWORD_ADA"}`, "", 1)
			},
			message: "configure at least one SIP account",
		},
		{
			name:    "missing secret",
			env:     map[string]string{"OPENAI_API_KEY": ""},
			message: "OPENAI_API_KEY is not set",
		},
		{
			name:    "plain http to a public host",
			edit:    func(s string) string { return strings.Replace(s, "http://127.0.0.1:3000", "http://example.com", 1) },
			message: "plain http is only allowed",
		},
		{
			name: "an old config with the control API",
			edit: func(s string) string {
				return strings.Replace(s, `"call": {`, `"control": {"listen": "127.0.0.1:8790", "tokenEnv": "X"}, "call": {`, 1)
			},
			message: `unknown field "control"`,
		},
		{
			name: "one SIP account twice",
			edit: func(s string) string {
				return strings.Replace(s, `"accounts": [`, `"accounts": [{"username": "ada", "passwordEnv": "SIP_PASSWORD_ADA"}, `, 1)
			},
			message: `"ada" is listed twice`,
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
				return strings.Replace(s, `"passwordEnv": "SIP_PASSWORD_ADA"`, `"passwordEnv": "SIP_PASSWORD_ADA", "voice": "quartz"`, 1)
			},
			message: `sip.accounts[0].voice: "quartz" is not a pipeline voice`,
		},
		{
			name: "unknown turn detection",
			edit: func(s string) string {
				return strings.Replace(s, `"realtimeModel": ""`, `"realtimeModel": "", "turnDetection": "vad"`, 1)
			},
			message: `speech.turnDetection: must be "semantic" or "silence"`,
		},
		{
			name: "unknown eagerness",
			edit: func(s string) string {
				return strings.Replace(s, `"realtimeModel": ""`, `"realtimeModel": "", "eagerness": "lazy"`, 1)
			},
			message: "speech.eagerness: must be low, medium, high, or auto",
		},
		{
			name: "silence too short",
			edit: func(s string) string {
				return strings.Replace(s, `"realtimeModel": ""`, `"realtimeModel": "", "silenceDuration": "100ms"`, 1)
			},
			message: "speech.silenceDuration: must be whole milliseconds between 200ms and 5s",
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
