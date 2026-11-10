// Command crumble-voice connects phone calls on a SIP registrar to Crumble bot chats.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	voiceaudio "github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/bridge"
	"github.com/crumble-project/crumble-voice/internal/call"
	"github.com/crumble-project/crumble-voice/internal/config"
	"github.com/crumble-project/crumble-voice/internal/crumble"
	"github.com/crumble-project/crumble-voice/internal/openai"
	"github.com/crumble-project/crumble-voice/internal/sipline"
)

func main() {
	configPath := flag.String("config", "crumble-voice.json", "path to the configuration file")
	verbose := flag.Bool("verbose", false, "log debug messages")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *configPath, log); err != nil {
		switch {
		case errors.Is(err, crumble.ErrUnauthorized):
			log.Error("the server refused the voice gateway token, which was revoked or replaced: " +
				"issue a new voice gateway token in Settings > Voice gateway and restart crumble-voice")
		case errors.Is(err, crumble.ErrReplaced):
			log.Error("another voice gateway connected with this token and took over: " +
				"run one gateway per token, or issue a new voice gateway token for this one")
		}

		log.Error("crumble-voice stopped", "error", err)
		stop()
		os.Exit(1) //nolint:gocritic // stop already ran; nothing else is deferred.
	}
}

func run(ctx context.Context, configPath string, log *slog.Logger) error {
	cfg, err := config.Load(ctx, configPath, os.LookupEnv)
	if err != nil {
		return fmt.Errorf("load %s: %w", configPath, err)
	}

	bot := crumble.NewClient(cfg.Crumble.BaseURL, cfg.Crumble.Token, crumble.DefaultHTTPClient())

	var recording []byte
	if cfg.Call.StrangerRecording != "" {
		recording, err = voiceaudio.LoadRecording(cfg.Call.StrangerRecording)
		if err != nil {
			return fmt.Errorf("call.strangerRecording: %w", err)
		}
	}

	services := call.Services{Speech: nil, Bot: bot, Log: log}
	calls := bridge.NewCalls(bot, log)

	gateway, err := sipline.New(cfg, services, sipline.Options{
		SpeechFor:         func(voice string) call.Speech { return newSpeech(cfg.Speech, voice, log) },
		StrangerRecording: recording,
		Calls:             calls,
	})
	if err != nil {
		return fmt.Errorf("set up SIP: %w", err)
	}

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error { return gateway.Run(ctx) })

	group.Go(func() error {
		if err := bridge.Run(ctx, gateway, bot, calls, log); err != nil {
			return fmt.Errorf("voice gateway events: %w", err)
		}

		return nil
	})

	if err := group.Wait(); err != nil {
		return fmt.Errorf("run gateway: %w", err)
	}

	return nil
}

// newSpeech builds the configured speech mode.
//
//nolint:ireturn // The modes are interchangeable implementations of call.Speech.
func newSpeech(cfg config.Speech, voice string, log *slog.Logger) call.Speech {
	switch cfg.Mode {
	case config.ModeLive:
		model := openai.NewLive(cfg.BaseURL, cfg.APIKey, cfg.LiveModel, voice, liveInstructions(cfg), log)

		return call.Live{Model: model, Acknowledging: true}
	case config.ModeRealtime:
		model := openai.NewRealtime(cfg.BaseURL, cfg.APIKey, openai.RealtimeOptions{
			Model: cfg.RealtimeModel, TranscriptionModel: cfg.TranscriptionModel, Language: cfg.Language,
			Voice: voice, Style: cfg.VoiceInstructions, TurnDetection: turnDetection(cfg),
		}, log)

		// It never speaks by itself, so the holding phrase fills long waits.
		return call.Live{Model: model, Acknowledging: false}
	}

	speechHTTP := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	return call.Pipeline{
		Transcriber: openai.NewTranscriber(cfg.BaseURL, cfg.APIKey, cfg.TranscriptionModel, cfg.Language, turnDetection(cfg), log),
		Synthesizer: openai.NewSynthesizer(cfg.BaseURL, cfg.APIKey, cfg.SynthesisModel, voice, cfg.VoiceInstructions, speechHTTP),
	}
}

func turnDetection(cfg config.Speech) openai.TurnDetection {
	return openai.TurnDetection{
		Semantic: cfg.TurnDetection == config.TurnSemantic, Eagerness: cfg.Eagerness, Silence: cfg.SilenceDuration,
	}
}

// liveInstructions adds the configured language, which GPT-Live has no separate field for.
func liveInstructions(cfg config.Speech) string {
	if cfg.Language == "" {
		return cfg.LiveInstructions
	}

	return cfg.LiveInstructions + "\n\nSpeak in the language with ISO-639-1 code " + cfg.Language + "."
}
