// Command crumble-voice connects phone calls on a SIP registrar to Crumble bot chats.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	voiceaudio "github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/call"
	"github.com/crumble-project/crumble-voice/internal/config"
	"github.com/crumble-project/crumble-voice/internal/control"
	"github.com/crumble-project/crumble-voice/internal/crumble"
	"github.com/crumble-project/crumble-voice/internal/openai"
	"github.com/crumble-project/crumble-voice/internal/sipline"
)

const shutdownTimeout = 10 * time.Second

var errBotDisabled = errors.New("bot is disabled")

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

	bot := crumble.NewClient(cfg.Crumble.BaseURL, cfg.Crumble.Email, cfg.Crumble.Password, crumble.DefaultHTTPClient())

	err = bot.SignIn(ctx)
	if err != nil {
		return fmt.Errorf("sign in to Crumble: %w", err)
	}

	names, err := checkBots(ctx, bot, cfg.Lines)
	if err != nil {
		return err
	}

	var recording []byte
	if cfg.Call.StrangerRecording != "" {
		recording, err = voiceaudio.LoadRecording(cfg.Call.StrangerRecording)
		if err != nil {
			return fmt.Errorf("call.strangerRecording: %w", err)
		}
	}

	services := call.Services{Speech: nil, Bot: bot, Log: log}

	gateway, err := sipline.New(cfg, services, sipline.Options{
		SpeechFor:         func(voice string) call.Speech { return newSpeech(cfg.Speech, voice, log) },
		BotNames:          names,
		StrangerRecording: recording,
	})
	if err != nil {
		return fmt.Errorf("set up SIP: %w", err)
	}

	server := control.NewServer(cfg.Control.Listen.String(), control.Handler(gateway, cfg.Control.Token, log), cfg.Call.UnansweredAfter)

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Control.Listen.String())
	if err != nil {
		return fmt.Errorf("listen for the control API: %w", err)
	}

	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error { return gateway.Run(ctx) })

	group.Go(func() error {
		log.Info("control API listening", "address", listener.Addr().String())

		if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve control API: %w", err)
		}

		return nil
	})

	group.Go(func() error {
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("stop control API: %w", err)
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
			Voice: voice, Style: cfg.VoiceInstructions,
		}, log)

		// It never speaks by itself, so the holding phrase fills long waits.
		return call.Live{Model: model, Acknowledging: false}
	}

	speechHTTP := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	return call.Pipeline{
		Transcriber: openai.NewTranscriber(cfg.BaseURL, cfg.APIKey, cfg.TranscriptionModel, cfg.Language, log),
		Synthesizer: openai.NewSynthesizer(cfg.BaseURL, cfg.APIKey, cfg.SynthesisModel, voice, cfg.VoiceInstructions, speechHTTP),
	}
}

// liveInstructions adds the configured language, which GPT-Live has no separate field for.
func liveInstructions(cfg config.Speech) string {
	if cfg.Language == "" {
		return cfg.LiveInstructions
	}

	return cfg.LiveInstructions + "\n\nSpeak in the language with ISO-639-1 code " + cfg.Language + "."
}

// checkBots fails at startup when a line names a missing or disabled bot, and
// returns the bots' names by ID.
func checkBots(ctx context.Context, client *crumble.Client, lines []config.Line) (map[string]string, error) {
	names := map[string]string{}

	for _, line := range lines {
		agent, err := client.Agent(ctx, line.BotID)
		if err != nil {
			return nil, fmt.Errorf("line %s: %w", line.Username, err)
		}

		if !agent.Enabled {
			return nil, fmt.Errorf("line %s: bot %s (%s): %w", line.Username, agent.Name, agent.ID, errBotDisabled)
		}

		names[agent.ID] = agent.Name
	}

	return names, nil
}
