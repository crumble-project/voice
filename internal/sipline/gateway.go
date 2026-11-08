// Package sipline registers the bots' lines with the SIP registrar and connects calls to bot chats.
package sipline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/emiago/diago"
	"github.com/emiago/diago/audio"
	"github.com/emiago/diago/media"
	"github.com/emiago/sipgo"
	"github.com/emiago/sipgo/sip"
	"golang.org/x/sync/errgroup"

	voiceaudio "github.com/crumble-project/crumble-voice/internal/audio"
	"github.com/crumble-project/crumble-voice/internal/call"
	"github.com/crumble-project/crumble-voice/internal/config"
)

const (
	hangupTimeout = 5 * time.Second
	registerRetry = 30 * time.Second
	// Tag length sipgo uses for the From headers it builds itself.
	registerTagLength = 16
)

var (
	// ErrUnknownBot means no line is configured for the bot.
	ErrUnknownBot = errors.New("no line is configured for this bot")
	// ErrNotOwner means the number does not belong to the owner.
	ErrNotOwner = errors.New("calls only go to the owner's numbers")
	// ErrBusy means the bot is already on a call or the gateway is at its call limit.
	ErrBusy = errors.New("the bot is already on a call or the gateway is at its call limit")
	// ErrUnanswered means nobody picked up in time.
	ErrUnanswered = errors.New("the call was not answered")
	// ErrNotRunning means Dial was called before Run or after it returned.
	ErrNotRunning = errors.New("the SIP gateway is not running")
)

// Gateway answers and places calls for the configured lines.
type Gateway struct {
	config   *config.Config
	services call.Services
	speech   map[string]call.Speech
	names    map[string]string
	stranger []byte
	log      *slog.Logger
	diago    *diago.Diago
	ua       *sipgo.UserAgent
	byUser   map[string]config.Line
	byBot    map[string]config.Line

	mu        sync.Mutex
	strangers int
	active    map[string]bool
	calls     sync.WaitGroup
	ctx       context.Context //nolint:containedctx // Outbound calls outlive the control request that placed them.
}

// Options are what the gateway needs beyond the configuration.
type Options struct {
	// SpeechFor builds the speech for a line's voice.
	SpeechFor func(voice string) call.Speech
	// BotNames maps bot IDs to the names the answer greeting uses.
	BotNames map[string]string
	// StrangerRecording is telephone PCM played to callers who are not the owner;
	// nil rejects them.
	StrangerRecording []byte
}

// New builds the SIP stack. Nothing is sent until Run. services.Speech is
// replaced per line by options.SpeechFor.
func New(cfg *config.Config, services call.Services, options Options) (*Gateway, error) {
	ua, err := sipgo.NewUA(sipgo.WithUserAgent("crumble-voice"))
	if err != nil {
		return nil, fmt.Errorf("create SIP user agent: %w", err)
	}

	// The media package keeps its port range as package state.
	media.RTPPortStart = cfg.SIP.RTPPortStart
	media.RTPPortEnd = cfg.SIP.RTPPortEnd

	transport := diago.Transport{
		Transport:    cfg.SIP.Transport,
		BindHost:     cfg.SIP.BindHost,
		BindPort:     cfg.SIP.BindPort,
		ExternalHost: cfg.SIP.ExternalHost,
	}

	gateway := &Gateway{
		config:   cfg,
		services: services,
		speech:   map[string]call.Speech{},
		names:    options.BotNames,
		stranger: options.StrangerRecording,
		log:      services.Log,
		ua:       ua,
		byUser:   map[string]config.Line{},
		byBot:    map[string]config.Line{},
		active:   map[string]bool{},
	}

	gateway.diago = diago.NewDiago(ua,
		diago.WithTransport(transport),
		diago.WithLogger(services.Log.With("component", "sip")),
		diago.WithMediaConfig(diago.MediaConfig{Codecs: []media.Codec{media.CodecAudioAlaw, media.CodecAudioUlaw}}),
	)

	for _, line := range cfg.Lines {
		gateway.byUser[line.Username] = line
		gateway.byBot[line.BotID] = line
		gateway.speech[line.Username] = options.SpeechFor(line.Voice)
	}

	return gateway, nil
}

// Run serves incoming calls and keeps every line registered until ctx ends.
func (g *Gateway) Run(ctx context.Context) error {
	defer func() { _ = g.ua.Close() }()

	g.mu.Lock()
	g.ctx = ctx
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		g.ctx = nil
		g.mu.Unlock()
	}()

	group, ctx := errgroup.WithContext(ctx)

	ready := make(chan struct{})

	group.Go(func() error {
		err := g.diago.ServeBackground(ctx, g.answer)
		close(ready)

		if err != nil {
			return fmt.Errorf("serve SIP: %w", err)
		}

		<-ctx.Done()

		return nil
	})

	for _, line := range g.config.Lines {
		group.Go(func() error {
			<-ready

			return g.register(ctx, line)
		})
	}

	err := group.Wait()
	g.calls.Wait()

	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("run SIP lines: %w", err)
	}

	return nil
}

// Dial calls one of the owner's numbers from a bot's line and returns once the call
// is answered; the conversation then runs in the background.
//
//nolint:contextcheck // The conversation runs under Run's context, not the request that placed the call.
func (g *Gateway) Dial(ctx context.Context, botID, number, greeting string) (string, error) {
	line, ok := g.byBot[botID]
	if !ok {
		return "", ErrUnknownBot
	}

	if !g.config.Owner.IsOwner(number) {
		return "", ErrNotOwner
	}

	g.mu.Lock()
	parent := g.ctx
	g.mu.Unlock()

	if parent == nil {
		return "", ErrNotRunning
	}

	if !g.claim(botID) {
		return "", ErrBusy
	}

	recipient := sip.Uri{User: number, Host: g.config.SIP.RegistrarHost, Port: g.config.SIP.RegistrarPort}
	from := &sip.FromHeader{
		DisplayName: line.DisplayName,
		Address:     sip.Uri{User: line.Username, Host: g.config.SIP.RegistrarHost},
		Params:      sip.NewParams(),
	}

	ringCtx, cancel := context.WithTimeoutCause(ctx, g.config.Call.UnansweredAfter, ErrUnanswered)
	defer cancel()

	dialog, session, err := g.diago.Invite(ringCtx, recipient, diago.InviteOptions{
		Username: line.Username,
		Password: line.Password,
		Headers:  []sip.Header{from},
		RTPNAT:   media.RTPNATSymetric,
	})
	if err != nil {
		g.release(botID)

		if errors.Is(context.Cause(ringCtx), ErrUnanswered) {
			return "", ErrUnanswered
		}

		return "", fmt.Errorf("place call: %w", err)
	}

	log := g.log.With("direction", "outbound", "call", dialog.ID, "to", number, "bot", botID)
	log.Info("call answered")

	go func() {
		defer g.release(botID)
		defer func() { _ = dialog.Close() }()

		callCtx, stop := context.WithCancel(parent)
		defer stop()

		go func() {
			select {
			case <-dialog.Context().Done():
				stop()
			case <-callCtx.Done():
			}
		}()

		g.converse(callCtx, session, line, greeting, log)
		hangup(callCtx, dialog.Hangup, log)
	}()

	return dialog.ID, nil
}

// playToStranger answers a caller who is not the owner, plays the stranger
// recording, and hangs up. The caller never reaches a bot.
func (g *Gateway) playToStranger(dialog *diago.DialogServerSession, reject func(int, string, string), log *slog.Logger) {
	if !g.claimStranger() {
		reject(sip.StatusBusyHere, "Busy Here", "rejected a stranger: too many recordings playing")

		return
	}

	defer g.releaseStranger()

	session, err := dialog.Answer(diago.AnswerOptions{RTPNAT: media.RTPNATSymetric})
	if err != nil {
		log.Error("answer stranger", "error", err)

		return
	}

	props := diago.MediaProps{}

	writer, err := session.AudioWriter(diago.WithAudioWriterMediaProps(&props))
	if err != nil {
		log.Error("open stranger audio", "error", err)

		return
	}

	encoder, err := audio.NewPCMEncoderWriter(props.Codec.PayloadType, writer)
	if err != nil {
		log.Error("encode stranger audio", "codec", props.Codec.Name, "error", err)

		return
	}

	log.Info("playing the stranger recording")

	ctx := dialog.Context()

	for offset := 0; offset < len(g.stranger) && ctx.Err() == nil; offset += voiceaudio.FrameBytes {
		frame := make([]byte, voiceaudio.FrameBytes)
		copy(frame, g.stranger[offset:])

		if _, err := encoder.Write(frame); err != nil {
			break
		}
	}

	hangup(ctx, dialog.Hangup, log)
}

func (g *Gateway) claimStranger() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Strangers get their own limit, so they cannot keep the owner from reaching a bot.
	if g.strangers >= g.config.Call.MaxConcurrent {
		return false
	}

	g.strangers++
	g.calls.Add(1)

	return true
}

func (g *Gateway) releaseStranger() {
	g.mu.Lock()
	g.strangers--
	g.mu.Unlock()

	g.calls.Done()
}

func (g *Gateway) register(ctx context.Context, line config.Line) error {
	recipient := sip.Uri{User: line.Username, Host: g.config.SIP.RegistrarHost, Port: g.config.SIP.RegistrarPort}
	if g.config.SIP.Transport != "udp" {
		recipient.UriParams = sip.NewParams()
		recipient.UriParams.Add("transport", g.config.SIP.Transport)
	}

	log := g.log.With("line", line.Username)

	for {
		err := g.registerOnce(ctx, recipient, line, log)

		if ctx.Err() != nil {
			return fmt.Errorf("register line %s: %w", line.Username, ctx.Err())
		}

		// Wrong credentials never fix themselves, so they stop the gateway.
		var refused *diago.RegisterResponseError
		if errors.As(err, &refused) && (refused.RegisterRes.StatusCode == sip.StatusUnauthorized ||
			refused.RegisterRes.StatusCode == sip.StatusProxyAuthRequired ||
			refused.RegisterRes.StatusCode == sip.StatusForbidden) {
			return fmt.Errorf("registrar refused line %s: %w", line.Username, err)
		}

		log.Error("registration lost, retrying", "error", err, "in", registerRetry.String())

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(registerRetry):
		}
	}
}

// registerOnce registers the line and keeps it registered until it fails or ctx ends.
// From and Contact carry the line's username: a FRITZ!Box silently drops a
// REGISTER whose From is not the account being registered.
func (g *Gateway) registerOnce(ctx context.Context, recipient sip.Uri, line config.Line, log *slog.Logger) error {
	transaction, err := g.diago.RegisterTransaction(ctx, recipient, diago.RegisterOptions{
		Username: line.Username,
		Password: line.Password,
		Expiry:   g.config.SIP.RegisterExpiry,
	})
	if err != nil {
		return fmt.Errorf("prepare registration: %w", err)
	}

	from := &sip.FromHeader{
		DisplayName: line.DisplayName,
		Address:     sip.Uri{User: line.Username, Host: g.config.SIP.RegistrarHost},
		Params:      sip.NewParams(),
	}
	from.Params.Add("tag", sip.GenerateTagN(registerTagLength))
	transaction.Origin.AppendHeader(from)

	if contact := transaction.Origin.Contact(); contact != nil {
		contact.Address.User = line.Username
	}

	err = transaction.Register(ctx)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	log.Info("line registered")

	defer func() {
		unregisterCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hangupTimeout)
		defer cancel()

		_ = transaction.Unregister(unregisterCtx)
	}()

	err = transaction.QualifyLoop(ctx)
	if err != nil {
		return fmt.Errorf("keep registration: %w", err)
	}

	return nil
}

// fromRegistrar checks the INVITE's network source, since anyone on the
// network can send an INVITE with the owner's caller ID.
func (g *Gateway) fromRegistrar(source string) bool {
	host, _, err := net.SplitHostPort(source)
	if err != nil {
		return false
	}

	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}

	for _, allowed := range g.config.SIP.RegistrarIPs {
		if addr.Unmap() == allowed {
			return true
		}
	}

	return false
}

func (g *Gateway) claim(botID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.active[botID] || len(g.active) >= g.config.Call.MaxConcurrent {
		return false
	}

	g.active[botID] = true

	g.calls.Add(1)

	return true
}

func (g *Gateway) release(botID string) {
	g.mu.Lock()
	delete(g.active, botID)
	g.mu.Unlock()

	g.calls.Done()
}

func (g *Gateway) answer(dialog *diago.DialogServerSession) {
	source := dialog.InviteRequest.Source()
	// The Request-URI is the Contact the line registered, so it names the line even
	// when To carries the dialed internal number.
	lineUser := dialog.InviteRequest.Recipient.User
	log := g.log.With("direction", "inbound", "call", dialog.ID, "from", dialog.FromUser(), "to", dialog.ToUser(), "line", lineUser)

	reject := func(status int, reason, message string) {
		log.Warn(message, "source", source)

		if err := dialog.Respond(status, reason, nil); err != nil {
			log.Error("reject call", "error", err)
		}
	}

	if !g.fromRegistrar(source) {
		reject(sip.StatusForbidden, "Forbidden", "rejected call that did not come from the registrar")

		return
	}

	line, ok := g.byUser[lineUser]
	if !ok {
		reject(sip.StatusNotFound, "Not Found", "rejected call to an unknown line")

		return
	}

	if !g.config.Owner.IsOwner(dialog.FromUser()) {
		if g.stranger == nil {
			reject(sip.StatusForbidden, "Forbidden", "rejected call from a number that is not the owner's")

			return
		}

		g.playToStranger(dialog, reject, log)

		return
	}

	if !g.claim(line.BotID) {
		reject(sip.StatusBusyHere, "Busy Here", "rejected call because the bot is busy")

		return
	}

	defer g.release(line.BotID)

	if err := dialog.Trying(); err != nil {
		log.Error("send trying", "error", err)

		return
	}

	session, err := dialog.Answer(diago.AnswerOptions{RTPNAT: media.RTPNATSymetric})
	if err != nil {
		log.Error("answer call", "error", err)

		return
	}

	log.Info("call answered", "bot", line.BotID)
	greeting := strings.ReplaceAll(g.config.Call.AnswerGreeting, config.NamePlaceholder, g.names[line.BotID])
	g.converse(dialog.Context(), session, line, greeting, log)
	hangup(dialog.Context(), dialog.Hangup, log)
}

func (g *Gateway) converse(ctx context.Context, session *diago.DialogMedia, line config.Line, greeting string, log *slog.Logger) {
	props := diago.MediaProps{}

	reader, err := session.AudioReader(diago.WithAudioReaderMediaProps(&props))
	if err != nil {
		log.Error("open call audio", "error", err)

		return
	}

	writer, err := session.AudioWriter()
	if err != nil {
		log.Error("open call audio", "error", err)

		return
	}

	decoder, err := audio.NewPCMDecoderReader(props.Codec.PayloadType, reader)
	if err != nil {
		log.Error("decode call audio", "codec", props.Codec.Name, "error", err)

		return
	}

	encoder, err := audio.NewPCMEncoderWriter(props.Codec.PayloadType, writer)
	if err != nil {
		log.Error("encode call audio", "codec", props.Codec.Name, "error", err)

		return
	}

	services := g.services
	services.Log = log
	services.Speech = g.speech[line.Username]
	options := call.Options{
		BotID:         line.BotID,
		Greeting:      greeting,
		CallNote:      g.config.Call.CallNote,
		HoldingPhrase: g.config.Call.HoldingPhrase,
		HoldingDelay:  g.config.Call.HoldingDelay,
		TimeoutPhrase: g.config.Call.TimeoutPhrase,
		FailurePhrase: g.config.Call.FailurePhrase,
		TurnTimeout:   g.config.Call.TurnTimeout,
		MaxDuration:   g.config.Call.MaxDuration,
	}

	err = call.Run(ctx, call.Media{In: decoder, Out: encoder}, services, options)

	switch {
	case ctx.Err() != nil, err == nil, errors.Is(err, io.EOF):
		log.Info("call ended")
	case errors.Is(err, call.ErrMaxDuration):
		log.Info("call reached its maximum duration")
	default:
		log.Error("call failed", "error", err)
	}
}

// hangup outlives the call's context, which is often what ended.
func hangup(ctx context.Context, do func(context.Context) error, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hangupTimeout)
	defer cancel()

	if err := do(ctx); err != nil && !errors.Is(err, sip.ErrTransactionTerminated) {
		log.Debug("hang up", "error", err)
	}
}
