// Package sipline registers the lines the server serves with the SIP registrar and connects calls to bot chats.
package sipline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"slices"
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
	"github.com/crumble-project/crumble-voice/internal/turn"
)

const (
	hangupTimeout = 5 * time.Second
	// A BYE or CANCEL to a registrar on the local network is answered in milliseconds.
	callsEndTimeout = 2 * time.Second
	registerRetry   = 30 * time.Second
	refusedRetry    = 10 * time.Minute
	// Tag length sipgo uses for the From headers it builds itself.
	registerTagLength = 16
)

var (
	// ErrUnknownLine means the gateway serves no registered line with the username.
	ErrUnknownLine = errors.New("the gateway serves no line with this username")
	// ErrNotOwner means the number does not belong to the owner.
	ErrNotOwner = errors.New("calls only go to the owner's numbers")
	// ErrBusy means the bot is already on a call or the gateway is at its call limit.
	ErrBusy = errors.New("the bot is already on a call or the gateway is at its call limit")
	// ErrUnanswered means nobody picked up in time.
	ErrUnanswered = errors.New("the call was not answered")
	// ErrOwnerBusy means the owner's phone answered busy.
	ErrOwnerBusy = errors.New("the owner's phone is busy")
	// ErrNotRunning means Dial was called before Run or after it returned.
	ErrNotRunning = errors.New("the SIP gateway is not running")
)

// Gateway answers and places calls for the lines the server serves.
type Gateway struct {
	config   *config.Config
	services call.Services
	state    Calls
	speech   map[string]call.Speech
	accounts map[string]config.Account
	stranger []byte
	log      *slog.Logger
	diago    *diago.Diago
	ua       *sipgo.UserAgent
	changed  chan struct{}
	reported chan struct{}

	mu        sync.Mutex
	owner     []string
	byUser    map[string]localLine
	states    []LineState
	pending   *Setup
	strangers int
	active    map[string]bool
	calls     sync.WaitGroup
	ctx       context.Context //nolint:containedctx // Outbound calls outlive the call event that placed them.
}

// LineState is a served line's registration; Error says why it is not registered.
type LineState struct {
	Username   string
	Registered bool
	Error      string
}

// Outbound is a call the server asked for: the line to call from, the owner number
// to dial, what the bot says when answered, and the call ID of its spoken messages.
type Outbound struct {
	CallID   string
	Username string
	Number   string
	Greeting string
}

const noAccount = "no SIP account with this username in sip.accounts"

// Setup is what the server says the gateway serves.
type Setup struct {
	OwnerNumbers []string
	Lines        []Line
}

// Line is one line the server serves: a SIP username answered by one bot.
type Line struct {
	Username    string
	BotID       string
	BotName     string
	DisplayName string
}

// localLine is a served line with the password of its local SIP account.
type localLine struct {
	Line

	password string
}

type registration struct {
	line   localLine
	cancel context.CancelFunc
	done   chan struct{}
}

// Options are what the gateway needs beyond the configuration.
type Options struct {
	// SpeechFor builds the speech for a SIP account's voice.
	SpeechFor func(voice string) call.Speech
	// StrangerRecording is telephone PCM played to callers who are not the owner;
	// nil rejects them.
	StrangerRecording []byte
	// Calls hears when each call with a bot starts and ends; nil tells no one.
	Calls Calls
}

// Calls hears when each call with a bot starts and ends, inbound or outbound.
type Calls interface {
	// Started runs before the greeting. hangUp ends the call, during Started or later.
	Started(ctx context.Context, username, callID string, hangUp func())
	// Ended runs once the call hung up.
	Ended(callID string, reason EndReason)
}

// EndReason says how a call ended.
type EndReason int

const (
	// EndHungUp means the media ended or the dialog closed.
	EndHungUp EndReason = iota
	// EndMaxDuration means the call reached call.maxDuration.
	EndMaxDuration
	// EndFailed means the conversation failed.
	EndFailed
	// EndStopping means the gateway is shutting down.
	EndStopping
	// EndByServer means the server ended the call, so it knows already.
	EndByServer
)

type noCalls struct{}

func (noCalls) Started(context.Context, string, string, func()) {}
func (noCalls) Ended(string, EndReason)                         {}

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
		state:    options.Calls,
		speech:   map[string]call.Speech{},
		accounts: map[string]config.Account{},
		stranger: options.StrangerRecording,
		log:      services.Log,
		ua:       ua,
		changed:  make(chan struct{}, 1),
		reported: make(chan struct{}, 1),
		byUser:   map[string]localLine{},
		active:   map[string]bool{},
	}

	gateway.diago = diago.NewDiago(ua,
		diago.WithTransport(transport),
		diago.WithLogger(services.Log.With("component", "sip")),
		diago.WithMediaConfig(diago.MediaConfig{Codecs: []media.Codec{media.CodecAudioAlaw, media.CodecAudioUlaw}}),
	)

	if gateway.state == nil {
		gateway.state = noCalls{}
	}

	for _, account := range cfg.SIP.Accounts {
		gateway.accounts[account.Username] = account
		gateway.speech[account.Username] = options.SpeechFor(account.Voice)
	}

	return gateway, nil
}

// Apply replaces the lines and the owner's numbers. Run applies them once no call is active,
// so a call keeps its line and bot until it ends.
func (g *Gateway) Apply(setup Setup) {
	g.mu.Lock()
	g.pending = &setup
	g.mu.Unlock()

	g.notify()
	signal(g.reported)
}

// Lines returns the registration state of every line the applied setup names,
// including lines without a local SIP account.
func (g *Gateway) Lines() []LineState {
	g.mu.Lock()
	defer g.mu.Unlock()

	return slices.Clone(g.states)
}

// LinesChanged receives a value when Lines may have changed or a setup arrived.
func (g *Gateway) LinesChanged() <-chan struct{} {
	return g.reported
}

// Run serves incoming calls and keeps every served line registered until ctx ends.
func (g *Gateway) Run(ctx context.Context) error {
	// The transport outlives ctx, so unregistering and a call's CANCEL or BYE go out and get their answers.
	serveCtx, stopServing := context.WithCancel(context.WithoutCancel(ctx))
	defer g.endCalls(stopServing)

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
		err := g.diago.ServeBackground(serveCtx, g.answer)
		close(ready)

		if err != nil {
			return fmt.Errorf("serve SIP: %w", err)
		}

		<-ctx.Done()

		return nil
	})

	group.Go(func() error {
		<-ready

		return g.keepLines(ctx, group)
	})

	err := group.Wait()

	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("run SIP lines: %w", err)
	}

	return nil
}

// Dial calls one of the owner's numbers from a line and returns once the call is
// answered; the conversation then runs in the background until hangup is called or it ends.
// Ending ctx before the answer cancels the call.
//
//nolint:contextcheck // The conversation runs under Run's context, not the event that placed the call.
func (g *Gateway) Dial(ctx context.Context, request Outbound) (func(), error) {
	line, parent, err := g.claimLine(request)
	if err != nil {
		return nil, err
	}

	botID := line.BotID

	recipient := sip.Uri{User: request.Number, Host: g.config.SIP.RegistrarHost, Port: g.config.SIP.RegistrarPort}
	from := &sip.FromHeader{
		DisplayName: line.DisplayName,
		Address:     sip.Uri{User: line.Username, Host: g.config.SIP.RegistrarHost},
		Params:      sip.NewParams(),
	}

	ringCtx, cancel := context.WithTimeoutCause(ctx, g.config.Call.UnansweredAfter, ErrUnanswered)
	defer cancel()

	dialog, session, err := g.diago.Invite(ringCtx, recipient, diago.InviteOptions{
		Username: line.Username,
		Password: line.password,
		Headers:  []sip.Header{from},
		RTPNAT:   media.RTPNATSymetric,
	})
	if err != nil {
		g.release(botID)

		if errors.Is(context.Cause(ringCtx), ErrUnanswered) {
			return nil, ErrUnanswered
		}

		var refused *sipgo.ErrDialogResponse
		if errors.As(err, &refused) &&
			(refused.Res.StatusCode == sip.StatusBusyHere || refused.Res.StatusCode == sip.StatusGlobalBusyEverywhere) {
			return nil, ErrOwnerBusy
		}

		return nil, fmt.Errorf("place call: %w", err)
	}

	log := g.log.With("direction", "outbound", "call", dialog.ID, "call_id", request.CallID, "to", request.Number, "bot", botID)
	log.Info("call answered")

	callCtx, stop := context.WithCancel(parent)
	begun := make(chan struct{})

	go func() {
		defer g.release(botID)
		defer func() { _ = dialog.Close() }()
		defer stop()

		go func() {
			select {
			case <-dialog.Context().Done():
				stop()
			case <-callCtx.Done():
			}
		}()

		g.converse(callCtx, line, connected{
			session: session, hangup: dialog.Hangup, greeting: request.Greeting, callID: request.CallID,
			begun: func() { close(begun) },
		}, log)
	}()

	// The server takes the start of a call it asked for as answered, so it goes first.
	<-begun

	return stop, nil
}

// endCalls stops the transport and closes the SIP stack once every call hung up. sipgo
// waits up to 32 seconds for an unanswered CANCEL, and closing the stack under it panics
// in sipgo, so after callsEndTimeout Run returns and the stack closes when it times out.
func (g *Gateway) endCalls(stopServing context.CancelFunc) {
	ended := make(chan struct{})

	go func() {
		g.calls.Wait()
		stopServing()
		_ = g.ua.Close()

		close(ended)
	}()

	select {
	case <-ended:
	case <-time.After(callsEndTimeout):
		g.log.Warn("a call is still hanging up; the SIP stack closes once it ends")
		stopServing()
	}
}

func (g *Gateway) notify() {
	signal(g.changed)
}

func signal(events chan struct{}) {
	select {
	case events <- struct{}{}:
	default:
	}
}

// setState records a line's registration while the applied setup still serves it.
func (g *Gateway) setState(username string, registered bool, reason string) {
	g.mu.Lock()

	for i := range g.states {
		if g.states[i].Username == username {
			g.states[i].Registered, g.states[i].Error = registered, reason
		}
	}

	g.mu.Unlock()

	signal(g.reported)
}

// keepLines registers the served lines and rebuilds the registrations on every applied setup.
// diago registers each line in its own transaction, so lines come and go on the running stack.
func (g *Gateway) keepLines(ctx context.Context, group *errgroup.Group) error {
	registrations := map[string]registration{}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-g.changed:
		}

		lines, ok := g.take()
		if !ok {
			continue
		}

		for username, current := range registrations {
			if next, kept := lines[username]; kept && next == current.line {
				continue
			}

			// The old registration unregisters before a new one for the same account starts.
			current.cancel()
			<-current.done
			delete(registrations, username)
		}

		for username, next := range lines {
			if _, running := registrations[username]; running {
				continue
			}

			lineCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			registrations[username] = registration{line: next, cancel: cancel, done: done}

			group.Go(func() error {
				defer close(done)

				return g.register(lineCtx, next)
			})
		}
	}
}

// take applies the pending setup when no call is active and returns its lines by username.
func (g *Gateway) take() (map[string]localLine, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.pending == nil {
		return nil, false
	}

	if len(g.active) > 0 {
		g.log.Info("new lines wait for the active calls to end")

		return nil, false
	}

	setup := g.pending
	g.pending = nil
	lines := map[string]localLine{}
	states := make([]LineState, 0, len(setup.Lines))

	for _, served := range setup.Lines {
		account, ok := g.accounts[served.Username]
		if !ok {
			g.log.Warn("line not registered: "+noAccount, "line", served.Username)
			states = append(states, LineState{Username: served.Username, Registered: false, Error: noAccount})

			continue
		}

		next := localLine{Line: served, password: account.Password}
		lines[served.Username] = next
		state := LineState{Username: served.Username, Registered: false, Error: ""}

		// A line served as before keeps its registration, so it keeps its state.
		if g.byUser[served.Username] == next {
			if index := slices.IndexFunc(g.states, func(s LineState) bool { return s.Username == served.Username }); index >= 0 {
				state = g.states[index]
			}
		}

		states = append(states, state)
	}

	g.byUser = lines
	g.states = states
	g.owner = slices.Clone(setup.OwnerNumbers)
	g.log.Info("lines applied", "lines", len(lines), "owner_numbers", len(g.owner))
	signal(g.reported)

	return lines, true
}

// playToStranger answers a caller who is not the owner, plays the stranger
// recording, and hangs up. The caller never reaches a bot.
func (g *Gateway) playToStranger(dialog *diago.DialogServerSession, log *slog.Logger) {
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

// claimStrangerLocked needs g.mu held.
func (g *Gateway) claimStrangerLocked() bool {
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

func (g *Gateway) register(ctx context.Context, line localLine) error {
	recipient := sip.Uri{User: line.Username, Host: g.config.SIP.RegistrarHost, Port: g.config.SIP.RegistrarPort}
	if g.config.SIP.Transport != "udp" {
		recipient.UriParams = sip.NewParams()
		recipient.UriParams.Add("transport", g.config.SIP.Transport)
	}

	log := g.log.With("line", line.Username)

	for {
		err := g.registerOnce(ctx, recipient, line, log)

		// A dropped line or a stopping gateway, not a failure.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		g.setState(line.Username, false, err.Error())

		// Refused credentials stay refused until someone fixes the account, and quick
		// retries can lock it on the registrar; the other lines keep running.
		retry := registerRetry

		var refused *diago.RegisterResponseError
		if errors.As(err, &refused) && (refused.RegisterRes.StatusCode == sip.StatusUnauthorized ||
			refused.RegisterRes.StatusCode == sip.StatusProxyAuthRequired ||
			refused.RegisterRes.StatusCode == sip.StatusForbidden) {
			retry = refusedRetry
		}

		log.Error("registration failed, retrying", "error", err, "in", retry.String())

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(retry):
		}
	}
}

// registerOnce registers the line and keeps it registered until it fails or ctx ends.
// From and Contact carry the line's username: a FRITZ!Box silently drops a
// REGISTER whose From is not the account being registered.
func (g *Gateway) registerOnce(ctx context.Context, recipient sip.Uri, line localLine, log *slog.Logger) error {
	transaction, err := g.diago.RegisterTransaction(ctx, recipient, diago.RegisterOptions{
		Username: line.Username,
		Password: line.password,
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
	g.setState(line.Username, true, "")

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

// claimLine looks up the line and claims its bot under one lock, so a setup applied
// in between cannot hand the call a bot the server no longer routes the line to.
func (g *Gateway) claimLine(request Outbound) (localLine, context.Context, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	line, ok := g.byUser[request.Username]

	switch {
	case !ok:
		return line, nil, ErrUnknownLine
	case !slices.Contains(g.owner, request.Number):
		return line, nil, ErrNotOwner
	case g.ctx == nil:
		return line, nil, ErrNotRunning
	case !g.claimLocked(line.BotID):
		return line, nil, ErrBusy
	}

	return line, g.ctx, nil
}

type admission int

const (
	admitOwner admission = iota
	admitStranger
	refuseStopping
	refuseUnknownLine
	refuseStranger
	refuseStrangersBusy
	refuseBusy
)

// admit decides on an inbound call and claims its slot under one lock, so neither a setup
// applied nor a shutdown in between lets the call in on a stale line or a closing SIP stack.
// Run clears g.ctx before it waits for the calls, so a claim made here is always waited for.
func (g *Gateway) admit(lineUser, caller string) (localLine, context.Context, admission) {
	g.mu.Lock()
	defer g.mu.Unlock()

	line, ok := g.byUser[lineUser]

	switch {
	// The transport keeps serving while Run stops; calls arriving then are turned away.
	case g.ctx == nil || g.ctx.Err() != nil:
		return line, nil, refuseStopping
	case !ok:
		return line, nil, refuseUnknownLine
	case !slices.Contains(g.owner, caller):
		switch {
		case g.stranger == nil:
			return line, nil, refuseStranger
		case !g.claimStrangerLocked():
			return line, nil, refuseStrangersBusy
		}

		return line, nil, admitStranger
	case !g.claimLocked(line.BotID):
		return line, nil, refuseBusy
	}

	return line, g.ctx, admitOwner
}

// claimLocked needs g.mu held.
func (g *Gateway) claimLocked(botID string) bool {
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
	idle := len(g.active) == 0 && g.pending != nil
	g.mu.Unlock()

	g.calls.Done()

	if idle {
		g.notify()
	}
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

	line, running, admitted := g.admit(lineUser, dialog.FromUser())

	switch admitted {
	case refuseStopping:
		reject(sip.StatusServiceUnavailable, "Service Unavailable", "rejected call because the gateway is stopping")

		return
	case refuseUnknownLine:
		reject(sip.StatusNotFound, "Not Found", "rejected call to an unknown line")

		return
	case refuseStranger:
		reject(sip.StatusForbidden, "Forbidden", "rejected call from a number that is not the owner's")

		return
	case refuseStrangersBusy:
		reject(sip.StatusBusyHere, "Busy Here", "rejected a stranger: too many recordings playing")

		return
	case refuseBusy:
		reject(sip.StatusBusyHere, "Busy Here", "rejected call because the bot is busy")

		return
	case admitStranger:
		g.playToStranger(dialog, log)

		return
	case admitOwner:
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
	g.converseInbound(running, dialog, session, line, log)
}

// converseInbound ends the call when the gateway stops, as an outbound call ends, so its
// end is reported as stopping.
//
//nolint:contextcheck // The call lives on the dialog's context; running only ends it early.
func (g *Gateway) converseInbound(
	running context.Context, dialog *diago.DialogServerSession, session *diago.DialogMedia, line localLine, log *slog.Logger,
) {
	callCtx, stop := context.WithCancel(dialog.Context())
	defer stop()

	unwatch := context.AfterFunc(running, stop)
	defer unwatch()

	greeting := strings.ReplaceAll(g.config.Call.AnswerGreeting, config.NamePlaceholder, line.BotName)
	g.converse(callCtx, line, connected{
		session: session, hangup: dialog.Hangup, greeting: greeting, callID: turn.NewID(), begun: func() {},
	}, log)
}

// connected is an answered call with a bot.
type connected struct {
	session  *diago.DialogMedia
	hangup   func(context.Context) error
	greeting string
	callID   string
	// begun runs once the start is reported, or once the call ended before that.
	begun func()
}

// converse reports the call's start before the greeting, holds the conversation, hangs
// up, and then reports how the call ended.
func (g *Gateway) converse(ctx context.Context, line localLine, connection connected, log *slog.Logger) {
	ctx, end := context.WithCancelCause(ctx)
	defer end(nil)

	begun := sync.OnceFunc(connection.begun)
	defer begun()

	media, ok := openMedia(connection.session, log)
	if !ok {
		hangup(ctx, connection.hangup, log)

		return
	}

	g.state.Started(ctx, line.Username, connection.callID, func() { end(turn.ErrCallEnded) })
	begun()

	err := context.Cause(ctx)
	if err == nil {
		services := g.services
		services.Log = log
		services.Speech = g.speech[line.Username]
		err = call.Run(ctx, media, services, call.Options{
			Line:          line.Username,
			BotID:         line.BotID,
			CallID:        connection.callID,
			Greeting:      connection.greeting,
			HoldingPhrase: g.config.Call.HoldingPhrase,
			HoldingDelay:  g.config.Call.HoldingDelay,
			TimeoutPhrase: g.config.Call.TimeoutPhrase,
			FailurePhrase: g.config.Call.FailurePhrase,
			TurnTimeout:   g.config.Call.TurnTimeout,
			MaxDuration:   g.config.Call.MaxDuration,
		})
	}

	reason := endReason(err, g.stopping())

	switch reason {
	case EndHungUp:
		log.Info("call ended")
	case EndMaxDuration:
		log.Info("call reached its maximum duration")
	case EndFailed:
		log.Error("call failed", "error", err)
	case EndStopping:
		log.Info("call ended because the gateway is stopping")
	case EndByServer:
		log.Info("the server ended the call, hanging up")
	}

	hangup(ctx, connection.hangup, log)
	g.state.Ended(connection.callID, reason)
}

// endReason tells how a call ended from call.Run's error.
func endReason(err error, stopping bool) EndReason {
	switch {
	case errors.Is(err, turn.ErrCallEnded):
		return EndByServer
	case errors.Is(err, call.ErrMaxDuration):
		return EndMaxDuration
	case stopping:
		return EndStopping
	// The server knows the bot asked, so the end is reported as an ordinary hang-up.
	case err == nil, errors.Is(err, io.EOF), errors.Is(err, context.Canceled), errors.Is(err, call.ErrBotHungUp):
		return EndHungUp
	default:
		return EndFailed
	}
}

func (g *Gateway) stopping() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.ctx == nil || g.ctx.Err() != nil
}

func openMedia(session *diago.DialogMedia, log *slog.Logger) (call.Media, bool) {
	props := diago.MediaProps{}

	reader, err := session.AudioReader(diago.WithAudioReaderMediaProps(&props))
	if err != nil {
		log.Error("open call audio", "error", err)

		return call.Media{}, false
	}

	writer, err := session.AudioWriter()
	if err != nil {
		log.Error("open call audio", "error", err)

		return call.Media{}, false
	}

	decoder, err := audio.NewPCMDecoderReader(props.Codec.PayloadType, reader)
	if err != nil {
		log.Error("decode call audio", "codec", props.Codec.Name, "error", err)

		return call.Media{}, false
	}

	encoder, err := audio.NewPCMEncoderWriter(props.Codec.PayloadType, writer)
	if err != nil {
		log.Error("encode call audio", "codec", props.Codec.Name, "error", err)

		return call.Media{}, false
	}

	return call.Media{In: decoder, Out: encoder}, true
}

// hangup outlives the call's context, which is often what ended.
func hangup(ctx context.Context, do func(context.Context) error, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hangupTimeout)
	defer cancel()

	if err := do(ctx); err != nil && !errors.Is(err, sip.ErrTransactionTerminated) {
		log.Debug("hang up", "error", err)
	}
}
