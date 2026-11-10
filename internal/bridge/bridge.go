// Package bridge connects the server's voice gateway events to the SIP gateway: it applies
// configs, places the calls bots ask for, and reports calls and line registration.
package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/crumble-project/crumble-voice/internal/crumble"
	"github.com/crumble-project/crumble-voice/internal/sipline"
)

// Run serves the events stream until ctx ends, like crumble.Client.Serve, and ends
// with ErrUnauthorized when the server refuses the token on a line report too. calls
// must be the gateway's sipline.Options.Calls.
func Run(ctx context.Context, gateway *sipline.Gateway, server *crumble.Client, calls *Calls, log *slog.Logger) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	group, ctx := errgroup.WithContext(ctx)
	placer := &placer{gateway: gateway, server: server, state: calls, log: log, owner: nil, calls: map[string]*placed{}}

	group.Go(func() error { return reportLines(ctx, gateway, server, log) })

	group.Go(func() error {
		defer cancel()

		// Each connection's first event is a config, which applies and reports the lines,
		// so the server gets the full line report once the stream is live again.
		return server.Serve(ctx, crumble.Handlers{
			Config: func(config crumble.VoiceConfig) {
				placer.configure(config)
				gateway.Apply(setup(config))
				placer.recheck(ctx)
				calls.resend(ctx)
			},
			Call:   func(call crumble.VoiceCall) { placer.call(ctx, call) },
			Cancel: placer.cancel,
		}, log)
	})

	err := group.Wait()
	placer.stopped(ctx)

	//nolint:wrapcheck // Serve's errors are the ones callers check with errors.Is.
	return err
}

// setup turns the server's config into the lines the SIP gateway serves.
func setup(served crumble.VoiceConfig) sipline.Setup {
	lines := make([]sipline.Line, 0, len(served.Lines))

	for _, line := range served.Lines {
		lines = append(lines, sipline.Line{
			Username: line.Username, BotID: line.BotID, BotName: line.BotName, DisplayName: line.DisplayName,
		})
	}

	return sipline.Setup{OwnerNumbers: served.OwnerNumbers, Lines: lines}
}

// reportLines sends the lines' registration state each time it may have changed. A refused
// report waits for the next change or reconnect; only a refused token ends it.
func reportLines(ctx context.Context, gateway *sipline.Gateway, server *crumble.Client, log *slog.Logger) error {
	report := func() error {
		states := gateway.Lines()
		lines := make([]crumble.LineReport, 0, len(states))

		for _, state := range states {
			var reason *string
			if state.Error != "" {
				reason = &state.Error
			}

			lines = append(lines, crumble.LineReport{Username: state.Username, Registered: state.Registered, Error: reason})
		}

		err := server.ReportLines(ctx, lines)
		if errors.Is(err, crumble.ErrUnauthorized) {
			return fmt.Errorf("report line registration: %w", err)
		}

		if err != nil && ctx.Err() == nil {
			log.Warn("report line registration", "error", err)
		}

		return nil
	}

	for {
		select {
		case <-gateway.LinesChanged():
			if err := report(); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// placer dials each call event's call once and reports how it went.
type placer struct {
	gateway *sipline.Gateway
	server  *crumble.Client
	state   *Calls
	log     *slog.Logger

	mu    sync.Mutex
	owner []string
	// Call IDs stay here for callMemory, so a re-sent call event never dials again.
	calls map[string]*placed
}

// The server re-sends a call only until it is acknowledged, at most 210 seconds
// after the call event; five minutes outlasts that with room for clock drift.
const callMemory = 5 * time.Minute

// Room for the reports of calls cut short by shutdown.
const shutdownReportTimeout = 5 * time.Second

type placed struct {
	taken time.Time
	// done is true once the call was reported or dropped; its conversation may go on.
	done bool
	// stop cancels the ringing and, once the call is answered, hangs up.
	stop func()
	// settled means the server took the answered report, so it sends no cancel for this call.
	settled bool
	// ringing is true while the call is dialed and not yet answered or ended.
	ringing bool
}

func (p *placer) configure(config crumble.VoiceConfig) {
	p.mu.Lock()
	p.owner = slices.Clone(config.OwnerNumbers)
	p.mu.Unlock()
}

func (p *placer) call(ctx context.Context, event crumble.VoiceCall) {
	log := p.log.With("call_id", event.CallID, "line", event.Username)

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, taken := p.calls[event.CallID]; taken {
		log.Debug("ignored a call event for a call already taken")

		return
	}

	now := time.Now()
	maps.DeleteFunc(p.calls, func(_ string, old *placed) bool { return old.done && now.Sub(old.taken) > callMemory })

	ringCtx, cancel := context.WithCancel(ctx)
	call := &placed{taken: now, done: false, stop: cancel, settled: false, ringing: false}
	p.calls[event.CallID] = call
	owner := slices.Contains(p.owner, event.To)

	go func() {
		defer func() {
			p.mu.Lock()
			call.done = true
			p.mu.Unlock()
		}()
		defer cancel()

		p.place(ctx, ringCtx, call, event, owner, log)
	}()
}

// recheck re-sends the ringing report of every call still ringing. The server sends
// cancel only to the stream live at that moment, so one lost in a reconnect shows up as a 404.
func (p *placer) recheck(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for callID, call := range p.calls {
		if !call.ringing {
			continue
		}

		go func() {
			err := p.server.ReportCall(ctx, callID, crumble.OutcomeRinging, "")
			if errors.Is(err, crumble.ErrCallGone) {
				p.log.Info("the server dropped a ringing call while disconnected, hanging up", "call_id", callID)
				p.cancel(callID)
			}
		}()
	}
}

// stopped reports every call not yet reported as failed once Run ends, so the bot does
// not wait for the server's timeout. It does not wait for the dials: a CANCEL on a
// closing SIP stack can take 32 seconds to time out.
func (p *placer) stopped(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownReportTimeout)
	defer cancel()

	var reports sync.WaitGroup

	p.mu.Lock()

	for callID, call := range p.calls {
		if call.done {
			continue
		}

		call.done = true

		reports.Go(func() {
			if err := p.server.ReportCall(ctx, callID, crumble.OutcomeFailed, "the voice gateway stopped"); err != nil {
				p.log.Warn("report call at shutdown", "call_id", callID, "error", err)
			}
		})
	}

	p.mu.Unlock()
	reports.Wait()
}

func (p *placer) cancel(callID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	call, ok := p.calls[callID]
	if !ok || call.settled {
		return
	}

	p.log.Info("the server cancelled the call", "call_id", callID)
	call.stop()
}

// place reports under ctx, not ringCtx: a report must go out after the ringing ended.
func (p *placer) place(ctx, ringCtx context.Context, call *placed, event crumble.VoiceCall, owner bool, log *slog.Logger) {
	if !owner {
		log.Warn("refused a call to a number that is not one of the owner's")
		p.report(ctx, event.CallID, crumble.OutcomeFailed, sipline.ErrNotOwner.Error(), log)

		return
	}

	err := p.server.ReportCall(ctx, event.CallID, crumble.OutcomeRinging, "")
	if errors.Is(err, crumble.ErrCallGone) {
		log.Info("the server dropped the call before it was dialed")

		return
	}

	if errors.Is(err, crumble.ErrUnauthorized) {
		log.Warn("the server refused the token, so the call is not dialed")

		return
	}

	// The outcome report acknowledges the call too, so a lost ringing report does not stop the dial.
	if err != nil {
		log.Warn("acknowledge call", "error", err)
	}

	p.mu.Lock()
	call.ringing = true
	p.mu.Unlock()

	hangup, err := p.gateway.Dial(ringCtx, sipline.Outbound{
		CallID: event.CallID, Username: event.Username, Number: event.To, Greeting: event.Greeting,
	})

	p.mu.Lock()
	call.ringing = false
	p.mu.Unlock()

	if err == nil {
		p.mu.Lock()
		stop := call.stop
		call.stop = func() {
			stop()
			hangup()
		}
		p.mu.Unlock()
	}

	if ringCtx.Err() != nil {
		if hangup != nil {
			hangup()
		}

		// A cancel means the server already dropped the call; on shutdown, stopped reports it.
		return
	}

	if err == nil {
		// An answered report that overtakes the start makes the server forget the call was
		// the bot's, so the start then records an inbound call without the greeting.
		p.state.awaitStart(ctx, event.CallID)
	}

	outcome, detail := outcomeOf(err)
	if outcome == crumble.OutcomeFailed {
		log.Warn("call failed", "error", err)
	}

	if p.report(ctx, event.CallID, outcome, detail, log) && outcome == crumble.OutcomeAnswered {
		p.mu.Lock()
		call.settled = true
		p.mu.Unlock()
	}
}

// report returns whether the server took the report. A 404 means the server dropped
// the call, so a call still running is hung up.
func (p *placer) report(ctx context.Context, callID, outcome, detail string, log *slog.Logger) bool {
	err := p.server.ReportCall(ctx, callID, outcome, detail)
	if errors.Is(err, crumble.ErrCallGone) {
		log.Info("the server no longer knows the call, hanging up", "outcome", outcome)
		p.cancel(callID)

		return false
	}

	if err != nil {
		log.Warn("report call", "outcome", outcome, "error", err)

		return false
	}

	return true
}

func outcomeOf(err error) (string, string) {
	switch {
	case err == nil:
		return crumble.OutcomeAnswered, ""
	case errors.Is(err, sipline.ErrUnanswered):
		return crumble.OutcomeUnanswered, ""
	case errors.Is(err, sipline.ErrOwnerBusy):
		return crumble.OutcomeBusy, ""
	default:
		return crumble.OutcomeFailed, err.Error()
	}
}

const (
	// The greeting waits at most this long for the start report; the report goes on after.
	startWait = time.Second
	// The gateway waits at most 2 seconds for its calls to hang up at shutdown.
	endWait        = 2 * time.Second
	maxUndelivered = 64
)

// Calls reports when each call starts and ends, and reports both again after every
// reconnect, since the server ends a call that a new connection does not confirm.
type Calls struct {
	server *crumble.Client
	log    *slog.Logger
	older  sync.Once

	mu     sync.Mutex
	active map[string]*running
	// undelivered holds the ended reports that failed, oldest first.
	undelivered []ending
}

type running struct {
	username string
	hangUp   func()
	// reported closes once the first start report got its answer or failed.
	reported chan struct{}
}

type ending struct {
	callID string
	reason string
}

// NewCalls returns the reports for sipline.Options.Calls.
func NewCalls(server *crumble.Client, log *slog.Logger) *Calls {
	return &Calls{server: server, log: log, older: sync.Once{}, active: map[string]*running{}, undelivered: nil}
}

// Started reports the call before its greeting, waiting at most startWait. A failed report
// does not stop the call; a 409 hangs it up.
func (c *Calls) Started(ctx context.Context, username, callID string, hangUp func()) {
	call := &running{username: username, hangUp: hangUp, reported: make(chan struct{})}

	c.mu.Lock()
	c.active[callID] = call
	c.mu.Unlock()

	go func() {
		defer close(call.reported)

		c.start(ctx, callID, call)
	}()

	select {
	case <-call.reported:
	case <-ctx.Done():
	case <-time.After(startWait):
		c.log.Warn("the call start report is slow; the call goes on", "call_id", callID)
	}
}

// Ended reports how the call ended. It runs before the gateway releases the call, so at
// shutdown the report goes out within the gateway's wait for its calls.
func (c *Calls) Ended(callID string, reason sipline.EndReason) {
	c.mu.Lock()
	delete(c.active, callID)
	c.mu.Unlock()

	wire, report := wireReason(reason)
	if !report {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), endWait)
	defer cancel()

	c.deliver(ctx, ending{callID: callID, reason: wire})
}

// awaitStart returns once the start report of an active call got its answer or failed.
func (c *Calls) awaitStart(ctx context.Context, callID string) {
	c.mu.Lock()
	call, ok := c.active[callID]
	c.mu.Unlock()

	if !ok {
		return
	}

	select {
	case <-call.reported:
	case <-ctx.Done():
	}
}

func (c *Calls) start(ctx context.Context, callID string, call *running) {
	err := c.server.StartCall(ctx, call.username, callID)

	switch {
	case err == nil, ctx.Err() != nil:
	case errors.Is(err, crumble.ErrCallEnded):
		c.log.Info("the server ended the call, hanging up", "call_id", callID)
		call.hangUp()
	case errors.Is(err, crumble.ErrNoCallState):
		c.older.Do(func() {
			c.log.Warn("the server does not know the call: it predates call state or no longer serves the line; calls go on",
				"call_id", callID, "line", call.username)
		})
	default:
		c.log.Warn("report call start", "call_id", callID, "error", err)
	}
}

// deliver keeps a report that may yet arrive for the next connection.
func (c *Calls) deliver(ctx context.Context, report ending) {
	err := c.server.ReportEnded(ctx, report.callID, report.reason)
	if err == nil {
		return
	}

	if errors.Is(err, crumble.ErrRefused) || errors.Is(err, crumble.ErrCallGone) || errors.Is(err, crumble.ErrUnauthorized) {
		c.log.Warn("the server refused the call end report", "call_id", report.callID, "error", err)

		return
	}

	c.log.Warn("report call end; sending it again after the next reconnect", "call_id", report.callID, "error", err)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.undelivered = append(c.undelivered, report)
	if len(c.undelivered) > maxUndelivered {
		c.undelivered = slices.Delete(c.undelivered, 0, len(c.undelivered)-maxUndelivered)
	}
}

// resend confirms every active call and sends every undelivered end report again.
func (c *Calls) resend(ctx context.Context) {
	c.mu.Lock()
	active := maps.Clone(c.active)
	undelivered := c.undelivered
	c.undelivered = nil
	c.mu.Unlock()

	for callID, call := range active {
		go c.start(ctx, callID, call)
	}

	for _, report := range undelivered {
		go c.deliver(ctx, report)
	}
}

// wireReason returns the reason the server knows, and false when no report is due.
func wireReason(reason sipline.EndReason) (string, bool) {
	switch reason {
	case sipline.EndHungUp:
		return crumble.ReasonHungUp, true
	case sipline.EndMaxDuration:
		return crumble.ReasonMaxDuration, true
	case sipline.EndFailed:
		return crumble.ReasonFailed, true
	case sipline.EndStopping:
		return crumble.ReasonStopping, true
	case sipline.EndByServer:
	}

	return "", false
}
