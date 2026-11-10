package sipline

import (
	"context"
	"log/slog"
	"testing"

	"github.com/crumble-project/crumble-voice/internal/call"
	"github.com/crumble-project/crumble-voice/internal/config"
)

const ownerNumber = "**610"

func admitting(t *testing.T) *Gateway {
	t.Helper()

	cfg := &config.Config{
		// The RTP range matches the other tests': New keeps it as package state.
		SIP: config.SIP{
			Transport: "udp", BindHost: "127.0.0.1", RTPPortStart: 26000, RTPPortEnd: 26100,
			Accounts: []config.Account{{Username: "ada", Password: "secret", Voice: "marin"}},
		},
		Call: config.Call{MaxConcurrent: 1},
	}

	gateway, err := New(cfg, call.Services{Speech: nil, Bot: nil, Log: slog.New(slog.DiscardHandler)},
		Options{SpeechFor: func(string) call.Speech { return nil }, StrangerRecording: nil, Calls: nil})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	gateway.Apply(Setup{
		OwnerNumbers: []string{ownerNumber},
		Lines:        []Line{{Username: "ada", BotID: "bot_ada", BotName: "Ada", DisplayName: "Ada"}},
	})

	if _, applied := gateway.take(); !applied {
		t.Fatal("the first setup was not applied")
	}

	return gateway
}

//nolint:paralleltest // New sets the media package's port range.
func TestAdmittedCallHoldsItsLine(t *testing.T) {
	gateway := admitting(t)
	gateway.ctx = context.Background()

	line, running, admitted := gateway.admit("ada", ownerNumber)
	if admitted != admitOwner || running == nil || line.BotID != "bot_ada" {
		t.Fatalf("admit = %v for the bot %q, want the owner admitted to bot_ada", admitted, line.BotID)
	}

	gateway.Apply(Setup{OwnerNumbers: nil, Lines: nil})

	if _, applied := gateway.take(); applied {
		t.Error("a setup was applied while an admitted call held its line")
	}

	if _, _, second := gateway.admit("ada", ownerNumber); second != refuseBusy {
		t.Errorf("second admit = %v, want busy", second)
	}

	gateway.release(line.BotID)

	if _, applied := gateway.take(); !applied {
		t.Fatal("the setup was not applied once the call ended")
	}

	if _, _, after := gateway.admit("ada", ownerNumber); after != refuseUnknownLine {
		t.Errorf("admit after the line was removed = %v, want unknown line", after)
	}
}

//nolint:paralleltest // New sets the media package's port range.
func TestNoCallIsAdmittedWhileStopping(t *testing.T) {
	gateway := admitting(t)

	stopped, cancel := context.WithCancel(context.Background())
	cancel()

	for name, running := range map[string]context.Context{"after Run": nil, "while Run stops": stopped} {
		//nolint:fatcontext // The gateway's running context is the state under test.
		gateway.ctx = running

		if _, _, admitted := gateway.admit("ada", ownerNumber); admitted != refuseStopping {
			t.Errorf("%s: admit = %v, want stopping", name, admitted)
		}

		if _, _, admitted := gateway.admit("ada", "999"); admitted != refuseStopping {
			t.Errorf("%s: a stranger's admit = %v, want stopping", name, admitted)
		}
	}

	if len(gateway.active) != 0 || gateway.strangers != 0 {
		t.Errorf("claimed %d calls and %d strangers while stopping, want none", len(gateway.active), gateway.strangers)
	}

	// Returns only if no refused call was added to the calls a shutdown waits for.
	gateway.calls.Wait()
}
