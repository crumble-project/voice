package control_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/crumble-project/crumble-voice/internal/control"
	"github.com/crumble-project/crumble-voice/internal/sipline"
)

const token = "0123456789abcdef0123456789abcdef"

type fakeDialer struct {
	err   error
	calls int
}

func (f *fakeDialer) Dial(context.Context, string, string, string) (string, error) {
	f.calls++

	return "call-1", f.err
}

func TestPlaceCall(t *testing.T) {
	t.Parallel()

	valid := `{"botId":"bot","to":"**610","greeting":"Hi, it is Ada."}`

	cases := []struct {
		name   string
		auth   string
		body   string
		err    error
		status int
		dialed bool
	}{
		{name: "placed", auth: "Bearer " + token, body: valid, status: http.StatusCreated, dialed: true},
		{name: "wrong token", auth: "Bearer nope", body: valid, status: http.StatusUnauthorized},
		{name: "no token", body: valid, status: http.StatusUnauthorized},
		{
			name: "unknown field", auth: "Bearer " + token,
			body: `{"botId":"bot","to":"**610","greeting":"Hi","record":true}`, status: http.StatusBadRequest,
		},
		{name: "no greeting", auth: "Bearer " + token, body: `{"botId":"bot","to":"**610","greeting":" "}`, status: http.StatusBadRequest},
		{
			name: "not the owner", auth: "Bearer " + token, body: valid, err: sipline.ErrNotOwner,
			status: http.StatusForbidden, dialed: true,
		},
		{name: "busy", auth: "Bearer " + token, body: valid, err: sipline.ErrBusy, status: http.StatusConflict, dialed: true},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dialer := &fakeDialer{err: test.err}
			handler := control.Handler(dialer, token, slog.New(slog.DiscardHandler))

			request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/calls", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")

			if test.auth != "" {
				request.Header.Set("Authorization", test.auth)
			}

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			if recorder.Code != test.status {
				t.Errorf("status = %d, want %d: %s", recorder.Code, test.status, recorder.Body.String())
			}

			if (dialer.calls > 0) != test.dialed {
				t.Errorf("dialed = %v, want %v", dialer.calls > 0, test.dialed)
			}
		})
	}
}
