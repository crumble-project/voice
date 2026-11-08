// Package control serves the local HTTP API that places outbound calls.
package control

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/crumble-project/crumble-voice/internal/sipline"
)

const (
	maxRequestBody    = 16 << 10
	maxHeaderBytes    = 16 << 10
	maxGreeting       = 1000
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	idleTimeout       = 60 * time.Second
	// Room to answer after the call was answered or timed out.
	writeMargin = 30 * time.Second
)

// Dialer places a call and returns its SIP call ID once answered.
type Dialer interface {
	Dial(ctx context.Context, botID, number, greeting string) (string, error)
}

// Handler returns the API: POST /v1/calls and GET /healthz.
func Handler(dialer Dialer, token string, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	mux.Handle("POST /v1/calls", authorized(token, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		placeCall(w, r, dialer, log)
	})))

	return mux
}

func authorized(token string, next http.Handler) http.Handler {
	expected := []byte("Bearer " + token)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), expected) != 1 {
			writeError(w, http.StatusUnauthorized, "missing or wrong bearer token")

			return
		}

		next.ServeHTTP(w, r)
	})
}

type callRequest struct {
	BotID    string `json:"botId"`
	To       string `json:"to"`
	Greeting string `json:"greeting"`
}

func placeCall(w http.ResponseWriter, r *http.Request, dialer Dialer, log *slog.Logger) {
	if media := r.Header.Get("Content-Type"); !strings.HasPrefix(media, "application/json") {
		writeError(w, http.StatusUnsupportedMediaType, "send application/json")

		return
	}

	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	decoder.DisallowUnknownFields()

	var request callRequest
	if err := decoder.Decode(&request); err != nil || decoder.More() {
		writeError(w, http.StatusBadRequest, "body must be one JSON object with botId, to, and greeting")

		return
	}

	if request.BotID == "" || request.To == "" {
		writeError(w, http.StatusBadRequest, "botId and to are required")

		return
	}

	if strings.TrimSpace(request.Greeting) == "" || utf8.RuneCountInString(request.Greeting) > maxGreeting {
		writeError(w, http.StatusBadRequest, "greeting is required, at most 1000 characters")

		return
	}

	id, err := dialer.Dial(r.Context(), request.BotID, request.To, request.Greeting)

	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, map[string]string{"callId": id})
	case errors.Is(err, sipline.ErrUnknownBot):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, sipline.ErrNotOwner):
		writeError(w, http.StatusForbidden, err.Error())
	case errors.Is(err, sipline.ErrBusy):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, sipline.ErrUnanswered):
		writeError(w, http.StatusGatewayTimeout, err.Error())
	default:
		log.Error("place call", "bot", request.BotID, "error", err)
		writeError(w, http.StatusBadGateway, "the call could not be placed")
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, body map[string]string) {
	data, err := json.Marshal(body)
	if err != nil {
		http.Error(w, "encode response", http.StatusInternalServerError)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(data, '\n'))
}

// NewServer returns an HTTP server with timeouts. Writes may wait for the call to be answered.
func NewServer(addr string, handler http.Handler, ringTimeout time.Duration) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      ringTimeout + writeMargin,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
	}
}
