package openai

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/coder/websocket"
	diagoaudio "github.com/emiago/diago/audio"

	"github.com/crumble-project/crumble-voice/internal/audio"
)

const (
	// eventTypeError is the type of the error event both APIs send.
	eventTypeError = "error"
	// formatPCMU is G.711 μ-law, the telephone's own format.
	formatPCMU = "audio/pcmu"
)

// audioAppend sends base64 audio; both APIs use the same shape under different type names.
type audioAppend struct {
	Type  string `json:"type"`
	Audio string `json:"audio"`
}

// dial opens an authenticated WebSocket to path under an API base such as https://api.openai.com.
func dial(ctx context.Context, base *url.URL, path string, query url.Values, apiKey string) (*websocket.Conn, error) {
	endpoint := *base
	endpoint.Scheme = map[string]string{"https": "wss", "http": "ws"}[base.Scheme]
	endpoint.Path = strings.TrimSuffix(endpoint.Path, "/") + path
	endpoint.RawQuery = query.Encode()

	conn, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + apiKey}},
	})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}

	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", path, err)
	}

	conn.SetReadLimit(maxEvent)

	return conn, nil
}

func sendJSON(ctx context.Context, conn *websocket.Conn, message any) error {
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()

	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		return fmt.Errorf("send event: %w", err)
	}

	return nil
}

func readJSON(ctx context.Context, conn *websocket.Conn, target any) error {
	kind, data, err := conn.Read(ctx)
	if err != nil {
		return fmt.Errorf("read event: %w", err)
	}

	if kind != websocket.MessageText {
		return ErrBinaryMessage
	}

	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("decode event: %w", err)
	}

	return nil
}

// sendUlaw uploads the caller's telephone PCM as μ-law until ctx or done ends.
// Everything queued while a send was in flight goes out as one message, so a
// slow link costs latency instead of dropping the call.
func sendUlaw(ctx context.Context, conn *websocket.Conn, input <-chan []byte, done <-chan struct{}, eventType string) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-done:
			return nil
		case pcm := <-input:
			batch := append([]byte(nil), pcm...)
			for drained := false; !drained && len(batch) < maxBatchBytes; {
				select {
				case more := <-input:
					batch = append(batch, more...)
				default:
					drained = true
				}
			}

			ulaw := make([]byte, len(batch)/audio.BytesPerSample)
			if _, err := diagoaudio.EncodeUlawTo(ulaw, batch); err != nil {
				return fmt.Errorf("encode caller audio: %w", err)
			}

			if err := sendJSON(ctx, conn, audioAppend{Type: eventType, Audio: base64.StdEncoding.EncodeToString(ulaw)}); err != nil {
				return err
			}
		}
	}
}

// decodeUlaw turns base64 μ-law from the model into telephone PCM.
func decodeUlaw(delta string) ([]byte, error) {
	ulaw, err := base64.StdEncoding.DecodeString(delta)
	if err != nil {
		return nil, fmt.Errorf("decode model audio: %w", err)
	}

	pcm := make([]byte, len(ulaw)*audio.BytesPerSample)
	if _, err := diagoaudio.DecodeUlawTo(pcm, ulaw); err != nil {
		return nil, fmt.Errorf("decode model audio: %w", err)
	}

	return pcm, nil
}
