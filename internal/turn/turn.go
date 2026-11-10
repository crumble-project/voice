// Package turn describes the progress of one bot turn, shared by the Crumble
// client that observes it and the call that reacts to it.
package turn

import (
	"crypto/rand"
	"errors"
	"fmt"
)

// ErrCallEnded means the server ended the call, so the gateway hangs up.
var ErrCallEnded = errors.New("the server ended the call")

// Observer receives a turn's progress. Every field is optional.
type Observer struct {
	// Accepted runs once Crumble queued the message.
	Accepted func()
	// Running runs once the bot started working on it.
	Running func()
	// Partial receives the reply while it is written: text is everything the
	// current generation wrote so far, and generation increases when the bot
	// starts a new piece of text, for example after a tool call.
	Partial func(generation int, text string)
	// HangUp runs when the bot asked to end the call once its reply was said.
	HangUp func()
}

// NotifyAccepted calls Accepted when set.
func (o Observer) NotifyAccepted() {
	if o.Accepted != nil {
		o.Accepted()
	}
}

// NotifyRunning calls Running when set.
func (o Observer) NotifyRunning() {
	if o.Running != nil {
		o.Running()
	}
}

// NotifyPartial calls Partial when set.
func (o Observer) NotifyPartial(generation int, text string) {
	if o.Partial != nil {
		o.Partial(generation, text)
	}
}

// NotifyHangUp calls HangUp when set.
func (o Observer) NotifyHangUp() {
	if o.HangUp != nil {
		o.HangUp()
	}
}

const (
	uuidVersionMask = 0x0f
	uuidVersion4    = 0x40
	uuidVariantMask = 0x3f
	uuidVariantRFC  = 0x80
)

// NewID returns a random version 4 UUID, the format the server requires for call and request IDs.
func NewID() string {
	var b [16]byte

	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error.

	b[6] = (b[6] & uuidVersionMask) | uuidVersion4
	b[8] = (b[8] & uuidVariantMask) | uuidVariantRFC

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
