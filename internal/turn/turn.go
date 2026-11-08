// Package turn describes the progress of one bot turn, shared by the Crumble
// client that observes it and the call that reacts to it.
package turn

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
