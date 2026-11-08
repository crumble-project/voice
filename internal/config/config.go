// Package config loads and validates the voice gateway configuration.
//
// The file holds no secrets: every secret is read from the environment
// variable the file names, so the file can be checked in or shared.
package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Config is the validated gateway configuration with secrets resolved.
type Config struct {
	Crumble Crumble
	SIP     SIP
	Owner   Owner
	Lines   []Line
	Speech  Speech
	Control Control
	Call    Call
}

// Crumble is how the gateway signs in to the Crumble server as the owner.
type Crumble struct {
	BaseURL  *url.URL
	Email    string
	Password string
}

// SIP is the registrar and the local transport.
type SIP struct {
	Registrar      string
	Transport      string
	BindHost       string
	BindPort       int
	ExternalHost   string
	RTPPortStart   int
	RTPPortEnd     int
	RegisterExpiry time.Duration
	RegistrarHost  string
	// RegistrarPort is 0 when the registrar uses the transport's default port.
	RegistrarPort int
	RegistrarIPs  []netip.Addr
}

// Owner lists the caller IDs that belong to the owner.
type Owner struct {
	Numbers []string
}

// Line is one SIP account on the registrar, answered by one bot.
type Line struct {
	Username    string
	Password    string
	DisplayName string
	BotID       string
	// Voice is the line's voice, speech.voice unless the line sets its own.
	Voice string
}

// Speech configures speech-to-text and text-to-speech.
type Speech struct {
	// Mode is "pipeline" (transcription plus text-to-speech), "realtime" (one
	// Realtime API session that only speaks the bot's text), or "live" (GPT-Live).
	Mode               string
	Provider           string
	APIKey             string
	BaseURL            *url.URL
	TranscriptionModel string
	Language           string
	SynthesisModel     string
	Voice              string
	VoiceInstructions  string
	LiveModel          string
	LiveInstructions   string
	RealtimeModel      string
}

// NamePlaceholder in call.answerGreeting becomes the bot's name.
const NamePlaceholder = "{name}"

// Speech modes.
const (
	ModePipeline = "pipeline"
	ModeRealtime = "realtime"
	ModeLive     = "live"
)

// Control is the local HTTP API that places outbound calls.
type Control struct {
	Listen netip.AddrPort
	Token  string
}

// Call bounds each call.
type Call struct {
	MaxDuration     time.Duration
	TurnTimeout     time.Duration
	MaxConcurrent   int
	CallNote        string
	HoldingPhrase   string
	HoldingDelay    time.Duration
	TimeoutPhrase   string
	FailurePhrase   string
	UnansweredAfter time.Duration
	// AnswerGreeting is said when the bot answers a call; {name} becomes the bot's name.
	AnswerGreeting string
	// StrangerRecording is a WAV file played to callers who are not the owner,
	// instead of rejecting them. Empty rejects them.
	StrangerRecording string
}

type fileConfig struct {
	Crumble fileCrumble `json:"crumble"`
	SIP     fileSIP     `json:"sip"`
	Owner   fileOwner   `json:"owner"`
	Lines   []fileLine  `json:"lines"`
	Speech  fileSpeech  `json:"speech"`
	Control fileControl `json:"control"`
	Call    fileCall    `json:"call"`
}

type fileCrumble struct {
	BaseURL     string `json:"baseUrl"`
	Email       string `json:"email"`
	PasswordEnv string `json:"passwordEnv"`
}

type fileSIP struct {
	Registrar      string   `json:"registrar"`
	Transport      string   `json:"transport"`
	BindHost       string   `json:"bindHost"`
	BindPort       int      `json:"bindPort"`
	ExternalHost   string   `json:"externalHost"`
	RTPPortStart   int      `json:"rtpPortStart"`
	RTPPortEnd     int      `json:"rtpPortEnd"`
	RegisterExpiry duration `json:"registerExpiry"`
}

type fileOwner struct {
	Numbers []string `json:"numbers"`
}

type fileLine struct {
	Username    string `json:"username"`
	PasswordEnv string `json:"passwordEnv"`
	DisplayName string `json:"displayName"`
	BotID       string `json:"botId"`
	Voice       string `json:"voice,omitempty"`
}

type fileSpeech struct {
	Mode               string `json:"mode"`
	Provider           string `json:"provider"`
	APIKeyEnv          string `json:"apiKeyEnv"`
	BaseURL            string `json:"baseUrl"`
	TranscriptionModel string `json:"transcriptionModel"`
	Language           string `json:"language"`
	SynthesisModel     string `json:"synthesisModel"`
	Voice              string `json:"voice"`
	VoiceInstructions  string `json:"voiceInstructions"`
	LiveModel          string `json:"liveModel"`
	LiveInstructions   string `json:"liveInstructions"`
	RealtimeModel      string `json:"realtimeModel"`
}

type fileControl struct {
	Listen   string `json:"listen"`
	TokenEnv string `json:"tokenEnv"`
}

type fileCall struct {
	MaxDuration    duration `json:"maxDuration"`
	TurnTimeout    duration `json:"turnTimeout"`
	MaxConcurrent  int      `json:"maxConcurrent"`
	CallNote       string   `json:"callNote"`
	HoldingPhrase  string   `json:"holdingPhrase"`
	HoldingDelay   duration `json:"holdingDelay"`
	TimeoutPhrase  string   `json:"timeoutPhrase"`
	FailurePhrase  string   `json:"failurePhrase"`
	AnswerGreeting string   `json:"answerGreeting"`
	// A relative path is relative to the configuration file.
	StrangerRecording string   `json:"strangerRecording"`
	UnansweredAfter   duration `json:"unansweredAfter"`
}

type duration time.Duration

func (d *duration) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return fmt.Errorf("duration must be a string such as \"30s\": %w", err)
	}

	parsed, err := time.ParseDuration(text)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", text, err)
	}

	*d = duration(parsed)

	return nil
}

const (
	maxConfigBytes      = 1 << 20
	minSecretLength     = 32
	maxCallNoteChars    = 1000
	maxPhraseChars      = 300
	maxDisplayName      = 64
	maxInstructions     = 4096
	maxLiveInstructions = 16384
	maxConcurrent       = 64
	maxEmailLength      = 320
)

var (
	// Telephone systems also use internal numbers such as **611, so * and # are allowed.
	numberPattern   = regexp.MustCompile(`^\+?[0-9*#]{1,32}$`)
	sipUserPattern  = regexp.MustCompile(`^[A-Za-z0-9._~+-]{1,64}$`)
	botIDPattern    = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	envNamePattern  = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,127}$`)
	languagePattern = regexp.MustCompile(`^[a-z]{2}$`)
	modelPattern    = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,100}$`)
)

var (
	// ErrTooLarge means the configuration file exceeds its size limit.
	ErrTooLarge = errors.New("config file is too large")
	// ErrTrailingData means the file holds more than one JSON value.
	ErrTrailingData = errors.New("decode config: trailing data after the JSON object")
	// ErrNoAddresses means the registrar host resolved to nothing.
	ErrNoAddresses = errors.New("host has no addresses")
)

// Environment returns the value of an environment variable and whether it is set.
type Environment func(name string) (string, bool)

// Load reads, decodes, and validates a configuration file.
func Load(ctx context.Context, path string, env Environment) (*Config, error) {
	file, err := os.Open(path) //nolint:gosec // The path is the operator's own command-line argument.
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}

	defer func() { _ = file.Close() }()

	data, err := readLimited(file)
	if err != nil {
		return nil, err
	}

	config, err := Parse(ctx, data, env)
	if err != nil {
		return nil, err
	}

	recording := config.Call.StrangerRecording
	if recording != "" && !filepath.IsAbs(recording) {
		config.Call.StrangerRecording = filepath.Join(filepath.Dir(path), recording)
	}

	return config, nil
}

func readLimited(file *os.File) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	if len(data) > maxConfigBytes {
		return nil, fmt.Errorf("%w: limit is %d bytes", ErrTooLarge, maxConfigBytes)
	}

	return data, nil
}

// Parse decodes and validates configuration JSON. Unknown fields are errors.
func Parse(ctx context.Context, data []byte, env Environment) (*Config, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var raw fileConfig
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	if decoder.More() {
		return nil, ErrTrailingData
	}

	v := validator{ctx: ctx, env: env}
	config := v.config(&raw)

	if err := errors.Join(v.errs...); err != nil {
		return nil, fmt.Errorf("invalid config:\n%w", err)
	}

	return config, nil
}

type validator struct {
	ctx  context.Context //nolint:containedctx // Validation is one synchronous pass under this context.
	env  Environment
	errs []error
}

func (v *validator) failf(format string, args ...any) {
	v.errs = append(v.errs, &FieldError{Message: fmt.Sprintf(format, args...)})
}

// FieldError is one invalid configuration field.
type FieldError struct {
	Message string
}

func (e *FieldError) Error() string { return e.Message }

func (v *validator) config(raw *fileConfig) *Config {
	config := &Config{
		Crumble: v.crumble(raw.Crumble),
		SIP:     v.sip(raw.SIP),
		Owner:   v.owner(raw.Owner),
		Lines:   v.lines(raw.Lines),
		Speech:  v.speech(raw.Speech),
		Control: v.control(raw.Control),
		Call:    v.call(raw.Call),
	}

	v.voices(config)

	return config
}

// Voices returns the voices OpenAI documents for a speech mode: text-to-speech
// voices for the pipeline, the Realtime API's ten, and the pipeline's plus
// GPT-Live's own for live mode.
func Voices(mode string) []string {
	pipeline := []string{
		"alloy", "ash", "ballad", "cedar", "coral", "echo", "fable", "marin", "nova", "onyx", "sage", "shimmer", "verse",
	}

	switch mode {
	case ModeRealtime:
		return []string{"alloy", "ash", "ballad", "cedar", "coral", "echo", "marin", "sage", "shimmer", "verse"}
	case ModeLive:
	default:
		return pipeline
	}

	return slices.Concat(pipeline, []string{
		"beacon", "bossa", "brise", "cinder", "delta", "flitz", "gleam", "harema", "juni", "meridian", "nira",
		"noeul", "nuri", "quartz", "ripple", "shida", "sillage", "stone", "tempo", "vesper", "willow",
	})
}

// voices checks speech.voice and each line's voice against the mode's voices,
// and gives lines without their own voice the default.
func (v *validator) voices(config *Config) {
	allowed := Voices(config.Speech.Mode)

	check := func(field, voice string) {
		if !slices.Contains(allowed, voice) {
			v.failf("%s: %q is not a %s voice; choose one of %s", field, voice, config.Speech.Mode, strings.Join(allowed, ", "))
		}
	}

	check("speech.voice", config.Speech.Voice)

	for i := range config.Lines {
		if config.Lines[i].Voice == "" {
			config.Lines[i].Voice = config.Speech.Voice

			continue
		}

		check("lines["+strconv.Itoa(i)+"].voice", config.Lines[i].Voice)
	}
}

func (v *validator) secret(field, name string, minLength int) string {
	if !envNamePattern.MatchString(name) {
		v.failf("%s: %q is not an environment variable name", field, name)

		return ""
	}

	value, ok := v.env(name)
	if !ok || value == "" {
		v.failf("%s: environment variable %s is not set", field, name)

		return ""
	}

	if len(value) < minLength {
		v.failf("%s: environment variable %s must hold at least %d characters", field, name, minLength)
	}

	return value
}

func (v *validator) crumble(raw fileCrumble) Crumble {
	base := v.serviceURL("crumble.baseUrl", raw.BaseURL)

	if raw.Email == "" || len(raw.Email) > maxEmailLength || !strings.Contains(raw.Email, "@") {
		v.failf("crumble.email: must be the owner's sign-in email")
	}

	return Crumble{
		BaseURL:  base,
		Email:    raw.Email,
		Password: v.secret("crumble.passwordEnv", raw.PasswordEnv, 1),
	}
}

// serviceURL accepts HTTPS anywhere and plain HTTP only to a loopback or private address.
func (v *validator) serviceURL(field, raw string) *url.URL {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		v.failf("%s: must be an absolute URL without credentials, query, or fragment", field)

		return nil
	}

	switch parsed.Scheme {
	case "https":
	case "http":
		if !privateHost(parsed.Hostname()) {
			v.failf("%s: plain http is only allowed to a loopback or private IP address", field)
		}
	default:
		v.failf("%s: scheme must be https or http", field)
	}

	if parsed.Path == "/" {
		parsed.Path = ""
	}

	return parsed
}

func privateHost(host string) bool {
	if host == "localhost" {
		return true
	}

	addr, err := netip.ParseAddr(host)

	return err == nil && (addr.IsLoopback() || addr.IsPrivate())
}

func (v *validator) sip(raw fileSIP) SIP {
	result := SIP{
		Registrar:      raw.Registrar,
		Transport:      raw.Transport,
		BindHost:       raw.BindHost,
		BindPort:       raw.BindPort,
		ExternalHost:   raw.ExternalHost,
		RTPPortStart:   raw.RTPPortStart,
		RTPPortEnd:     raw.RTPPortEnd,
		RegisterExpiry: time.Duration(raw.RegisterExpiry),
	}

	result.RegistrarHost, result.RegistrarPort, result.RegistrarIPs = v.registrar(raw.Registrar)

	if raw.Transport != "udp" && raw.Transport != "tcp" {
		v.failf("sip.transport: must be udp or tcp")
	}

	if _, err := netip.ParseAddr(raw.BindHost); err != nil {
		v.failf("sip.bindHost: must be an IP address: %v", err)
	}

	if raw.ExternalHost != "" {
		if _, err := netip.ParseAddr(raw.ExternalHost); err != nil {
			v.failf("sip.externalHost: must be an IP address when set: %v", err)
		}
	}

	if raw.BindPort < 1 || raw.BindPort > 65535 {
		v.failf("sip.bindPort: must be 1-65535")
	}

	if raw.RTPPortStart < 1024 || raw.RTPPortEnd > 65535 || raw.RTPPortEnd-raw.RTPPortStart < 2 ||
		raw.RTPPortStart%2 != 0 {
		v.failf("sip.rtpPortStart/rtpPortEnd: need an even start of at least 1024 and an end above start+2, at most 65535")
	}

	if result.RegisterExpiry < time.Minute || result.RegisterExpiry > time.Hour {
		v.failf("sip.registerExpiry: must be between 1m and 1h")
	}

	return result
}

func (v *validator) registrar(raw string) (string, int, []netip.Addr) {
	host, port := raw, 0

	if splitHost, splitPort, err := net.SplitHostPort(raw); err == nil {
		parsed, convErr := strconv.Atoi(splitPort)
		if convErr != nil || parsed < 1 || parsed > 65535 {
			v.failf("sip.registrar: port must be 1-65535")
		}

		host, port = splitHost, parsed
	}

	if host == "" {
		v.failf("sip.registrar: must be the registrar host, optionally with :port")

		return host, port, nil
	}

	addrs, err := resolve(v.ctx, host)
	if err != nil {
		v.failf("sip.registrar: %v", err)
	}

	return host, port, addrs
}

func resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	if addr, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{addr.Unmap()}, nil
	}

	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", host, err)
	}

	addrs := make([]netip.Addr, 0, len(ips))
	for _, ip := range ips {
		addrs = append(addrs, ip.Unmap())
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("resolve %s: %w", host, ErrNoAddresses)
	}

	return addrs, nil
}

func (v *validator) owner(raw fileOwner) Owner {
	if len(raw.Numbers) == 0 {
		v.failf("owner.numbers: list at least one number the owner calls from")
	}

	seen := map[string]bool{}

	for i, number := range raw.Numbers {
		if !numberPattern.MatchString(number) {
			v.failf("owner.numbers[%d]: %q must be digits, *, or #, with an optional leading +", i, number)
		}

		if seen[number] {
			v.failf("owner.numbers[%d]: %q is listed twice", i, number)
		}

		seen[number] = true
	}

	return Owner(raw)
}

func (v *validator) lines(raw []fileLine) []Line {
	if len(raw) == 0 {
		v.failf("lines: configure at least one line")
	}

	lines := make([]Line, 0, len(raw))
	users := map[string]bool{}
	bots := map[string]bool{}

	for i, line := range raw {
		field := "lines[" + strconv.Itoa(i) + "]"

		if !sipUserPattern.MatchString(line.Username) {
			v.failf("%s.username: must be 1-64 characters of letters, digits, or ._~+-", field)
		}

		if !botIDPattern.MatchString(line.BotID) {
			v.failf("%s.botId: must be a Crumble bot ID", field)
		}

		if len(line.DisplayName) > maxDisplayName {
			v.failf("%s.displayName: at most 64 characters", field)
		}

		if users[line.Username] {
			v.failf("%s.username: %q is used by another line", field, line.Username)
		}

		// One bot chat handles one turn at a time, so a bot answers at most one line.
		if bots[line.BotID] {
			v.failf("%s.botId: %q already answers another line", field, line.BotID)
		}

		users[line.Username] = true
		bots[line.BotID] = true

		lines = append(lines, Line{
			Username:    line.Username,
			Password:    v.secret(field+".passwordEnv", line.PasswordEnv, 1),
			DisplayName: line.DisplayName,
			BotID:       line.BotID,
			Voice:       line.Voice,
		})
	}

	return lines
}

func (v *validator) speech(raw fileSpeech) Speech {
	if raw.Provider != "openai" {
		v.failf("speech.provider: only \"openai\" is supported")
	}

	required := v.speechMode(raw)

	for field, model := range map[string]string{
		"speech.transcriptionModel": raw.TranscriptionModel,
		"speech.synthesisModel":     raw.SynthesisModel,
		"speech.liveModel":          raw.LiveModel,
		"speech.realtimeModel":      raw.RealtimeModel,
	} {
		if _, needed := required[field]; !needed && model != "" && !modelPattern.MatchString(model) {
			v.failf("%s: letters, digits, or ._:- only", field)
		}
	}

	for field, model := range required {
		if !modelPattern.MatchString(model) {
			v.failf("%s: required, letters, digits, or ._:-", field)
		}
	}

	if raw.Language != "" && !languagePattern.MatchString(raw.Language) {
		v.failf("speech.language: must be an ISO-639-1 code such as \"de\" when set")
	}

	if len(raw.VoiceInstructions) > maxInstructions {
		v.failf("speech.voiceInstructions: at most 4096 characters")
	}

	// GPT-Live accepts 16,384 tokens of instructions; characters are a safe upper bound.
	if len(raw.LiveInstructions) > maxLiveInstructions {
		v.failf("speech.liveInstructions: at most %d characters", maxLiveInstructions)
	}

	return Speech{
		Mode:               raw.Mode,
		Provider:           raw.Provider,
		APIKey:             v.secret("speech.apiKeyEnv", raw.APIKeyEnv, 1),
		BaseURL:            v.serviceURL("speech.baseUrl", raw.BaseURL),
		TranscriptionModel: raw.TranscriptionModel,
		Language:           raw.Language,
		SynthesisModel:     raw.SynthesisModel,
		Voice:              raw.Voice,
		VoiceInstructions:  raw.VoiceInstructions,
		LiveModel:          raw.LiveModel,
		LiveInstructions:   raw.LiveInstructions,
		RealtimeModel:      raw.RealtimeModel,
	}
}

// speechMode returns the model fields the chosen mode needs. Only the active
// mode's fields are required, so switching modes changes one field.
func (v *validator) speechMode(raw fileSpeech) map[string]string {
	required := map[string]string{}

	switch raw.Mode {
	case ModePipeline:
		required["speech.transcriptionModel"] = raw.TranscriptionModel
		required["speech.synthesisModel"] = raw.SynthesisModel
	case ModeRealtime:
		required["speech.realtimeModel"] = raw.RealtimeModel
		required["speech.transcriptionModel"] = raw.TranscriptionModel
	case ModeLive:
		required["speech.liveModel"] = raw.LiveModel

		if strings.TrimSpace(raw.LiveInstructions) == "" {
			v.failf("speech.liveInstructions: required in live mode")
		}
	default:
		v.failf("speech.mode: must be %q, %q, or %q", ModePipeline, ModeRealtime, ModeLive)
	}

	return required
}

func (v *validator) control(raw fileControl) Control {
	listen, err := netip.ParseAddrPort(raw.Listen)
	if err != nil {
		v.failf("control.listen: must be ip:port: %v", err)
	} else if !listen.Addr().IsLoopback() && !listen.Addr().IsPrivate() {
		v.failf("control.listen: must be a loopback or private address; the API has no TLS")
	}

	return Control{
		Listen: listen,
		Token:  v.secret("control.tokenEnv", raw.TokenEnv, minSecretLength),
	}
}

func (v *validator) call(raw fileCall) Call {
	result := Call{
		MaxDuration:       time.Duration(raw.MaxDuration),
		TurnTimeout:       time.Duration(raw.TurnTimeout),
		MaxConcurrent:     raw.MaxConcurrent,
		CallNote:          raw.CallNote,
		HoldingPhrase:     raw.HoldingPhrase,
		HoldingDelay:      time.Duration(raw.HoldingDelay),
		TimeoutPhrase:     raw.TimeoutPhrase,
		FailurePhrase:     raw.FailurePhrase,
		AnswerGreeting:    raw.AnswerGreeting,
		StrangerRecording: raw.StrangerRecording,
		UnansweredAfter:   time.Duration(raw.UnansweredAfter),
	}

	if result.MaxDuration < time.Minute || result.MaxDuration > 4*time.Hour {
		v.failf("call.maxDuration: must be between 1m and 4h")
	}

	if result.TurnTimeout < 5*time.Second || result.TurnTimeout > 30*time.Minute {
		v.failf("call.turnTimeout: must be between 5s and 30m")
	}

	if result.HoldingDelay < 500*time.Millisecond || result.HoldingDelay >= result.TurnTimeout {
		v.failf("call.holdingDelay: must be at least 500ms and shorter than call.turnTimeout")
	}

	if result.UnansweredAfter < 5*time.Second || result.UnansweredAfter > 3*time.Minute {
		v.failf("call.unansweredAfter: must be between 5s and 3m")
	}

	if result.MaxConcurrent < 1 || result.MaxConcurrent > maxConcurrent {
		v.failf("call.maxConcurrent: must be 1-64")
	}

	v.phrases(result)

	if len(result.CallNote) > maxCallNoteChars {
		v.failf("call.callNote: at most %d characters", maxCallNoteChars)
	}

	return result
}

// phrases checks what the gateway says during a call.
func (v *validator) phrases(result Call) {
	if strings.TrimSpace(result.StrangerRecording) != result.StrangerRecording ||
		(result.StrangerRecording != "" && !strings.EqualFold(filepath.Ext(result.StrangerRecording), ".wav")) {
		v.failf("call.strangerRecording: must be the path of a .wav file, or empty to reject strangers")
	}

	if len(result.AnswerGreeting) > maxPhraseChars {
		v.failf("call.answerGreeting: at most %d characters", maxPhraseChars)
	}

	if leftover := strings.ReplaceAll(result.AnswerGreeting, NamePlaceholder, ""); strings.ContainsAny(leftover, "{}") {
		v.failf("call.answerGreeting: the only placeholder is %s", NamePlaceholder)
	}

	for field, phrase := range map[string]string{
		"call.holdingPhrase": result.HoldingPhrase,
		"call.timeoutPhrase": result.TimeoutPhrase,
		"call.failurePhrase": result.FailurePhrase,
	} {
		if phrase == "" || len(phrase) > maxPhraseChars {
			v.failf("%s: required, at most %d characters", field, maxPhraseChars)
		}
	}
}

// IsOwner reports whether a caller ID belongs to the owner. The match is exact.
func (o Owner) IsOwner(number string) bool {
	return slices.Contains(o.Numbers, number)
}
