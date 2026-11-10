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
	Speech  Speech
	Call    Call
}

// Crumble is the server the gateway connects to with its voice gateway token.
type Crumble struct {
	BaseURL *url.URL
	Token   string
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
	// Accounts are the SIP accounts the gateway can register; the server decides which become lines.
	Accounts []Account
}

// Account is one SIP account on the registrar.
type Account struct {
	Username string
	Password string
	// Voice is the account's voice, speech.voice unless the account sets its own.
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
	// TurnDetection is "semantic" or "silence"; live mode has no turn detection to configure.
	TurnDetection string
	// Eagerness is how soon semantic turn detection ends a turn that sounds unfinished.
	Eagerness string
	// SilenceDuration ends a turn in silence-based turn detection.
	SilenceDuration time.Duration
}

// NamePlaceholder in call.answerGreeting becomes the bot's name.
const NamePlaceholder = "{name}"

// Speech modes.
const (
	ModePipeline = "pipeline"
	ModeRealtime = "realtime"
	ModeLive     = "live"
)

// Turn detections.
const (
	TurnSemantic = "semantic"
	TurnSilence  = "silence"
)

// Turn detection defaults for files that leave the fields out.
const (
	// Medium caps the wait after unfinished-sounding words at 4s; low waits up to 8s of dead air.
	defaultEagerness       = "medium"
	defaultSilenceDuration = 600 * time.Millisecond
	minSilenceDuration     = 200 * time.Millisecond
	maxSilenceDuration     = 5 * time.Second
)

// Call bounds each call.
type Call struct {
	MaxDuration     time.Duration
	TurnTimeout     time.Duration
	MaxConcurrent   int
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
	// AllowOutside lets the server have the gateway dial numbers that are not the owner's,
	// within the outside number rules. Off unless the file switches it on.
	AllowOutside bool
}

type fileConfig struct {
	Crumble fileCrumble `json:"crumble"`
	SIP     fileSIP     `json:"sip"`
	Speech  fileSpeech  `json:"speech"`
	Call    fileCall    `json:"call"`
}

type fileCrumble struct {
	BaseURL  string `json:"baseUrl"`
	TokenEnv string `json:"tokenEnv"`
}

type fileSIP struct {
	Registrar      string        `json:"registrar"`
	Transport      string        `json:"transport"`
	BindHost       string        `json:"bindHost"`
	BindPort       int           `json:"bindPort"`
	ExternalHost   string        `json:"externalHost"`
	RTPPortStart   int           `json:"rtpPortStart"`
	RTPPortEnd     int           `json:"rtpPortEnd"`
	RegisterExpiry duration      `json:"registerExpiry"`
	Accounts       []fileAccount `json:"accounts"`
}

type fileAccount struct {
	Username    string `json:"username"`
	PasswordEnv string `json:"passwordEnv"`
	Voice       string `json:"voice,omitempty"`
}

type fileSpeech struct {
	Mode               string   `json:"mode"`
	Provider           string   `json:"provider"`
	APIKeyEnv          string   `json:"apiKeyEnv"`
	BaseURL            string   `json:"baseUrl"`
	TranscriptionModel string   `json:"transcriptionModel"`
	Language           string   `json:"language"`
	SynthesisModel     string   `json:"synthesisModel"`
	Voice              string   `json:"voice"`
	VoiceInstructions  string   `json:"voiceInstructions"`
	LiveModel          string   `json:"liveModel"`
	LiveInstructions   string   `json:"liveInstructions"`
	RealtimeModel      string   `json:"realtimeModel"`
	TurnDetection      string   `json:"turnDetection"`
	Eagerness          string   `json:"eagerness"`
	SilenceDuration    duration `json:"silenceDuration"`
}

type fileCall struct {
	MaxDuration    duration `json:"maxDuration"`
	TurnTimeout    duration `json:"turnTimeout"`
	MaxConcurrent  int      `json:"maxConcurrent"`
	HoldingPhrase  string   `json:"holdingPhrase"`
	HoldingDelay   duration `json:"holdingDelay"`
	TimeoutPhrase  string   `json:"timeoutPhrase"`
	FailurePhrase  string   `json:"failurePhrase"`
	AnswerGreeting string   `json:"answerGreeting"`
	// A relative path is relative to the configuration file.
	StrangerRecording string   `json:"strangerRecording"`
	UnansweredAfter   duration `json:"unansweredAfter"`
	AllowOutside      bool     `json:"allowOutside"`
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
	maxPhraseChars      = 300
	maxInstructions     = 4096
	maxLiveInstructions = 16384
	maxConcurrent       = 64
)

var (
	// The server's own rule for line usernames, which are path segments in its line routes.
	sipUserPattern = regexp.MustCompile(`^[A-Za-z0-9._~+-]{1,64}$`)
	// A voice gateway token is the account ID, a dot, and 32 random bytes in base64url.
	tokenPattern    = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}\.[A-Za-z0-9_-]{43}$`)
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
		Speech:  v.speech(raw.Speech),
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

// voices checks speech.voice and each account's voice against the mode's voices,
// and gives accounts without their own voice the default.
func (v *validator) voices(config *Config) {
	allowed := Voices(config.Speech.Mode)

	check := func(field, voice string) {
		if !slices.Contains(allowed, voice) {
			v.failf("%s: %q is not a %s voice; choose one of %s", field, voice, config.Speech.Mode, strings.Join(allowed, ", "))
		}
	}

	check("speech.voice", config.Speech.Voice)

	accounts := config.SIP.Accounts
	for i := range accounts {
		if accounts[i].Voice == "" {
			accounts[i].Voice = config.Speech.Voice

			continue
		}

		check("sip.accounts["+strconv.Itoa(i)+"].voice", accounts[i].Voice)
	}
}

func (v *validator) secret(field, name string) string {
	if !envNamePattern.MatchString(name) {
		v.failf("%s: %q is not an environment variable name", field, name)

		return ""
	}

	value, ok := v.env(name)
	if !ok || value == "" {
		v.failf("%s: environment variable %s is not set", field, name)

		return ""
	}

	return value
}

func (v *validator) crumble(raw fileCrumble) Crumble {
	base := v.serviceURL("crumble.baseUrl", raw.BaseURL)

	token := v.secret("crumble.tokenEnv", raw.TokenEnv)
	if token != "" && !tokenPattern.MatchString(token) {
		v.failf("crumble.tokenEnv: environment variable %s does not hold a voice gateway token; "+
			"issue one in Settings > Voice gateway", raw.TokenEnv)
	}

	return Crumble{BaseURL: base, Token: token}
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
	result.Accounts = v.accounts(raw.Accounts)

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

func (v *validator) accounts(raw []fileAccount) []Account {
	if len(raw) == 0 {
		v.failf("sip.accounts: configure at least one SIP account")
	}

	accounts := make([]Account, 0, len(raw))
	users := map[string]bool{}

	for i, account := range raw {
		field := "sip.accounts[" + strconv.Itoa(i) + "]"

		if !sipUserPattern.MatchString(account.Username) || account.Username == "." || account.Username == ".." {
			v.failf("%s.username: must be 1-64 characters of letters, digits, or ._~+-, and not . or ..", field)
		}

		if users[account.Username] {
			v.failf("%s.username: %q is listed twice", field, account.Username)
		}

		users[account.Username] = true

		accounts = append(accounts, Account{
			Username: account.Username,
			Password: v.secret(field+".passwordEnv", account.PasswordEnv),
			Voice:    account.Voice,
		})
	}

	return accounts
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

	turns, eagerness, silence := v.turnDetection(raw)

	return Speech{
		Mode:               raw.Mode,
		Provider:           raw.Provider,
		APIKey:             v.secret("speech.apiKeyEnv", raw.APIKeyEnv),
		BaseURL:            v.serviceURL("speech.baseUrl", raw.BaseURL),
		TranscriptionModel: raw.TranscriptionModel,
		Language:           raw.Language,
		SynthesisModel:     raw.SynthesisModel,
		Voice:              raw.Voice,
		VoiceInstructions:  raw.VoiceInstructions,
		LiveModel:          raw.LiveModel,
		LiveInstructions:   raw.LiveInstructions,
		RealtimeModel:      raw.RealtimeModel,
		TurnDetection:      turns,
		Eagerness:          eagerness,
		SilenceDuration:    silence,
	}
}

// turnDetection fills in the defaults and checks the values in every mode, so
// switching speech.mode or speech.turnDetection changes one field.
func (v *validator) turnDetection(raw fileSpeech) (string, string, time.Duration) {
	turns, eagerness, silence := raw.TurnDetection, raw.Eagerness, time.Duration(raw.SilenceDuration)

	if turns == "" {
		turns = TurnSemantic
	}

	if eagerness == "" {
		eagerness = defaultEagerness
	}

	if silence == 0 {
		silence = defaultSilenceDuration
	}

	if turns != TurnSemantic && turns != TurnSilence {
		v.failf("speech.turnDetection: must be %q or %q", TurnSemantic, TurnSilence)
	}

	if !slices.Contains([]string{"low", "medium", "high", "auto"}, eagerness) {
		v.failf("speech.eagerness: must be low, medium, high, or auto")
	}

	if silence < minSilenceDuration || silence > maxSilenceDuration || silence%time.Millisecond != 0 {
		v.failf("speech.silenceDuration: must be whole milliseconds between %v and %v", minSilenceDuration, maxSilenceDuration)
	}

	return turns, eagerness, silence
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

func (v *validator) call(raw fileCall) Call {
	result := Call{
		MaxDuration:       time.Duration(raw.MaxDuration),
		TurnTimeout:       time.Duration(raw.TurnTimeout),
		MaxConcurrent:     raw.MaxConcurrent,
		HoldingPhrase:     raw.HoldingPhrase,
		HoldingDelay:      time.Duration(raw.HoldingDelay),
		TimeoutPhrase:     raw.TimeoutPhrase,
		FailurePhrase:     raw.FailurePhrase,
		AnswerGreeting:    raw.AnswerGreeting,
		StrangerRecording: raw.StrangerRecording,
		UnansweredAfter:   time.Duration(raw.UnansweredAfter),
		AllowOutside:      raw.AllowOutside,
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
