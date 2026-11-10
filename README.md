# crumble-voice

The voice gateway for Crumble. It registers SIP lines on a telephone system and connects each line to a bot chat: the owner can call a bot, and a bot can call the owner. Calls from other numbers never reach a bot.

## Run

1. In Crumble, open **Settings > Voice gateway**: add the owner's numbers and the lines (a bot and the SIP username it answers on), enable the voice gateway, and issue a voice gateway token. The token is shown once.
2. Start the gateway:

```sh
cp crumble-voice.example.json crumble-voice.json   # then edit it
export CRUMBLE_VOICE_GATEWAY_TOKEN=... SIP_PASSWORD_ADA=... OPENAI_API_KEY=...
make build
./bin/crumble-voice -config crumble-voice.json
```

The config holds no secrets: each `...Env` field names an environment variable. `crumble-voice.example.json` shows every field. The gateway refuses to start on an invalid config.

The server decides which lines exist and which numbers belong to the owner, and sends changes while the gateway runs; the gateway applies them once no call is active. The file keeps what belongs to the machine the gateway runs on:

- `crumble`: the server's `baseUrl` and `tokenEnv`, the variable that holds the voice gateway token.
- `sip`: the registrar and network settings, and `accounts`, the SIP accounts the gateway may register, each with a `username`, a `passwordEnv`, and an optional `voice`. A line on the server registers only if an account here has its username; the gateway logs the others.
- `speech` and `call`: speech provider, and call limits and phrases.

The gateway reports each line's registration to the server, which shows it in **Settings > Voice gateway**, including lines that have no matching account in `sip.accounts`. A line the registrar refuses (wrong password) shows the registrar's answer there and is retried every 10 minutes; the other lines keep running.

If the server refuses the token (revoked, or replaced by a newer one), the gateway stops and logs that a new voice gateway token is needed. If another gateway connects with the same token, the older one stops.

### Upgrading from a config with `owner` and `lines`

The gateway no longer reads `crumble.email`, `crumble.passwordEnv`, `owner`, `lines`, `call.callNote`, or `control`, and an old file fails to start with an unknown-field error. The local call API (`POST /v1/calls`) is gone: bots place calls through the server, so delete the `control` section and the `CRUMBLE_VOICE_TOKEN` variable. Re-enter the lines and the owner's numbers in **Settings > Voice gateway**, move each line's `username`, `passwordEnv`, and `voice` to `sip.accounts`, and set `crumble.tokenEnv`. The server now tells the bot that a message was spoken, so `callNote` has no replacement.

`speech.mode` picks the speech backend: `pipeline` (transcription plus text-to-speech), `realtime` (one Realtime API session), or `live` (GPT-Live). In every mode the words come from the bot.

`speech.turnDetection` decides when the caller has finished speaking, in `pipeline` and `realtime` mode. `semantic` (the default) uses OpenAI's semantic turn detection: it ends the turn right after a sentence that sounds finished and waits when the words trail off. `speech.eagerness` caps that wait: `low` up to 8 seconds, `medium` (the default, same as `auto`) up to 4, `high` up to 2. `silence` ends the turn after `speech.silenceDuration` of silence (default `600ms`, the gateway's old behavior). If the gateway sends half a sentence to the bot, set `eagerness` to `low`, or with `silence` raise `silenceDuration`. If it waits too long before answering, set `eagerness` to `high`, or lower `silenceDuration`. `live` mode ignores these fields: GPT-Live has no turn detection setting.

To play a recording to strangers, set `call.strangerRecording` to a WAV file (8 kHz, mono, 16-bit, at most two minutes):

```sh
afconvert -f WAVE -d LEI16@8000 -c 1 input.m4a stranger.wav
```

## Calls from a bot

A bot calls the owner only when its line has **Can call you** switched on in **Settings > Voice gateway**. The bot then gets a tool to place the call, and the server asks the gateway to dial the first of the owner's numbers from the bot's line. The gateway dials only a number that is among the owner's numbers in the server's current config, rings for at most `call.unansweredAfter`, says the bot's greeting when the owner answers, and reports the outcome to the server. If the server cancels the call while it rings, the gateway hangs up.

To test a call, ask the bot in its chat to call you.

## Calls in the bot chat

Every call with a bot, from the owner or from the bot, shows in the bot's chat: "Call started" when it connects, with the bot's greeting on a call the bot placed, and "Call ended" with the reason. The gateway reports the start before it speaks the greeting, and the end when the owner hangs up, the call reaches `call.maxDuration`, the call fails, or the gateway stops. When a call ends, the server tells the bot, so it can do what it promised on the call, such as calling back.

A bot can end the call itself, for example when the owner says goodbye. The gateway says the bot's reply, waits until no speech went out for 3 seconds, and hangs up.

If the server has ended a call, for example because the line went to another bot, the gateway hangs up. After a reconnect, the gateway confirms its running calls and sends again any end report the server did not get. A server without call state answers the start report with 404: the gateway logs that once and the call goes on, without the chat entries.

## Develop

```sh
make check   # tidy, golangci-lint, go test -race, govulncheck
```

Tests need no network or API keys. `internal/sipline` uses loopback ports 25060-25080 and 26000-26100; `internal/bridge` uses 25160-25170 and 26200-26300.

## Known limits

- Caller IDs can be forged by anyone who can send traffic from the registrar's address.
- One gateway per token: a second gateway with the same token takes over, and the first one stops.
- A bot's call dials only the first of the owner's numbers, with no fallback to the next one.
- There is no local way to place a call; a test call is a chat message asking the bot to call.
- The gateway needs a Crumble server with the voice gateway routes under `/v1/voice/`.
- OpenAI shuts down `gpt-4o-mini-tts` on 2027-01-06, so pipeline mode needs a new synthesis model before then.

## License

Licensed under the [GNU Affero General Public License v3.0](LICENSE) (`AGPL-3.0-only`).
