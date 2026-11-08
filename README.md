# crumble-voice

The voice gateway for Crumble. It registers SIP lines on a telephone system and connects each line to a bot chat: the owner can call a bot, and a bot can call the owner. Calls from other numbers never reach a bot.

## Run

```sh
cp crumble-voice.example.json crumble-voice.json   # then edit it
export CRUMBLE_PASSWORD=... SIP_PASSWORD_ADA=... OPENAI_API_KEY=...
export CRUMBLE_VOICE_TOKEN="$(openssl rand -hex 32)"
make build
./bin/crumble-voice -config crumble-voice.json
```

The config holds no secrets: each `...Env` field names an environment variable. `crumble-voice.example.json` shows every field. The gateway refuses to start on an invalid config.

`speech.mode` picks the speech backend: `pipeline` (transcription plus text-to-speech), `realtime` (one Realtime API session), or `live` (GPT-Live). In every mode the words come from the bot.

To play a recording to strangers, set `call.strangerRecording` to a WAV file (8 kHz, mono, 16-bit, at most two minutes):

```sh
afconvert -f WAVE -d LEI16@8000 -c 1 input.m4a stranger.wav
```

## Place a call

```sh
curl -X POST http://127.0.0.1:8790/v1/calls \
  -H "Authorization: Bearer $CRUMBLE_VOICE_TOKEN" -H "Content-Type: application/json" \
  -d '{"botId":"...","to":"**610","greeting":"Hi, it is Ada. Your build finished."}'
```

Returns `201 {"callId": ...}` once answered, or `403` (not the owner's number), `404` (no line for the bot), `409` (bot already on a call), `504` (no answer).

## Develop

```sh
make check   # tidy, golangci-lint, go test -race, govulncheck
```

Tests need no network or API keys. `internal/sipline` uses loopback ports 25060-25080 and 26000-26100.

## Known limits

- The gateway signs in with the owner's password and holds a full session.
- Caller IDs can be forged by anyone who can send traffic from the registrar's address.
- Phone messages show as typed by the owner; `call.callNote` tells the bot its reply is spoken.
- Streaming needs a Crumble server that supports `partial=true` on the events stream.
- OpenAI shuts down `gpt-4o-mini-tts` on 2027-01-06, so pipeline mode needs a new synthesis model before then.

## License

Licensed under the [GNU Affero General Public License v3.0](LICENSE) (`AGPL-3.0-only`).
