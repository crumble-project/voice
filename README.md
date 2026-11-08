# crumble-voice

The voice gateway for Crumble. It registers SIP lines on a telephone system, connects each line to a bot chat, and turns the call into owner messages and spoken replies. The owner can call a bot, and a bot can call the owner.

## How a call works

1. The gateway registers each configured line with the registrar, for example `ada@192.168.1.1`.
2. On an incoming call, the gateway checks three things: the INVITE came from the registrar's address, the called line exists, and the caller ID is one of the owner's numbers. A call that fails a check is rejected with `403` or `404`, except a caller who isn't the owner: with `call.strangerRecording` set, they hear that recording and the gateway hangs up. Strangers never reach a bot.
3. The caller's words go to the line's bot chat as an owner message, through `POST /v1/agents/{id}/messages`.
4. The gateway follows the reply on `GET /v1/agents/{id}/events?partial=true` and speaks each sentence as soon as the bot has written it, with Markdown removed. If the caller starts talking, speech stops.
5. Anything the caller says while the bot is still answering is sent as one message after the current turn finishes.

`speech.mode` chooses who hears and speaks:

| Mode | Hearing | Speaking | While the bot thinks |
| --- | --- | --- | --- |
| `pipeline` | OpenAI Realtime transcription | OpenAI text-to-speech, one sentence ahead | `call.holdingPhrase` after `call.holdingDelay` |
| `realtime` | One Realtime API session (`gpt-realtime-2.1` or `-mini`) | The same session, told to say each sentence word for word | `call.holdingPhrase` after `call.holdingDelay` |
| `live` | GPT-Live | GPT-Live speaks the bot's sentences | GPT-Live acknowledges by itself |

In every mode the bot is the only one that answers:

- **Realtime** runs with `create_response: false`, so the model never replies on its own; it only speaks when the gateway asks. Each sentence is an out-of-band reply instructed to say the text word for word. OpenAI doesn't guarantee instructions are followed, so it may still reword occasionally. When the caller talks, the gateway cancels the current reply.
- **GPT-Live** is told to acknowledge and to repeat the bot's text, but it may rephrase that text (OpenAI documents this for `session.commentary.append`). Instructions can't fully stop it from adding words of its own.

Each call is part of the bot chat, so the transcript shows the whole conversation.

## Run

```sh
cp crumble-voice.example.json crumble-voice.json   # then edit it
export CRUMBLE_PASSWORD=... SIP_PASSWORD_ADA=... OPENAI_API_KEY=...
export CRUMBLE_VOICE_TOKEN="$(openssl rand -hex 32)"
make build
./bin/crumble-voice -config crumble-voice.json
```

The gateway refuses to start when the config has an unknown field, a value is out of range, a secret is missing, or a line's bot is missing or disabled.

## Configuration

The file holds no secrets. Every `...Env` field names the environment variable that holds the secret.

| Field | Meaning |
| --- | --- |
| `crumble.baseUrl` | Crumble server. Plain `http` is accepted only to a loopback or private IP address. |
| `crumble.email`, `crumble.passwordEnv` | The owner's sign-in. The gateway acts as the owner. |
| `sip.registrar` | Registrar `host[:port]`. Incoming calls are only accepted from the addresses it resolves to. |
| `sip.bindHost`, `sip.bindPort`, `sip.externalHost` | Local SIP address, and the address announced in SDP when it differs. |
| `sip.rtpPortStart`, `sip.rtpPortEnd` | RTP port range. Open it in the firewall. |
| `owner.numbers` | Caller IDs that belong to the owner, matched exactly. These are the only numbers that can call in, and the only numbers the gateway calls. |
| `lines[]` | One SIP account per bot: `username`, `passwordEnv`, `displayName`, `botId`, and an optional `voice`. A bot answers at most one line. |
| `speech.mode` | `pipeline`, `realtime`, or `live`. Only the active mode's model fields are required, so switching changes one field. |
| `speech.transcriptionModel` | Pipeline and realtime mode. |
| `speech.synthesisModel` | Pipeline mode. |
| `speech.realtimeModel` | Realtime mode: `gpt-realtime-2.1-mini` (cheaper) or `gpt-realtime-2.1`. |
| `speech.voiceInstructions` | How to speak, in pipeline and realtime mode. The words always come from the bot. |
| `speech.liveModel`, `speech.liveInstructions` | Live mode, for example `gpt-live-1`. The instructions keep GPT-Live to acknowledgements and the bot's words. |
| `speech.voice`, `speech.language` | Default voice for lines without their own, and an optional language (ISO-639-1). |
| `control.listen`, `control.tokenEnv` | Local control API. Only loopback or private addresses are accepted. The token must be at least 32 characters. |
| `call.answerGreeting` | What a bot says when it answers, such as `Hi, this is {name}.`; `{name}` becomes the bot's name. Leave it empty to answer silently. |
| `call.strangerRecording` | A WAV file played to callers who aren't the owner, instead of rejecting them. A relative path is relative to the config file. Empty rejects them. |
| `call.callNote` | Text added before the first message of each call, so the bot knows its reply is spoken. |
| `call.holdingPhrase`, `call.holdingDelay` | What the gateway says when the bot takes longer than the delay. |
| `call.turnTimeout`, `call.timeoutPhrase` | When the gateway stops waiting for a turn. The turn keeps running, and its reply stays in the bot chat. |
| `call.failurePhrase` | What the gateway says when a turn fails. |
| `call.maxDuration`, `call.maxConcurrent`, `call.unansweredAfter` | Call limits. |

## Stranger recording

The recording must be WAV, 8 kHz, mono, 16-bit, and at most two minutes long. The gateway checks it at startup and refuses to start otherwise. Convert any recording on macOS with:

```sh
afconvert -f WAVE -d LEI16@8000 -c 1 input.m4a stranger.wav
```

Or have the Mac speak it:

```sh
say -o message.aiff "Hi, you reached Crumble. This line only talks to its owner."
afconvert -f WAVE -d LEI16@8000 -c 1 message.aiff stranger.wav
```

Stranger calls have their own limit (`call.maxConcurrent`), so they can't keep the owner from reaching a bot.

## Voices

Set `speech.voice` for every line, or `voice` on a line to give one bot its own voice. The gateway checks the name against the voices OpenAI documents and refuses to start on a typo.

- Pipeline mode: `alloy`, `ash`, `ballad`, `cedar`, `coral`, `echo`, `fable`, `marin`, `nova`, `onyx`, `sage`, `shimmer`, `verse`. OpenAI recommends `marin` and `cedar`.
- Realtime mode: `alloy`, `ash`, `ballad`, `cedar`, `coral`, `echo`, `marin`, `sage`, `shimmer`, `verse`. No `fable`, `nova`, or `onyx`.
- Live mode: all pipeline voices, plus `beacon`, `bossa`, `brise`, `cinder`, `delta`, `flitz`, `gleam`, `harema`, `juni`, `meridian`, `nira`, `noeul`, `nuri`, `quartz`, `ripple`, `shida`, `sillage`, `stone`, `tempo`, `vesper`, `willow`.

Listen to the voices on [openai.fm](https://www.openai.fm) before choosing. A voice change takes effect when the gateway restarts.

## Place a call

```sh
curl -X POST http://127.0.0.1:8790/v1/calls \
  -H "Authorization: Bearer $CRUMBLE_VOICE_TOKEN" -H "Content-Type: application/json" \
  -d '{"botId":"...","to":"**610","greeting":"Hi, it is Ada. Your build finished."}'
```

The request returns `201 {"callId": ...}` once the call is answered. The bot speaks the greeting, then the conversation continues like an incoming call. Other answers are `403` (the number is not the owner's), `404` (no line for the bot), `409` (the bot is already on a call), and `504` (nobody answered).

## Develop

```sh
make check   # go mod tidy -diff, golangci-lint (all linters, see .golangci.yml), go test -race, govulncheck
```

The tests need no network access and no API keys. `internal/sipline` runs real SIP and RTP over loopback against a fake registrar, on ports 25060-25080 and 26000-26100.

## Timing

Each answered turn logs a `turn timing` line. `transcribe_ms` counts from the caller going quiet to the text arriving. The rest count from sending the message, and `-1` means the step wasn't seen:

| Field | Up to |
| --- | --- |
| `accepted_ms` | Crumble queued the message. |
| `running_ms` | The bot started working. A large gap after `accepted_ms` means the bot was busy with something else. |
| `first_text_ms` | The bot's first words arrived. The gap after `running_ms` is Crumble preparing the turn plus the model's first token. |
| `first_sentence_ms` | The first complete sentence went to the voice. |
| `reply_ms` | The turn ended. |

In pipeline mode, a `speaking` line for each sentence gives `waited_ms`: time from handing the sentence to the voice until its first audio. That covers text-to-speech, plus any sentence still playing ahead of it.

## Known limits

These need changes in the Crumble server:

- **The owner's full session.** The server has no service credential, so the gateway signs in with the owner's password and holds a full session.
- **Phone messages look typed.** The public API can't set a message origin, so phone messages show as typed by the owner. `call.callNote` is the workaround.
- **No third-party calls.** Calls only reach the owner's numbers. Before a bot can call anyone else, Crumble needs a way to keep a third party's words apart from the owner's messages.
- **Streaming needs a current server.** It needs a Crumble server with `partial=true` on the events stream. An older server ignores the parameter, and the gateway then speaks the whole reply when the turn ends.

Streaming:

- **Text before a tool call can lose its end.** When the bot writes, calls a tool, and writes again, the stream may never show the end of the first piece. The gateway drops that unseen end instead of saying it out of order. The full text stays in the bot chat.

On the speech side:

- **The pipeline voice must move before 2027-01-06.** OpenAI is shutting down `gpt-4o-mini-tts` and `tts-1` on that date, and names `gpt-realtime-2.1-mini` as their replacement. Realtime and live mode don't use them.
- **Realtime's 8 kHz rate isn't documented.** The Realtime docs name the μ-law format but no sample rate for it. The gateway assumes 8 kHz, the telephone standard. If the voice sounds too fast or too slow in realtime mode, this is the cause.
- **GPT-Live has gaps in the docs.** OpenAI doesn't document an event for the caller starting to speak, the end of a turn, or what happens when a second commentary arrives while the first is still being spoken. The gateway treats the first transcript of new speech as an interruption, and the model starting to answer, or 800 ms of quiet, as the end of the caller's turn.
- **Caller IDs can be forged.** Anyone who can send traffic from the registrar's address can claim any caller ID. On a home network, that's the telephone system itself.
