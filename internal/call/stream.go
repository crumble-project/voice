package call

import (
	"strings"
	"time"
)

// streamSpeaker speaks a reply sentence by sentence while the bot writes it.
type streamSpeaker struct {
	say func(text string)

	generation int
	text       string
	// spoken is how much of text was handed to say.
	spoken  int
	started time.Time
}

// partial receives everything the current generation wrote so far. A new
// generation drops the previous one's unspoken tail: the stream never shows it,
// and speaking it later would put it out of order.
func (s *streamSpeaker) partial(generation int, text string) {
	if generation != s.generation {
		s.generation, s.spoken = generation, 0
	}

	s.text = text

	end := lastSentenceEnd(text, s.spoken)
	if end <= s.spoken {
		return
	}

	chunk := Speakable(text[s.spoken:end])
	s.spoken = end

	if chunk == "" {
		return
	}

	if s.started.IsZero() {
		s.started = time.Now()
	}

	for _, piece := range Chunks(chunk, synthesisChunk) {
		s.say(piece)
	}
}

// remainder returns what is left to say once the turn ended with reply parts.
func (s *streamSpeaker) remainder(parts []string) string {
	if s.started.IsZero() {
		return strings.Join(parts, "\n\n")
	}

	last := parts[len(parts)-1]
	spoken := strings.TrimSpace(s.text[:s.spoken])

	if strings.HasPrefix(last, spoken) {
		return last[len(spoken):]
	}

	// The final answer is a piece of text the stream never showed.
	return last
}

// lastSentenceEnd returns the end of the last complete sentence in text after from,
// or from when there is none. A sentence never ends inside an open code block.
func lastSentenceEnd(text string, from int) int {
	end := from

	for i := from; i < len(text); i++ {
		switch text[i] {
		case '\n':
			end = i + 1
		case '.', '!', '?', ':', ';':
			if i+1 < len(text) && (text[i+1] == ' ' || text[i+1] == '\n') {
				end = i + 1
			}
		}
	}

	if strings.Count(text[:end], "```")%2 == 1 {
		end = strings.LastIndex(text[:end], "```")
	}

	return max(end, from)
}
