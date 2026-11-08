package call

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestLastSentenceEnd(t *testing.T) {
	t.Parallel()

	cases := []struct {
		text string
		from int
		want int
	}{
		{"Hello there. How", 0, len("Hello there.")},
		{"Hello there. How are you? Fine", 0, len("Hello there. How are you?")},
		{"Hello there. How are you? Fine", len("Hello there."), len("Hello there. How are you?")},
		{"No end yet", 0, 0},
		{"Version 3.5 is out", 0, 0},
		{"Run this:\n```go\nfmt.Println(1.5)\n", 0, len("Run this:\n")},
		{"Line one\nLine two", 0, len("Line one\n")},
	}

	for _, test := range cases {
		if got := lastSentenceEnd(test.text, test.from); got != test.want {
			t.Errorf("lastSentenceEnd(%q, %d) = %d, want %d", test.text, test.from, got, test.want)
		}
	}
}

func TestStreamSpeakerKeepsSpeechWithinOneRequest(t *testing.T) {
	t.Parallel()

	var said []string

	speaker := &streamSpeaker{say: func(text string) { said = append(said, text) }}
	speaker.partial(0, strings.Repeat("word ", 1000)+"end.\n")

	if len(said) < 2 {
		t.Fatalf("said %d pieces, want the long line split", len(said))
	}

	for _, text := range said {
		if n := utf8.RuneCountInString(text); n > synthesisChunk {
			t.Fatalf("said %d characters at once, limit %d", n, synthesisChunk)
		}
	}
}
