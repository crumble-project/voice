package call

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	codeBlock   = regexp.MustCompile("(?s)```.*?```")
	inlineCode  = regexp.MustCompile("`([^`]*)`")
	image       = regexp.MustCompile(`!\[([^\]]*)\]\([^)]*\)`)
	link        = regexp.MustCompile(`\[([^\]]+)\]\([^)]*\)`)
	bareURL     = regexp.MustCompile(`https?://\S+`)
	heading     = regexp.MustCompile(`(?m)^\s{0,3}#{1,6}\s+`)
	listMarker  = regexp.MustCompile(`(?m)^\s*(?:[-*+]|\d+[.)])\s+`)
	quoteMarker = regexp.MustCompile(`(?m)^\s*>\s?`)
	emphasis    = regexp.MustCompile(`(\*\*|__|\*|_|~~)([^*_~\n]+)(\*\*|__|\*|_|~~)`)
	tableRule   = regexp.MustCompile(`(?m)^\s*\|?\s*:?-{3,}.*$`)
	spaces      = regexp.MustCompile(`[ \t]+`)
	lineEdges   = regexp.MustCompile(`(?m)^ | $`)
	blankLines  = regexp.MustCompile(`\n{2,}`)
	sentenceEnd = regexp.MustCompile(`[.!?…]\s`)
)

// Speakable turns a Markdown reply into text a voice can read: code and
// addresses are named instead of spelled out, since the transcript keeps them.
func Speakable(markdown string) string {
	text := codeBlock.ReplaceAllString(markdown, " (code is in the chat) ")
	text = image.ReplaceAllString(text, "$1")
	text = link.ReplaceAllString(text, "$1")
	text = bareURL.ReplaceAllString(text, "(link in the chat)")
	text = inlineCode.ReplaceAllString(text, "$1")
	text = tableRule.ReplaceAllString(text, "")
	text = heading.ReplaceAllString(text, "")
	text = listMarker.ReplaceAllString(text, "")
	text = quoteMarker.ReplaceAllString(text, "")

	for emphasis.MatchString(text) {
		text = emphasis.ReplaceAllString(text, "$2")
	}

	text = strings.ReplaceAll(text, "|", ", ")
	text = spaces.ReplaceAllString(text, " ")
	text = lineEdges.ReplaceAllString(text, "")
	text = blankLines.ReplaceAllString(text, "\n")

	return strings.TrimSpace(text)
}

// Chunks splits text into pieces of at most limit characters, preferring
// sentence ends, so each piece fits one synthesis request.
func Chunks(text string, limit int) []string {
	var chunks []string

	for utf8.RuneCountInString(text) > limit {
		runes := []rune(text)
		window := string(runes[:limit])
		// One rune of lookahead finds a sentence that ends exactly at the limit.
		lookahead := string(runes[:limit+1])

		cut := len(window)
		if ends := sentenceEnd.FindAllStringIndex(lookahead, -1); len(ends) > 0 {
			// Cut after the punctuation; the matched whitespace is one ASCII byte.
			cut = ends[len(ends)-1][1] - 1
		} else if space := strings.LastIndexAny(window, " \n"); space > 0 {
			cut = space
		}

		chunks = append(chunks, strings.TrimSpace(window[:cut]))
		text = strings.TrimSpace(text[cut:])
	}

	if text != "" {
		chunks = append(chunks, text)
	}

	return chunks
}
