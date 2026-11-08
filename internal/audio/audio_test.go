package audio_test

import (
	"math"
	"slices"
	"testing"

	"github.com/crumble-project/crumble-voice/internal/audio"
)

const speechRate = audio.TelephoneRate * audio.Factor

// tone returns one second of a sine at the 24 kHz speech rate.
func tone(frequency float64) []int16 {
	samples := make([]int16, speechRate)
	for i := range samples {
		samples[i] = int16(10000 * math.Sin(2*math.Pi*frequency*float64(i)/speechRate))
	}

	return samples
}

func rms(samples []int16) float64 {
	var sum float64
	for _, sample := range samples {
		sum += float64(sample) * float64(sample)
	}

	return math.Sqrt(sum / float64(len(samples)))
}

func TestDownsamplerKeepsSpeechAndRemovesAliases(t *testing.T) {
	t.Parallel()

	// Skip the filter's start-up so only the settled output is measured.
	speech := audio.NewDownsampler().Process(tone(1000))[100:]
	alias := audio.NewDownsampler().Process(tone(7000))[100:]

	if got := rms(speech) / rms(tone(1000)); got < 0.9 || got > 1.1 {
		t.Errorf("1 kHz gain = %.2f, want about 1", got)
	}

	// The top of the telephone band must survive, or voices sound muffled.
	edge := audio.NewDownsampler().Process(tone(3400))[200:]
	if got := 20 * math.Log10(rms(edge)/rms(tone(3400))); got < -1 {
		t.Errorf("3.4 kHz loses %.1f dB, want at most 1", -got)
	}

	// 7 kHz would fold back to 1 kHz at 8 kHz without the filter.
	if got := rms(alias) / rms(tone(7000)); got > 0.05 {
		t.Errorf("7 kHz leaks through at %.3f", got)
	}

	if len(speech)+100 != audio.TelephoneRate {
		t.Errorf("one second became %d samples", len(speech)+100)
	}
}

func TestUpsamplerJoinsChunks(t *testing.T) {
	t.Parallel()

	var upsampler audio.Upsampler

	first := upsampler.Process([]int16{0, 300})
	second := upsampler.Process([]int16{600})

	want := []int16{0, 0, 0, 100, 200, 300, 400, 500, 600}
	got := slices.Concat(first, second)

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestBytesRoundTrip(t *testing.T) {
	t.Parallel()

	samples := []int16{math.MinInt16, -1, 0, 1, math.MaxInt16}
	back := audio.Samples(audio.Bytes(samples))

	for i := range samples {
		if back[i] != samples[i] {
			t.Fatalf("round trip = %v", back)
		}
	}
}
