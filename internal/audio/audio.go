// Package audio converts 16-bit mono PCM between the telephone rate and speech service rates.
package audio

import (
	"encoding/binary"
	"math"
)

const (
	// TelephoneRate is the G.711 sample rate in Hz.
	TelephoneRate = 8000
	// FrameSamples is 20 ms of telephone audio, the RTP packet size.
	FrameSamples = TelephoneRate / 50
	// FrameBytes is FrameSamples as 16-bit little-endian PCM.
	FrameBytes = FrameSamples * BytesPerSample
	// Factor is the ratio between the 24 kHz speech rate and the telephone rate.
	Factor = 3
	// BytesPerSample is the size of one 16-bit sample.
	BytesPerSample = 2
)

// Blackman window coefficients.
const (
	blackmanA0 = 0.42
	blackmanA1 = 0.5
	blackmanA2 = 0.08
)

// Samples decodes 16-bit little-endian PCM. A trailing odd byte is ignored.
func Samples(pcm []byte) []int16 {
	samples := make([]int16, len(pcm)/BytesPerSample)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(pcm[BytesPerSample*i:])) //nolint:gosec // Reinterpreting PCM bits is intended.
	}

	return samples
}

// Bytes encodes samples as 16-bit little-endian PCM.
func Bytes(samples []int16) []byte {
	pcm := make([]byte, BytesPerSample*len(samples))
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(pcm[BytesPerSample*i:], uint16(sample)) //nolint:gosec // Reinterpreting PCM bits is intended.
	}

	return pcm
}

// Upsampler triples the sample rate with linear interpolation, keeping the
// last sample of the previous chunk so chunks join without clicks.
type Upsampler struct {
	last    int16
	started bool
}

// Process returns len(in)*Factor samples.
func (u *Upsampler) Process(in []int16) []int16 {
	out := make([]int16, 0, len(in)*Factor)

	for _, sample := range in {
		previous := sample
		if u.started {
			previous = u.last
		}

		for step := 1; step <= Factor; step++ {
			value := int32(previous) + (int32(sample)-int32(previous))*int32(step)/Factor
			out = append(out, int16(value)) //nolint:gosec // value lies between two int16 samples.
		}

		u.last, u.started = sample, true
	}

	return out
}

// Downsampler divides the sample rate by Factor after a low-pass filter, so
// 24 kHz speech has no aliasing above the 4 kHz telephone band.
type Downsampler struct {
	taps    []float64
	history []float64
	phase   int
}

// Taps of a windowed-sinc filter: 127 keeps the band flat to 3.4 kHz with a
// 2.6 ms delay; 63 taps lost 6 dB there and made voices sound muffled.
const downsamplerTaps = 127

// NewDownsampler returns a filter flat to 3.4 kHz, the telephone band, at 24 kHz input.
func NewDownsampler() *Downsampler {
	const (
		inputRate = TelephoneRate * Factor
		cutoff    = 3700.0 / inputRate
	)

	taps := make([]float64, downsamplerTaps)
	span := float64(downsamplerTaps - 1)
	middle := span / 2 //nolint:mnd // The filter is symmetric around its middle tap.

	omega := 2 * math.Pi * cutoff //nolint:mnd // Angular frequency is 2πf.

	var sum float64

	for i := range taps {
		x := float64(i) - middle

		sinc := omega / math.Pi
		if x != 0 {
			sinc = math.Sin(omega*x) / (math.Pi * x)
		}

		phase := 2 * math.Pi * float64(i) / span
		window := blackmanA0 - blackmanA1*math.Cos(phase) + blackmanA2*math.Cos(2*phase) //nolint:mnd // Third term runs at twice the phase.
		taps[i] = sinc * window
		sum += taps[i]
	}

	for i := range taps {
		taps[i] /= sum
	}

	return &Downsampler{taps: taps, history: make([]float64, downsamplerTaps)}
}

// Process consumes 24 kHz samples and returns the 8 kHz samples they complete.
func (d *Downsampler) Process(in []int16) []int16 {
	out := make([]int16, 0, len(in)/Factor+1)

	for _, sample := range in {
		copy(d.history, d.history[1:])
		d.history[len(d.history)-1] = float64(sample)

		d.phase++
		if d.phase < Factor {
			continue
		}

		d.phase = 0

		var acc float64
		for i, tap := range d.taps {
			acc += tap * d.history[i]
		}

		out = append(out, clamp(acc))
	}

	return out
}

func clamp(value float64) int16 {
	switch {
	case value > math.MaxInt16:
		return math.MaxInt16
	case value < math.MinInt16:
		return math.MinInt16
	default:
		return int16(math.Round(value))
	}
}

// Silence returns one frame of telephone silence.
func Silence() []byte {
	return make([]byte, FrameBytes)
}
