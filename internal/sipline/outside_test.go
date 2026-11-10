package sipline_test

import (
	"errors"
	"testing"

	"github.com/crumble-project/crumble-voice/internal/sipline"
)

func TestOutsideNumber(t *testing.T) {
	t.Parallel()

	owner := []string{"+49 30 1111111", "**610"}

	cases := []struct {
		number string
		want   string
		err    error
	}{
		{number: "+4930123456", want: "+4930123456", err: nil},
		{number: "0049 (30) 123-456", want: "+4930123456", err: nil},
		{number: "030.123.456", want: "030123456", err: nil},
		{number: "[030] 123456", want: "030123456", err: nil},
		{number: "12345", want: "12345", err: nil},
		{number: "+123456789012345", want: "+123456789012345", err: nil},
		{number: "0118123456", want: "0118123456", err: nil},
		{number: "1234", want: "", err: sipline.ErrNumberLength},
		{number: "112", want: "", err: sipline.ErrNumberLength},
		{number: "+1234", want: "", err: sipline.ErrNumberLength},
		{number: "1234567890123456", want: "", err: sipline.ErrNumberLength},
		{number: "", want: "", err: sipline.ErrNumberLength},
		{number: "+", want: "", err: sipline.ErrNumberLength},
		{number: "030 1234*56", want: "", err: sipline.ErrNumberCharacters},
		{number: "**610", want: "", err: sipline.ErrNumberCharacters},
		{number: "030123456#", want: "", err: sipline.ErrNumberCharacters},
		{number: "++4930123456", want: "", err: sipline.ErrNumberCharacters},
		{number: "49+30123456", want: "", err: sipline.ErrNumberCharacters},
		{number: "030/123456", want: "", err: sipline.ErrNumberCharacters},
		{number: "sip:030123456", want: "", err: sipline.ErrNumberCharacters},
		{number: "0900 1234567", want: "", err: sipline.ErrNumberBlocked},
		{number: "0137 123456", want: "", err: sipline.ErrNumberBlocked},
		{number: "0180 5123456", want: "", err: sipline.ErrNumberBlocked},
		{number: "11833", want: "", err: sipline.ErrNumberBlocked},
		{number: "+49 900 1234567", want: "", err: sipline.ErrNumberBlocked},
		{number: "0049 900 1234567", want: "", err: sipline.ErrNumberBlocked},
		{number: "+49 137 123456", want: "", err: sipline.ErrNumberBlocked},
		{number: "+49 180 5123456", want: "", err: sipline.ErrNumberBlocked},
		{number: "+979 12345678", want: "", err: sipline.ErrNumberBlocked},
		{number: "+870 123456789", want: "", err: sipline.ErrNumberBlocked},
		{number: "+881 234567890", want: "", err: sipline.ErrNumberBlocked},
		{number: "00882 12345678", want: "", err: sipline.ErrNumberBlocked},
		{number: "+49301111111", want: "", err: sipline.ErrNumberIsOwner},
		{number: "0049-30-1111111", want: "", err: sipline.ErrNumberIsOwner},
	}

	for _, test := range cases {
		t.Run(test.number, func(t *testing.T) {
			t.Parallel()

			got, err := sipline.OutsideNumber(test.number, owner)
			if got != test.want || !errors.Is(err, test.err) || (test.err == nil) != (err == nil) {
				t.Errorf("OutsideNumber(%q) = %q, %v, want %q, %v", test.number, got, err, test.want, test.err)
			}
		})
	}
}

func TestDialable(t *testing.T) {
	t.Parallel()

	owner := []string{"**610"}

	cases := []struct {
		name         string
		party        sipline.Party
		number       string
		allowOutside bool
		want         string
		err          error
	}{
		{"the owner", sipline.PartyOwner, "**610", false, "**610", nil},
		{"not an owner number", sipline.PartyOwner, "+4930123456", true, "", sipline.ErrNotOwner},
		{"outside switched off", sipline.PartyOutside, "+4930123456", false, "", sipline.ErrOutsideOff},
		{"outside", sipline.PartyOutside, "0049 30 123456", true, "+4930123456", nil},
		{"outside to the owner", sipline.PartyOutside, "**610", true, "", sipline.ErrNumberCharacters},
		{"outside premium", sipline.PartyOutside, "09001234567", true, "", sipline.ErrNumberBlocked},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			request := sipline.Outbound{CallID: "", Username: "ada", Number: test.number, Greeting: "", Party: test.party}

			got, err := sipline.Dialable(request, owner, test.allowOutside)
			if got != test.want || !errors.Is(err, test.err) || (test.err == nil) != (err == nil) {
				t.Errorf("Dialable = %q, %v, want %q, %v", got, err, test.want, test.err)
			}
		})
	}
}
