package sipline

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Party is who a call the server asked for goes to.
type Party int

const (
	// PartyOwner is a call to one of the owner's numbers.
	PartyOwner Party = iota
	// PartyOutside is a call to someone other than the owner, which needs call.allowOutside.
	PartyOutside
)

var (
	// ErrOutsideOff means call.allowOutside is off, so the gateway dials only the owner.
	ErrOutsideOff = errors.New("outside calls are switched off on this voice gateway")
	// ErrNumberCharacters means an outside number holds more than digits and one leading +.
	ErrNumberCharacters = errors.New("an outside number may hold only digits and one leading +")
	// ErrNumberLength means an outside number has fewer than 5 or more than 15 digits.
	ErrNumberLength = errors.New("an outside number must have 5 to 15 digits")
	// ErrNumberBlocked means an outside number starts with a premium, service, or satellite prefix.
	ErrNumberBlocked = errors.New("an outside number must not start with a premium, service, or satellite prefix")
	// ErrNumberIsOwner means an outside number is one of the owner's numbers.
	ErrNumberIsOwner = errors.New("an outside call must not go to one of the owner's numbers")
)

const (
	minOutsideDigits = 5
	maxOutsideDigits = 15
)

// blockedPrefixes are German premium and service numbers, also after +49, international
// premium, and satellite networks.
func blockedPrefixes() []string {
	return []string{
		"0900", "0137", "0180", "118",
		"+49900", "+49137", "+49180", "+49118",
		"+979", "+870", "+881", "+882",
	}
}

// Dialable returns the number to dial for request, or why the gateway refuses it. A call
// to the owner must go to one of the numbers in owner; an outside call needs allowOutside
// and a number that passes OutsideNumber.
func Dialable(request Outbound, owner []string, allowOutside bool) (string, error) {
	switch {
	case request.Party == PartyOwner && slices.Contains(owner, request.Number):
		return request.Number, nil
	case request.Party == PartyOwner:
		return "", ErrNotOwner
	case !allowOutside:
		return "", ErrOutsideOff
	default:
		return OutsideNumber(request.Number, owner)
	}
}

// OutsideNumber normalizes the number of an outside call and checks it against the number
// rules the server applies too. It returns the number to dial.
func OutsideNumber(number string, owner []string) (string, error) {
	normalized := normalize(number)

	digits := strings.TrimPrefix(normalized, "+")
	if strings.ContainsFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) {
		return "", ErrNumberCharacters
	}

	if len(digits) < minOutsideDigits || len(digits) > maxOutsideDigits {
		return "", ErrNumberLength
	}

	for _, prefix := range blockedPrefixes() {
		if strings.HasPrefix(normalized, prefix) {
			return "", fmt.Errorf("%w: %s", ErrNumberBlocked, prefix)
		}
	}

	for _, own := range owner {
		if normalize(own) == normalized {
			return "", ErrNumberIsOwner
		}
	}

	return normalized, nil
}

// normalize strips spaces, dashes, dots, and brackets, and turns a leading 00 into +.
func normalize(number string) string {
	stripped := strings.Map(func(r rune) rune {
		if strings.ContainsRune(" -.()[]{}", r) {
			return -1
		}

		return r
	}, number)

	if rest, ok := strings.CutPrefix(stripped, "00"); ok {
		return "+" + rest
	}

	return stripped
}
