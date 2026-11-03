package data

import (
	"crypto/rand"
	"encoding/hex"
	"regexp"
)

var uidRe = regexp.MustCompile(`^[0-9a-zA-Z_-]{1,64}$`)

func generateShortUID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func validUID(uid string) bool {
	return uidRe.MatchString(uid)
}
