package security

import (
	"crypto/hmac"
	"crypto/sha256"
)

func Verify(key, msg, sig []byte) (bool, error) {
	if key == nil || len(key) != 64 {
		return false, &KeyError{Reason: "Invalid key"}
	}
	return hmac.Equal(Sign(key, msg), sig), nil
}

func Sign(key, msg []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(msg)
	return m.Sum(nil)
}
