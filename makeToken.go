package main

import (
	"crypto/rand"
	"encoding/base64"
	"log"
)

// generate a random printable token of n characters
// using ceiling(6n / 8) random bytes
func MakeToken(n int) string {
	buf := make([]byte, (6 * n + 7) / 8)
	if _, err := rand.Read(buf); err != nil {
		log.Fatal("Unable to get random bits")
	}
	return base64.RawStdEncoding.EncodeToString(buf)[:n]
}
