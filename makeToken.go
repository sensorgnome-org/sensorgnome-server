package main

import (
	"crypto/rand"
	// TESTING: 	"fmt"
	"log"
	"strings"
)

// generate a random token of printable characters
//
// adapted from https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-go
//
// JMB changes:
// - use crypto/rand
// - generate strings from an alphabet of 64 bytes so we make full use of 6 random bits
// - cache unused random bits across calls

const tokenBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/"
const (
	tokenIdxBits = 6  // 6 bits to represent a token index
	tokenIdxMask = 63 // All 1-bits, as many as tokenIdxBits
)

var (
	byteCache      []byte = make([]byte, 48) // note: size divisible by 3 bytes = 4 tokenBytes
	byteCacheIndex int    = 48
	bitCache       uint8  = 0
	bitCacheSize   uint8  = 0
)

// generate a random printable token of n characters
func MakeToken(n int) string {
	sb := strings.Builder{}
	sb.Grow(n)
	for i := n - 1; i >= 0; i-- {
		var idx uint8
		if bitCacheSize == 6 {
			// need 0 bits from new byte
			idx = bitCache
			bitCacheSize = 0
			bitCache = 0
		} else {
			// we'll need a byte to make up 6 bits
			if byteCacheIndex >= len(byteCache) {
				if _, err := rand.Read(byteCache); err != nil {
					log.Fatal("Unable to get random bits")
				}
				// TESTING: 				bitsRequested += 8 * len(byteCache)
				byteCacheIndex = 0
			}
			idx = bitCache | ((byteCache[byteCacheIndex] & (1<<(tokenIdxBits-bitCacheSize) - 1)) << bitCacheSize)
			bitCache = byteCache[byteCacheIndex] >> (tokenIdxBits - bitCacheSize)
			bitCacheSize += 2 // added 8, used 6
			byteCacheIndex++
		}
		sb.WriteByte(tokenBytes[idx])
		// TESTING: 		bitsUsed += 6
	}
	return sb.String()
}

// TESTING: var bitsUsed, bitsRequested int = 0, 0
// TESTING:
// TESTING: func main() {
// TESTING: 	for i := 1; i <= 384; i++ {
// TESTING: 		fmt.Println(MakeToken(i))
// TESTING: 	}
// TESTING: 	fmt.Printf("Total random bits used: %d, requested:%d\n", bitsUsed, bitsRequested)
// TESTING: }
