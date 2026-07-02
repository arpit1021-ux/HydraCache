package utils

import (
	"fmt"
	"math/rand"
	"time"
)

func GenerateID() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		r.Int31(), r.Int31n(0xffff), r.Int31n(0xffff), r.Int31n(0xffff), r.Int63n(0xffffffffffff))
}

func ShortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
