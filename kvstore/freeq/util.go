package freeq

import (
	"encoding/base64"
	"encoding/hex"
)

var kvencode = kvencodeHex
var kvdecode = kvdecodeHex

func kvencodeHex(b []byte) []byte {
	return []byte(hex.EncodeToString(b))
}

func kvdecodeHex(b []byte) ([]byte, error) {
	return hex.DecodeString(string(b))
}

func kvencodeBase64(b []byte) []byte {
	return []byte(base64.RawStdEncoding.EncodeToString(b))
}

func kvdecodeBase64(b []byte) ([]byte, error) {
	return base64.RawStdEncoding.DecodeString(string(b))
}
