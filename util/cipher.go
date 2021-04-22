// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package util

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
)

var cipherKey = []byte("consistentcipher")
var aesCipher, _ = aes.NewCipher(cipherKey)
var blockSize = aesCipher.BlockSize()
var iv = cipherKey[:blockSize]

var encodeURL = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_!"
var base64RawURLEncoding = base64.NewEncoding(encodeURL).WithPadding(base64.NoPadding)

func CheckCipherKey(key string) bool {
	return key == string(cipherKey)
}

func AesEncrypt(origin string) string {
	if len(origin) == 0 {
		return ""
	}
	originBytes := padding([]byte(origin), blockSize)
	blockMode := cipher.NewCBCEncrypter(aesCipher, iv)
	encryptBytes := make([]byte, len(originBytes))
	blockMode.CryptBlocks(encryptBytes, originBytes)
	return base64RawURLEncoding.EncodeToString(encryptBytes)
}

func AesDecrypt(encrypt string) string {
	if len(encrypt) == 0 {
		return ""
	}
	encryptBytes, err := base64RawURLEncoding.DecodeString(encrypt)
	if err != nil {
		return err.Error()
	}
	if len(encryptBytes)%blockSize != 0 {
		return "crypto/cipher: input not full blocks"
	}
	blockMode := cipher.NewCBCDecrypter(aesCipher, iv)
	originBytes := make([]byte, len(encryptBytes))
	blockMode.CryptBlocks(originBytes, encryptBytes)
	return string(unpadding(originBytes))
}

func padding(data []byte, blockSize int) []byte {
	num := ((len(data)-1)/blockSize+1)*blockSize - len(data)
	pad := bytes.Repeat([]byte("\x00"), num)
	return append(data, pad...)
}

func unpadding(data []byte) []byte {
	return bytes.Trim(data, "\x00")
}
