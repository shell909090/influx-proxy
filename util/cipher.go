package util

import (
    "bytes"
    "crypto/aes"
    "crypto/cipher"
    "encoding/base64"
)

func AesEncrypt(origin string, key string) string {
    if len(origin) == 0 {
        return ""
    }
    originBytes := []byte(origin)
    k := []byte(key)
    block, _ := aes.NewCipher(k)
    blockSize := block.BlockSize()
    originBytes = PKCS7Padding(originBytes, blockSize)
    blockMode := cipher.NewCBCEncrypter(block, k[:blockSize])
    encryptBytes := make([]byte, len(originBytes))
    blockMode.CryptBlocks(encryptBytes, originBytes)
    return base64.StdEncoding.EncodeToString(encryptBytes)
}

func AesDecrypt(encrypt string, key string) string {
    if len(encrypt) != 24 || len(key) != 24 {
        return ""
    }
    encryptBytes, _ := base64.StdEncoding.DecodeString(encrypt)
    k := []byte(key)
    block, _ := aes.NewCipher(k)
    blockSize := block.BlockSize()
    blockMode := cipher.NewCBCDecrypter(block, k[:blockSize])
    plainBytes := make([]byte, len(encryptBytes))
    blockMode.CryptBlocks(plainBytes, encryptBytes)
    plainBytes = PKCS7UnPadding(plainBytes)
    return string(plainBytes)
}

func PKCS7Padding(originBytes []byte, blockSize int) []byte {
    padding := blockSize - len(originBytes) % blockSize
    padBytes := bytes.Repeat([]byte{byte(padding)}, padding)
    return append(originBytes, padBytes...)
}

func PKCS7UnPadding(plainBytes []byte) []byte {
    length := len(plainBytes)
    unpadding := int(plainBytes[length-1])
    return plainBytes[:(length - unpadding)]
}
