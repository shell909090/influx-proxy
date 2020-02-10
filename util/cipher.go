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
    // 转成字节数组
    originBytes := []byte(origin)
    k := []byte(key)
    // 分组秘钥
    block, _ := aes.NewCipher(k)
    // 获取秘钥块的长度
    blockSize := block.BlockSize()
    // 补全码
    originBytes = PKCS7Padding(originBytes, blockSize)
    // 加密模式
    blockMode := cipher.NewCBCEncrypter(block, k[:blockSize])
    // 创建数组
    encryptBytes := make([]byte, len(originBytes))
    // 加密
    blockMode.CryptBlocks(encryptBytes, originBytes)
    return base64.StdEncoding.EncodeToString(encryptBytes)
}

func AesDecrypt(encrypt string, key string) string {
    // 转成字节数组
    if len(encrypt) != 24 || len(key) != 24 {
        return ""
    }
    encryptBytes, _ := base64.StdEncoding.DecodeString(encrypt)
    k := []byte(key)
    // 分组秘钥
    block, _ := aes.NewCipher(k)
    // 获取秘钥块的长度
    blockSize := block.BlockSize()
    // 加密模式
    blockMode := cipher.NewCBCDecrypter(block, k[:blockSize])
    // 创建数组
    plainBytes := make([]byte, len(encryptBytes))
    // 解密
    blockMode.CryptBlocks(plainBytes, encryptBytes)
    // 去补全码
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
