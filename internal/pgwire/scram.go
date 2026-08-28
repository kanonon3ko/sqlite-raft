// SCRAM-SHA-256 认证（RFC 5802 / RFC 7677）。
// 服务端只保存 salt/迭代次数/StoredKey/ServerKey，不保存明文密码。
package pgwire

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// scramIterations 是 PBKDF2 迭代次数。
const scramIterations = 4096

// credential 是一份 SCRAM 凭据。
type credential struct {
	salt       []byte
	iterations int
	storedKey  []byte
	serverKey  []byte
}

// newCredential 由明文密码生成凭据（随机盐）。
func newCredential(password string) *credential {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		panic(err)
	}
	return credentialFromSalt(password, salt, scramIterations)
}

// credentialFromSalt 由固定盐生成凭据（测试用）。
func credentialFromSalt(password string, salt []byte, iterations int) *credential {
	salted := pbkdf2SHA256([]byte(password), salt, iterations, sha256.Size)
	clientKey := hmacSHA256(salted, []byte("Client Key"))
	serverKey := hmacSHA256(salted, []byte("Server Key"))
	return &credential{
		salt:       salt,
		iterations: iterations,
		storedKey:  sha256Sum(clientKey),
		serverKey:  serverKey,
	}
}

// serverFirst 生成 server-first-message，返回消息与完整的 nonce。
func (c *credential) serverFirst(clientNonce string) (serverFirst, nonce string, err error) {
	buf := make([]byte, 18)
	if _, err := rand.Read(buf); err != nil {
		return "", "", err
	}
	serverNonce := fmt.Sprintf("%x", buf)
	nonce = clientNonce + serverNonce
	serverFirst = fmt.Sprintf("r=%s,s=%s,i=%d",
		nonce, base64.StdEncoding.EncodeToString(c.salt), c.iterations)
	return serverFirst, nonce, nil
}

// verifyClientFinal 验证 client-final-message 的 proof，返回 server-final-message。
func (c *credential) verifyClientFinal(clientFirstBare, serverFirst, clientFinal string) (string, error) {
	fields, err := parseScramFields(clientFinal)
	if err != nil {
		return "", err
	}
	proofB64, ok := fields["p"]
	if !ok {
		return "", errors.New("scram: client-final missing proof")
	}
	proof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return "", errors.New("scram: bad proof encoding")
	}

	// AuthMessage = client-first-bare + "," + server-first + "," + client-final-without-proof
	withoutProof := clientFinal
	if i := strings.LastIndex(clientFinal, ",p="); i >= 0 {
		withoutProof = clientFinal[:i]
	}
	authMessage := clientFirstBare + "," + serverFirst + "," + withoutProof

	clientSignature := hmacSHA256(c.storedKey, []byte(authMessage))
	clientKey := xorBytes(proof, clientSignature)
	if !hmac.Equal(sha256Sum(clientKey), c.storedKey) {
		return "", errors.New("scram: proof verification failed")
	}
	serverSignature := hmacSHA256(c.serverKey, []byte(authMessage))
	return "v=" + base64.StdEncoding.EncodeToString(serverSignature), nil
}

// parseScramFields 解析 `k=v,k=v,...` 形式的 SCRAM 字段。
func parseScramFields(s string) (map[string]string, error) {
	out := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 || kv[0] == "" {
			return nil, fmt.Errorf("scram: malformed field %q", part)
		}
		out[kv[0]] = kv[1]
	}
	return out, nil
}

// pbkdf2SHA256 是 RFC 2898 的 PBKDF2（HMAC-SHA256）。
func pbkdf2SHA256(password, salt []byte, iter, keyLen int) []byte {
	hashLen := sha256.Size
	numBlocks := (keyLen + hashLen - 1) / hashLen
	out := make([]byte, 0, numBlocks*hashLen)
	for block := 1; block <= numBlocks; block++ {
		// U1 = HMAC(password, salt || INT(block))
		msg := make([]byte, len(salt)+4)
		copy(msg, salt)
		msg[len(salt)] = byte(block >> 24)
		msg[len(salt)+1] = byte(block >> 16)
		msg[len(salt)+2] = byte(block >> 8)
		msg[len(salt)+3] = byte(block)
		u := hmacSHA256(password, msg)
		t := make([]byte, hashLen)
		copy(t, u)
		for i := 1; i < iter; i++ {
			u = hmacSHA256(password, u)
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

func sha256Sum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

func xorBytes(a, b []byte) []byte {
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out
}

// parseClientFirstBare 去掉 client-first-message 的机制前缀 "n,,"。
func parseClientFirstBare(clientFirst string) string {
	if i := strings.Index(clientFirst, ",,"); i >= 0 {
		return clientFirst[i+2:]
	}
	return clientFirst
}

// scramClientNonce 提取 client-first-message 中的客户端 nonce（r=...）。
func scramClientNonce(clientFirst string) string {
	for _, part := range strings.Split(clientFirst, ",") {
		if strings.HasPrefix(part, "r=") {
			return part[2:]
		}
	}
	return ""
}
