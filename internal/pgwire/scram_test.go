package pgwire

import (
	"encoding/base64"
	"testing"
)

// TestScramRFC7677 用 RFC 7677 附录 A 的官方测试向量验证算法。
func TestScramRFC7677(t *testing.T) {
	salt, err := base64.StdEncoding.DecodeString("W22ZaJ0SNY7soEsUEjb6gQ==")
	if err != nil {
		t.Fatalf("decode salt: %v", err)
	}
	cred := credentialFromSalt("pencil", salt, 4096)

	clientFirst := "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
	clientFirstBare := parseClientFirstBare(clientFirst)
	serverFirst := "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," +
		"s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
	clientFinal := "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," +
		"p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="

	serverFinal, err := cred.verifyClientFinal(clientFirstBare, serverFirst, clientFinal)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	want := "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
	if serverFinal != want {
		t.Fatalf("server-final = %q, want %q", serverFinal, want)
	}
}

// TestScramWrongProof 验证错误密码会被拒绝。
func TestScramWrongProof(t *testing.T) {
	cred := newCredential("correct-password")
	clientFirst := "n,,n=user,r=abc123"
	clientFirstBare := parseClientFirstBare(clientFirst)
	serverFirst, nonce, err := cred.serverFirst("abc123")
	if err != nil {
		t.Fatalf("server first: %v", err)
	}

	// 用错误密码构造 client-final（客户端侧计算）
	clientFinal := scramClientFinal("wrong-password", cred.salt, cred.iterations,
		clientFirstBare, serverFirst, nonce)

	if _, err := cred.verifyClientFinal(clientFirstBare, serverFirst, clientFinal); err == nil {
		t.Fatal("expected verification failure for wrong password")
	}
}

// TestScramRoundTrip 模拟完整服务端流程：server-first → client-final → server-final。
func TestScramRoundTrip(t *testing.T) {
	cred := newCredential("s3cret")
	clientFirst := "n,,n=alice,r=clientnonce123"
	clientFirstBare := parseClientFirstBare(clientFirst)
	serverFirst, nonce, err := cred.serverFirst("clientnonce123")
	if err != nil {
		t.Fatalf("server first: %v", err)
	}

	clientFinal := scramClientFinal("s3cret", cred.salt, cred.iterations,
		clientFirstBare, serverFirst, nonce)
	serverFinal, err := cred.verifyClientFinal(clientFirstBare, serverFirst, clientFinal)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if serverFinal == "" {
		t.Fatal("empty server final")
	}
}

// scramClientFinal 是测试用客户端侧计算（与服务端共享 PBKDF2 等原语）。
func scramClientFinal(password string, salt []byte, iterations int,
	clientFirstBare, serverFirst, nonce string) string {
	clientFinalWithoutProof := "c=biws,r=" + nonce
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof
	salted := pbkdf2SHA256([]byte(password), salt, iterations, 32)
	ck := hmacSHA256(salted, []byte("Client Key"))
	storedKey := sha256Sum(ck)
	clientSignature := hmacSHA256(storedKey, []byte(authMessage))
	proof := xorBytes(ck, clientSignature)
	return clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
}
