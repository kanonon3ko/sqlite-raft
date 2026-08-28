//go:build !race

package server

// 普通构建：完整混沌负载。
const (
	chaosLoadSeconds = 15
	chaosFaultRounds = 6
)
