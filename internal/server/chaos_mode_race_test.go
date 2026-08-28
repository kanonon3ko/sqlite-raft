//go:build race

package server

// race 构建：缩短混沌负载，避免整体测试超时。
const (
	chaosLoadSeconds = 6
	chaosFaultRounds = 3
)
