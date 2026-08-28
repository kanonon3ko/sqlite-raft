// Package lincheck 验证操作历史的线性一致性（linearizability）。
//
// 模型：单 key 寄存器。Write 写入一个唯一值（不同客户端写不同值），
// Read 返回最近一次写出的值（或初始值）。每个操作记录
// [开始时间, 结束时间] 与客户端 ID。
//
// 线性化条件：存在一个合法顺序（线性化点），使得：
//   - 每个操作的线性化点落在其时间窗口内；
//   - 同一客户端的操作保持调用顺序；
//   - 每次 Read 返回的值 = 该点之前最近一次同 key 写出的值（或初始值）。
//
// 检查使用带回溯的拓扑排序：窗口短、操作数少（< 40）时可接受。
package lincheck

import (
	"fmt"
	"sort"
	"time"
)

// OpKind 是操作类型。
type OpKind int

const (
	Write OpKind = iota
	Read
)

// Op 是一条已完成的客户端操作记录。
type Op struct {
	Client int
	Kind   OpKind
	Key    string
	Value  string // Write 的新值 / Read 的返回值
	Start  time.Time
	End    time.Time
}

// Check 验证历史是否线性化；返回 nil 表示通过。
func Check(ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	// 单 key 寄存器模型：只支持一个 key（负载设计保证）。
	// 过滤：只保留该 key 的操作（模型限制）。
	key := ops[0].Key
	filtered := make([]Op, 0, len(ops))
	for _, op := range ops {
		if op.Key != key {
			return fmt.Errorf("lincheck: multi-key history not supported (key %q vs %q)", op.Key, key)
		}
		if op.End.Before(op.Start) {
			return fmt.Errorf("lincheck: op %v has end before start", op)
		}
		filtered = append(filtered, op)
	}

	// 同一客户端顺序约束：同 client 的操作按 Start 排序后必须保持该顺序。
	clientOrder := make(map[int][]int)
	for i := range filtered {
		clientOrder[filtered[i].Client] = append(clientOrder[filtered[i].Client], i)
	}
	for c := range clientOrder {
		sort.Slice(clientOrder[c], func(a, b int) bool {
			return filtered[clientOrder[c][a]].Start.Before(filtered[clientOrder[c][b]].Start)
		})
	}

	solver := &solver{
		ops:        filtered,
		clientNext: clientOrder,
	}
	order, ok := solver.solve()
	if !ok {
		return fmt.Errorf("lincheck: history is not linearizable (%d ops)", len(filtered))
	}
	_ = order
	return nil
}

type solver struct {
	ops        []Op
	clientNext map[int][]int
}

// solve 尝试找到合法线性化顺序；返回顺序（索引列表）与是否成功。
func (s *solver) solve() ([]int, bool) {
	placed := make([]bool, len(s.ops))
	clientPos := make(map[int]int) // 每个 client 已放置到第几个
	state := make(map[string]string)

	var order []int
	var search func(int, time.Time) bool
	search = func(n int, cur time.Time) bool {
		if n == len(s.ops) {
			return true
		}
		// 候选：未放置、同 client 前序已放置、线性化点可落在窗口内
		candidates := make([]int, 0)
		for i := range s.ops {
			if placed[i] {
				continue
			}
			op := s.ops[i]
			pos := clientPos[op.Client]
			// 同 client 顺序：该 client 的下一个未放置操作必须是当前这个
			if pos < len(s.clientNext[op.Client]) && s.clientNext[op.Client][pos] != i {
				continue
			}
			nextCur := cur
			if op.Start.After(nextCur) {
				nextCur = op.Start
			}
			if nextCur.After(op.End) {
				continue // 线性化点无法落在窗口内
			}
			candidates = append(candidates, i)
		}
		sort.Slice(candidates, func(a, b int) bool {
			return s.ops[candidates[a]].End.Before(s.ops[candidates[b]].End)
		})

		for _, i := range candidates {
			op := s.ops[i]
			// 值约束：Write 放置时写入；Read 放置时返回值必须等于当前状态。
			val, exists := state[op.Key]
			if op.Kind == Read {
				if !exists {
					if op.Value != "" {
						continue // 读到了非初始值，但还没有写
					}
				} else if op.Value != val {
					continue // 读到的值不是最近写的值
				}
			}

			placed[i] = true
			clientPos[op.Client]++
			nextCur := cur
			if op.Start.After(nextCur) {
				nextCur = op.Start
			}
			prev, hadPrev := state[op.Key]
			if op.Kind == Write {
				state[op.Key] = op.Value
			}
			order = append(order, i)

			if search(n+1, nextCur) {
				return true
			}

			// 回溯
			order = order[:len(order)-1]
			if op.Kind == Write {
				if hadPrev {
					state[op.Key] = prev
				} else {
					delete(state, op.Key)
				}
			}
			clientPos[op.Client]--
			placed[i] = false
		}
		return false
	}

	if !search(0, time.Time{}) {
		return nil, false
	}
	return order, true
}
