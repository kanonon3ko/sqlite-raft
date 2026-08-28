package lincheck

import (
	"testing"
	"time"
)

var t0 = time.Now()

func op(client int, kind OpKind, val string, start, end time.Duration) Op {
	return Op{
		Client: client,
		Kind:   kind,
		Key:    "k",
		Value:  val,
		Start:  t0.Add(start),
		End:    t0.Add(end),
	}
}

func TestLinearizableHistory(t *testing.T) {
	// 写1 与 写2 并发，读 2 读到 写2 的值（合法：线性化点 写2 在 读2 前）
	ops := []Op{
		op(1, Write, "a", 0, 10),
		op(2, Write, "b", 1, 9),
		op(3, Read, "b", 5, 15),
	}
	if err := Check(ops); err != nil {
		t.Fatalf("expected linearizable, got %v", err)
	}
}

func TestNonLinearizableHistory(t *testing.T) {
	// 读 返回 b，但写 b 的窗口在读之后才开始 → 不可能线性化
	ops := []Op{
		op(1, Write, "a", 0, 5),
		op(2, Read, "b", 1, 4),
		op(3, Write, "b", 10, 20),
	}
	if err := Check(ops); err == nil {
		t.Fatal("expected non-linearizable, got nil")
	}
}

func TestClientOrderConstraint(t *testing.T) {
	// 同一客户端先读后写：读 a 后写 b；若检查器允许交换则错误
	ops := []Op{
		op(1, Write, "b", 0, 10),
		op(1, Read, "b", 5, 15),
		op(2, Read, "", 20, 30), // 在写 b 之后读初始值 → 违反
	}
	if err := Check(ops); err == nil {
		t.Fatal("expected non-linearizable (client 2 read initial after write b), got nil")
	}
}

func TestInitialValueRead(t *testing.T) {
	ops := []Op{
		op(1, Read, "", 0, 10),
		op(2, Write, "x", 20, 30),
	}
	if err := Check(ops); err != nil {
		t.Fatalf("expected linearizable, got %v", err)
	}
}
