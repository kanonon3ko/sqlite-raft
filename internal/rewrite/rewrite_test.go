package rewrite

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
)

var fixedNow = time.Date(2026, 8, 27, 12, 34, 56, 789012000, time.UTC)

// fixedRandBytes 是固定字节序列，用于断言 RANDOM()/RANDOMBLOB() 的确定性结果。
var fixedRandBytes = []byte{
	0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	0xAA, 0xBB, 0xCC, 0xDD,
}

type fakeSchema struct {
	tables map[string]*TableInfo
	next   map[string][]int64 // 每次 NextAutoIncrement 调用返回的下一个值序列
}

func (f *fakeSchema) TableInfo(table string) (*TableInfo, error) {
	t, ok := f.tables[table]
	if !ok {
		return nil, nil
	}
	return t, nil
}

func (f *fakeSchema) NextAutoIncrement(table string) (int64, error) {
	seq := f.next[table]
	if len(seq) == 0 {
		return 0, errors.New("unexpected NextAutoIncrement call")
	}
	f.next[table] = seq[1:]
	return seq[0], nil
}

func rewrite(t *testing.T, sql string, params []*sqlraftpb.Value, schema Schema) *Result {
	t.Helper()
	res, err := Rewrite(sql, params, schema, Options{Now: fixedNow, Rand: bytes.NewReader(fixedRandBytes)})
	if err != nil {
		t.Fatalf("rewrite %q: %v", sql, err)
	}
	return res
}

func TestRewriteTimeFunctions(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"SELECT NOW()", "SELECT '2026-08-27 12:34:56.789012'"},
		{"INSERT INTO t (ts) VALUES (NOW())", "INSERT INTO t (ts) VALUES ('2026-08-27 12:34:56.789012')"},
		{"SELECT CURRENT_TIMESTAMP", "SELECT '2026-08-27 12:34:56'"},
		{"SELECT CURRENT_DATE", "SELECT '2026-08-27'"},
		{"SELECT CURRENT_TIME", "SELECT '12:34:56'"},
		{"SELECT now() FROM t", "SELECT '2026-08-27 12:34:56.789012' FROM t"},
		{"SELECT 'NOW()'", "SELECT 'NOW()'"},                     // 字符串内不改写
		{"SELECT now FROM t", "SELECT now FROM t"},               // 非调用不改写
		{"SELECT NOW(3)", "SELECT '2026-08-27 12:34:56.789012'"}, // 忽略精度参数
	}
	for _, c := range cases {
		res := rewrite(t, c.in, nil, nil)
		if res.SQL != c.want {
			t.Errorf("%q => %q, want %q", c.in, res.SQL, c.want)
		}
	}
}

func TestRewriteRandom(t *testing.T) {
	res := rewrite(t, "INSERT INTO t (v) VALUES (RANDOM())", nil, nil)
	if res.SQL != "INSERT INTO t (v) VALUES (578437695752307201)" {
		t.Fatalf("RANDOM() rewritten as %q", res.SQL)
	}

	res = rewrite(t, "INSERT INTO t (v) VALUES (RANDOMBLOB(4))", nil, nil)
	if res.SQL != "INSERT INTO t (v) VALUES (X'01020304')" {
		t.Fatalf("RANDOMBLOB(4) rewritten as %q", res.SQL)
	}
}

func TestRewriteRandomBlobParam(t *testing.T) {
	params := []*sqlraftpb.Value{
		{Kind: &sqlraftpb.Value_I{I: 7}}, // 对应第一个 ?
		{Kind: &sqlraftpb.Value_I{I: 3}}, // 对应 RANDOMBLOB 的长度 ?
	}
	res := rewrite(t, "INSERT INTO t (a, b) VALUES (?, RANDOMBLOB(?))", params, nil)
	want := "INSERT INTO t (a, b) VALUES (?, X'010203')"
	if res.SQL != want {
		t.Fatalf("got %q, want %q", res.SQL, want)
	}
	if len(res.Params) != 1 || res.Params[0].GetI() != 7 {
		t.Fatalf("params after rewrite = %v, want only [7]", res.Params)
	}
}

func TestRewriteDeterministic(t *testing.T) {
	a := rewrite(t, "INSERT INTO t (x) VALUES (NOW() + RANDOM())", nil, nil)
	b := rewrite(t, "INSERT INTO t (x) VALUES (NOW() + RANDOM())", nil, nil)
	if a.SQL != b.SQL {
		t.Fatalf("rewrite not deterministic:\n a=%q\n b=%q", a.SQL, b.SQL)
	}
}

func TestRewriteAutoIncrement(t *testing.T) {
	schema := &fakeSchema{
		tables: map[string]*TableInfo{
			"t": {
				Columns:       []string{"id", "name"},
				AutoIncrement: "id",
			},
			"u": {
				Columns:       []string{"a", "id", "b"},
				AutoIncrement: "id",
			},
		},
		next: map[string][]int64{"t": {5, 6, 8, 9}, "u": {100}},
	}

	cases := []struct {
		in   string
		want string
	}{
		{"INSERT INTO t (name) VALUES ('x')", `INSERT INTO t (name, "id") VALUES ('x', 5)`},
		{"INSERT INTO t (name) VALUES ('x'), ('y')", `INSERT INTO t (name, "id") VALUES ('x', 6), ('y', 7)`},
		{"INSERT INTO u (a, b) VALUES (1, 2)", `INSERT INTO u (a, b, "id") VALUES (1, 2, 100)`},
		// 无列清单：自增列位置已含用户提供的值，不改写
		{"INSERT INTO u VALUES (1, 2, 3)", "INSERT INTO u VALUES (1, 2, 3)"},
		{"INSERT INTO t DEFAULT VALUES", `INSERT INTO t ("id") VALUES (8)`},
		// 自增列已显式给出：不改写
		{"INSERT INTO t (id, name) VALUES (9, 'x')", "INSERT INTO t (id, name) VALUES (9, 'x')"},
		// 无自增列的表：不改写
		{"INSERT INTO nope (a) VALUES (1)", "INSERT INTO nope (a) VALUES (1)"},
	}
	for _, c := range cases {
		res := rewrite(t, c.in, nil, schema)
		if res.SQL != c.want {
			t.Errorf("%q => %q, want %q", c.in, res.SQL, c.want)
		}
	}

	// 序列记录
	res := rewrite(t, "INSERT INTO t (name) VALUES ('x')", nil, schema)
	if res.Sequence["t"] != 9 {
		t.Fatalf("sequence = %v, want t=9", res.Sequence)
	}
}

func TestRewriteInsertWithNondeterministicCall(t *testing.T) {
	schema := &fakeSchema{
		tables: map[string]*TableInfo{
			"t": {Columns: []string{"id", "v"}, AutoIncrement: "id"},
		},
		next: map[string][]int64{"t": {5}},
	}
	// 同时含自增列与 RANDOM()：整体走函数改写，AUTOINCREMENT 原样放行
	res := rewrite(t, "INSERT INTO t (v) VALUES (RANDOM())", nil, schema)
	if !strings.Contains(res.SQL, "578437695752307201") {
		t.Fatalf("RANDOM() not rewritten: %q", res.SQL)
	}
	if res.Sequence != nil {
		t.Fatalf("sequence should be nil, got %v", res.Sequence)
	}
}

func TestRewritePreservesCommentsAndStrings(t *testing.T) {
	res := rewrite(t, "INSERT INTO t (v) VALUES ('NOW()') -- CURRENT_TIMESTAMP", nil, nil)
	if !strings.Contains(res.SQL, "'NOW()'") || !strings.Contains(res.SQL, "-- CURRENT_TIMESTAMP") {
		t.Fatalf("string/comment content modified: %q", res.SQL)
	}
}

func TestRewriteSchemaError(t *testing.T) {
	schema := &fakeSchema{
		tables: map[string]*TableInfo{
			"t": {Columns: []string{"id"}, AutoIncrement: "id"},
		},
		next: map[string][]int64{},
	}
	// 改写失败（例如自增列已显式给出）不应报错，而是原样放行
	res, err := Rewrite("INSERT INTO t (id) VALUES (3)", nil, schema,
		Options{Now: fixedNow, Rand: bytes.NewReader(fixedRandBytes)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.SQL != "INSERT INTO t (id) VALUES (3)" {
		t.Fatalf("got %q", res.SQL)
	}
}

func TestRewriteSchemaReturnsError(t *testing.T) {
	errSchema := &errorSchema{}
	if _, err := Rewrite("INSERT INTO t (a) VALUES (1)", nil, errSchema,
		Options{Now: fixedNow, Rand: bytes.NewReader(fixedRandBytes)}); err != nil {
		t.Fatalf("schema error should be swallowed (pass-through), got %v", err)
	}
}

type errorSchema struct{}

func (errorSchema) TableInfo(string) (*TableInfo, error) { return nil, errors.New("boom") }
func (errorSchema) NextAutoIncrement(string) (int64, error) {
	return 0, errors.New("boom")
}
