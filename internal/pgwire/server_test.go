package pgwire

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/kanonon3ko/sqlite-raft/gen/logpb"
	"github.com/kanonon3ko/sqlite-raft/gen/raftpb"
	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
	"github.com/kanonon3ko/sqlite-raft/internal/raft"
	"github.com/kanonon3ko/sqlite-raft/internal/server"
	"github.com/kanonon3ko/sqlite-raft/internal/store"
)

// pgResult 是一次查询的结果或错误。
type pgResult struct {
	columns []string
	rows    [][]string
	tag     string
	errCode string
	errMsg  string
}

func (r *pgResult) Err() error {
	if r.errCode != "" {
		return fmt.Errorf("SQLSTATE %s: %s", r.errCode, r.errMsg)
	}
	return nil
}

// pgClient 是测试用的最小 PG wire 客户端。
type pgClient struct {
	conn   net.Conn
	r      *bufio.Reader
	pid    uint32
	secret uint32
}

func dialPG(t *testing.T, addr string) *pgClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	c := &pgClient{conn: conn, r: bufio.NewReader(conn)}

	// StartupMessage
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, protocolVersion)
	params := "user\x00sqlraft\x00database\x00sqlraft\x00\x00"
	payload = append(payload, params...)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)+4))
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write startup: %v", err)
	}

	// 读取握手消息直到 ReadyForQuery
	for {
		typ, body, err := c.readMsg()
		if err != nil {
			t.Fatalf("read handshake: %v", err)
		}
		switch typ {
		case msgReadyForQuery:
			return c
		case msgBackendKeyData:
			c.pid = binary.BigEndian.Uint32(body[0:4])
			c.secret = binary.BigEndian.Uint32(body[4:8])
		case msgErrorResponse:
			t.Fatalf("handshake error: %s", parseError(body))
		}
	}
}

// dialPGAuth 以 SCRAM-SHA-256 方式认证连接。
func dialPGAuth(t *testing.T, addr, user, password string) *pgClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	c := &pgClient{conn: conn, r: bufio.NewReader(conn)}

	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, protocolVersion)
	payload = append(payload, "user\x00"+user+"\x00database\x00sqlraft\x00\x00"...)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)+4))
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatalf("write startup: %v", err)
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write startup: %v", err)
	}

	// 1) AuthenticationSASL
	for {
		typ, body, err := c.readMsg()
		if err != nil {
			t.Fatalf("read auth: %v", err)
		}
		switch typ {
		case msgErrorResponse:
			t.Fatalf("auth error: %s", parseError(body))
		case msgAuthenticationOK:
			if binary.BigEndian.Uint32(body[0:4]) == 10 {
				goto sasl
			}
		case msgReadyForQuery:
			return c
		}
	}

sasl:
	// 2) SASLInitialResponse
	clientNonce := "clientnonce123"
	clientFirst := "n,,n=" + user + ",r=" + clientNonce
	init := append([]byte("SCRAM-SHA-256\x00"), u32(uint32(len(clientFirst)))...)
	init = append(init, clientFirst...)
	c.sendMsg(frontPassword, init)

	// 3) AuthenticationSASLContinue
	var serverFirst string
	for {
		typ, body, err := c.readMsg()
		if err != nil {
			t.Fatalf("read continue: %v", err)
		}
		switch typ {
		case msgErrorResponse:
			t.Fatalf("auth error: %s", parseError(body))
		case msgAuthenticationOK:
			if binary.BigEndian.Uint32(body[0:4]) == 11 {
				serverFirst = string(body[4:])
				goto saslContinue
			}
		}
	}

saslContinue:
	// 4) SASLResponse
	fields, err := parseScramFields(serverFirst)
	if err != nil {
		t.Fatalf("parse server-first: %v", err)
	}
	salt, err := base64.StdEncoding.DecodeString(fields["s"])
	if err != nil {
		t.Fatalf("decode salt: %v", err)
	}
	iter, err := strconv.Atoi(fields["i"])
	if err != nil {
		t.Fatalf("parse iterations: %v", err)
	}
	clientFirstBare := parseClientFirstBare(clientFirst)
	clientFinal := scramClientFinal(password, salt, iter, clientFirstBare, serverFirst,
		scramClientNonce(serverFirst))
	c.sendMsg(frontPassword, append(u32(uint32(len(clientFinal))), clientFinal...))

	// 5) AuthenticationSASLFinal + AuthenticationOk + 参数 + ReadyForQuery
	for {
		typ, body, err := c.readMsg()
		if err != nil {
			t.Fatalf("read final: %v", err)
		}
		switch typ {
		case msgErrorResponse:
			t.Fatalf("auth error: %s", parseError(body))
		case msgReadyForQuery:
			return c
		}
	}
}

func (c *pgClient) readMsg() (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	body := make([]byte, n-4)
	if _, err := io.ReadFull(c.r, body); err != nil {
		return 0, nil, err
	}
	return hdr[0], body, nil
}

func (c *pgClient) sendMsg(typ byte, payload []byte) {
	out := make([]byte, 0, 1+4+len(payload))
	out = append(out, typ)
	out = binary.BigEndian.AppendUint32(out, uint32(len(payload)+4))
	out = append(out, payload...)
	c.conn.Write(out)
}

func cstr(s string) []byte { return append([]byte(s), 0) }

// simpleQuery 发送 Q 并读取所有响应直到 ReadyForQuery。
func (c *pgClient) simpleQuery(sql string) ([]pgResult, error) {
	c.sendMsg(frontQuery, cstr(sql))
	var results []pgResult
	cur := &pgResult{}
	for {
		typ, body, err := c.readMsg()
		if err != nil {
			return nil, err
		}
		switch typ {
		case msgRowDescription:
			cur.columns = parseRowDescription(body)
		case msgDataRow:
			cur.rows = append(cur.rows, parseDataRow(body))
		case msgCommandComplete:
			cur.tag = strings.TrimRight(string(body), "\x00")
			results = append(results, *cur)
			cur = &pgResult{}
		case msgEmptyQueryResponse:
			results = append(results, *cur)
			cur = &pgResult{}
		case msgErrorResponse:
			cur.errCode, cur.errMsg = parseErrorFields(body)
			results = append(results, *cur)
			cur = &pgResult{}
		case msgReadyForQuery:
			return results, nil
		}
	}
}

// extendedQuery 发送 Parse/Bind/Execute/Sync，参数为文本格式。
func (c *pgClient) extendedQuery(sql string, params []string) (*pgResult, error) {
	c.sendMsg(frontParse, append(cstr(""), append(cstr(sql), u16(0)...)...))
	c.sendMsg(frontBind, buildBind("", params))
	c.sendMsg(frontExecute, append(cstr(""), u32(0)...))
	c.sendMsg(frontSync, nil)

	res := &pgResult{}
	for {
		typ, body, err := c.readMsg()
		if err != nil {
			return nil, err
		}
		switch typ {
		case msgParseComplete, msgBindComplete:
			continue
		case msgRowDescription:
			res.columns = parseRowDescription(body)
		case msgDataRow:
			res.rows = append(res.rows, parseDataRow(body))
		case msgCommandComplete:
			res.tag = strings.TrimRight(string(body), "\x00")
		case msgErrorResponse:
			res.errCode, res.errMsg = parseErrorFields(body)
		case msgReadyForQuery:
			return res, nil
		}
	}
}

func (c *pgClient) close() { c.conn.Close() }

func u16(v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return b[:]
}

func u32(v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return b[:]
}

func buildBind(portal string, params []string) []byte {
	var b []byte
	b = append(b, cstr(portal)...)
	b = append(b, cstr("")...) // 未命名语句
	b = append(b, u16(1)...)   // 1 个参数格式
	b = append(b, u16(0)...)   // 文本格式
	b = append(b, u16(uint16(len(params)))...)
	for _, p := range params {
		b = append(b, u32(uint32(len(p)))...)
		b = append(b, p...)
	}
	b = append(b, u16(0)...) // 0 个结果格式
	return b
}

func parseRowDescription(body []byte) []string {
	n := int(binary.BigEndian.Uint16(body[0:2]))
	cols := make([]string, 0, n)
	i := 2
	for j := 0; j < n; j++ {
		end := i
		for body[end] != 0 {
			end++
		}
		cols = append(cols, string(body[i:end]))
		i = end + 1 + 4 + 2 + 4 + 2 + 4 + 2
	}
	return cols
}

func parseDataRow(body []byte) []string {
	n := int(binary.BigEndian.Uint16(body[0:2]))
	vals := make([]string, 0, n)
	i := 2
	for j := 0; j < n; j++ {
		ln := int32(binary.BigEndian.Uint32(body[i:]))
		i += 4
		if ln < 0 {
			vals = append(vals, "<NULL>")
			continue
		}
		vals = append(vals, string(body[i:i+int(ln)]))
		i += int(ln)
	}
	return vals
}

func parseError(body []byte) string {
	_, msg := parseErrorFields(body)
	return msg
}

func parseErrorFields(body []byte) (code, msg string) {
	i := 0
	for i < len(body) {
		typ := body[i]
		i++
		if typ == 0 {
			break
		}
		end := i
		for end < len(body) && body[end] != 0 {
			end++
		}
		text := string(body[i:end])
		i = end + 1
		switch typ {
		case 'C':
			code = text
		case 'M':
			msg = text
		}
	}
	return code, msg
}

// ---------- 测试环境 ----------

func startPGServer(t *testing.T) (string, *raft.Node, *store.Store) {
	return startPGServerUsers(t, nil)
}

// startPGServerUsers 启动带用户认证的 PG wire 服务。
func startPGServerUsers(t *testing.T, users map[string]string) (string, *raft.Node, *store.Store) {
	t.Helper()
	st, err := store.Open("")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	node, err := raft.New(raft.Config{
		ID:                0,
		ElectionTimeout:   100 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		Apply: func(ctx context.Context, index uint64, entry *logpb.LogEntry) (*raft.ApplyResult, error) {
			outcome, err := st.ApplyEntry(ctx, index, entry)
			if err != nil {
				return nil, err
			}
			return &raft.ApplyResult{
				RowsAffected: outcome.RowsAffected,
				LastInsertID: outcome.LastInsertID,
				Columns:      outcomeColumnNames(outcome),
				RowTypes:     outcomeColumnTypes(outcome),
				Rows:         outcome.Rows,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	raftpb.RegisterRaftServiceServer(gs, node)
	api := server.New(node, st, nil)
	sqlraftpb.RegisterSqlRaftServer(gs, api)
	go gs.Serve(lis)
	node.Start()
	t.Cleanup(func() {
		node.Stop()
		gs.Stop()
		st.Close()
	})

	pgLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen pg: %v", err)
	}
	pg := New(api, st, "sqlraft", "sqlraft", users, nil)
	go pg.Serve(pgLis)
	t.Cleanup(func() { pgLis.Close() })

	deadline := time.Now().Add(3 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("node did not become leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return pgLis.Addr().String(), node, st
}

// TestScramAuthentication 验证 SCRAM-SHA-256 认证成功后可执行查询。
func TestScramAuthentication(t *testing.T) {
	addr, _, _ := startPGServerUsers(t, map[string]string{"alice": "s3cret"})
	c := dialPGAuth(t, addr, "alice", "s3cret")
	defer c.close()

	res, err := c.simpleQuery("SELECT 1 AS ok")
	if err != nil {
		t.Fatalf("query after auth: %v", err)
	}
	mustResult(t, res[0])
	if row := firstRow(res[0]); len(row) != 1 || row[0] != "1" {
		t.Fatalf("row = %v", res[0].rows)
	}
}

// TestScramWrongPassword 验证错误密码被拒绝且无法继续查询。
func TestScramWrongPassword(t *testing.T) {
	addr, _, _ := startPGServerUsers(t, map[string]string{"alice": "s3cret"})
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	c := &pgClient{conn: conn, r: bufio.NewReader(conn)}

	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, protocolVersion)
	payload = append(payload, "user\x00alice\x00database\x00sqlraft\x00\x00"...)
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)+4))
	conn.Write(hdr[:])
	conn.Write(payload)

	// 读 AuthenticationSASL
	for {
		typ, body, err := c.readMsg()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if typ == msgAuthenticationOK && binary.BigEndian.Uint32(body[0:4]) == 10 {
			break
		}
	}
	clientFirst := "n,,n=alice,r=nonce123"
	init := append([]byte("SCRAM-SHA-256\x00"), u32(uint32(len(clientFirst)))...)
	init = append(init, clientFirst...)
	c.sendMsg(frontPassword, init)

	var serverFirst string
	for {
		typ, body, err := c.readMsg()
		if err != nil {
			t.Fatalf("read continue: %v", err)
		}
		if typ == msgAuthenticationOK && binary.BigEndian.Uint32(body[0:4]) == 11 {
			serverFirst = string(body[4:])
			break
		}
	}
	fields, _ := parseScramFields(serverFirst)
	salt, _ := base64.StdEncoding.DecodeString(fields["s"])
	iter, _ := strconv.Atoi(fields["i"])
	badFinal := scramClientFinal("wrong", salt, iter, parseClientFirstBare(clientFirst),
		serverFirst, scramClientNonce(serverFirst))
	c.sendMsg(frontPassword, append(u32(uint32(len(badFinal))), badFinal...))

	// 应收到 FATAL 错误（28000）
	for {
		typ, body, err := c.readMsg()
		if err != nil {
			return // 服务器关闭连接也可接受
		}
		if typ == msgErrorResponse {
			code, _ := parseErrorFields(body)
			if code != "28000" {
				t.Fatalf("error code = %s, want 28000", code)
			}
			return
		}
		if typ == msgReadyForQuery {
			t.Fatal("authentication succeeded with wrong password")
		}
	}
}

func outcomeColumnNames(o *store.ApplyOutcome) []string {
	var out []string
	for _, c := range o.Columns {
		out = append(out, c.Name)
	}
	return out
}

func outcomeColumnTypes(o *store.ApplyOutcome) []string {
	var out []string
	for _, c := range o.Columns {
		out = append(out, c.Type)
	}
	return out
}

func mustResult(t *testing.T, res pgResult) {
	t.Helper()
	if err := res.Err(); err != nil {
		t.Fatalf("query error: %v", err)
	}
}

func firstRow(res pgResult) []string {
	if len(res.rows) == 0 {
		return nil
	}
	return res.rows[0]
}

// ---------- 协议集成测试 ----------

func TestSimpleQuery(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	res, err := c.simpleQuery("SELECT 1 AS n")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("results = %d, want 1", len(res))
	}
	mustResult(t, res[0])
	if res[0].tag != "SELECT 1" {
		t.Fatalf("tag = %q, want SELECT 1", res[0].tag)
	}
	if len(res[0].columns) != 1 || res[0].columns[0] != "n" {
		t.Fatalf("columns = %v", res[0].columns)
	}
	if row := firstRow(res[0]); len(row) != 1 || row[0] != "1" {
		t.Fatalf("row = %v", res[0].rows)
	}
}

func TestWritesAndReads(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	res, _ := c.simpleQuery("CREATE TABLE t (id SERIAL PRIMARY KEY, name VARCHAR(50), ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
	mustResult(t, res[0])
	if res[0].tag != "CREATE TABLE" {
		t.Fatalf("create tag = %q", res[0].tag)
	}

	res, _ = c.simpleQuery("INSERT INTO t (name) VALUES ('a')")
	mustResult(t, res[0])
	if res[0].tag != "INSERT 0 1" {
		t.Fatalf("insert tag = %q", res[0].tag)
	}

	res, _ = c.simpleQuery("SELECT id, name FROM t")
	mustResult(t, res[0])
	if len(res[0].rows) != 1 {
		t.Fatalf("rows = %v", res[0].rows)
	}
	row := res[0].rows[0]
	if row[0] != "1" || row[1] != "a" {
		t.Fatalf("row = %v", row)
	}
}

func TestExtendedProtocolWithParams(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	if res, _ := c.simpleQuery("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); res[0].Err() != nil {
		t.Fatalf("create: %v", res[0].Err())
	}

	res, err := c.extendedQuery("INSERT INTO t (id, name) VALUES ($1, $2)", []string{"1", "hello"})
	if err != nil {
		t.Fatalf("extended insert: %v", err)
	}
	if err := res.Err(); err != nil {
		t.Fatalf("extended insert error: %v", err)
	}
	if res.tag != "INSERT 0 1" {
		t.Fatalf("tag = %q", res.tag)
	}

	res, err = c.extendedQuery("SELECT name FROM t WHERE id = $1", []string{"1"})
	if err != nil {
		t.Fatalf("extended select: %v", err)
	}
	if err := res.Err(); err != nil {
		t.Fatalf("extended select error: %v", err)
	}
	if row := firstRow(*res); len(row) != 1 || row[0] != "hello" {
		t.Fatalf("row = %v", res.rows)
	}
}

// TestDescribeParameterizedStatement 验证 Describe 返回参数类型与结果列信息。
func TestDescribeParameterizedStatement(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	if res, _ := c.simpleQuery("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); res[0].Err() != nil {
		t.Fatalf("create: %v", res[0].Err())
	}

	// Parse + Describe(statement) + Sync
	c.sendMsg(frontParse, append(cstr(""), append(cstr("SELECT name FROM t WHERE id = $1"), u16(0)...)...))
	c.sendMsg(frontDescribe, append([]byte{'S'}, cstr("")...))
	c.sendMsg(frontSync, nil)

	var paramOIDs []uint32
	var cols []string
	for {
		typ, body, err := c.readMsg()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		switch typ {
		case msgParseComplete:
			continue
		case msgParameterDescription:
			n := int(binary.BigEndian.Uint16(body[0:2]))
			for i := 0; i < n; i++ {
				paramOIDs = append(paramOIDs, binary.BigEndian.Uint32(body[2+i*4:]))
			}
		case msgRowDescription:
			cols = parseRowDescription(body)
		case msgReadyForQuery:
			goto done
		case msgErrorResponse:
			t.Fatalf("describe error: %s", parseError(body))
		}
	}
done:
	if len(paramOIDs) != 1 || paramOIDs[0] != oidText {
		t.Fatalf("param oids = %v, want [TEXT]", paramOIDs)
	}
	if len(cols) != 1 || cols[0] != "name" {
		t.Fatalf("columns = %v, want [name]", cols)
	}
}

func TestReturning(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	c.simpleQuery("CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT)")
	res, _ := c.simpleQuery("INSERT INTO t (v) VALUES ('x') RETURNING id")
	mustResult(t, res[0])
	if len(res[0].rows) != 1 {
		t.Fatalf("returning rows = %v", res[0].rows)
	}
	if got := res[0].rows[0][0]; got != "1" {
		t.Fatalf("returning id = %q, want 1", got)
	}
}

func TestErrors(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	res, _ := c.simpleQuery("SELECT * FROM missing_table")
	if err := res[0].Err(); err == nil {
		t.Fatal("expected error for missing table")
	} else if res[0].errCode != "42P01" {
		t.Fatalf("sqlstate = %s, want 42P01 (%v)", res[0].errCode, err)
	}

	c.simpleQuery("CREATE TABLE u (k TEXT PRIMARY KEY)")
	c.simpleQuery("INSERT INTO u (k) VALUES ('x')")
	res, _ = c.simpleQuery("INSERT INTO u (k) VALUES ('x')")
	if res[0].errCode != "23505" {
		t.Fatalf("sqlstate = %s, want 23505", res[0].errCode)
	}
}

func TestShims(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	cases := []struct {
		sql     string
		tag     string
		col     string
		wantRow string
	}{
		{"SET extra_float_digits = 3", "SET", "", ""},
		{"BEGIN", "BEGIN", "", ""},
		{"COMMIT", "COMMIT", "", ""},
		{"SHOW server_version", "", "server_version", "16.0 (sqlraft)"},
		{"SELECT version()", "", "version", "PostgreSQL 16.0 (sqlraft)"},
		{"SELECT current_database()", "", "current_database", "sqlraft"},
		{"SELECT current_schema()", "", "current_schema", "public"},
		{"SELECT pg_catalog.set_config('search_path', '', false)", "", "set_config", "t"},
	}
	for _, tc := range cases {
		res, err := c.simpleQuery(tc.sql)
		if err != nil {
			t.Fatalf("%q: %v", tc.sql, err)
		}
		if len(res) == 0 {
			t.Fatalf("%q: no result", tc.sql)
		}
		mustResult(t, res[0])
		if tc.tag != "" {
			if res[0].tag != tc.tag {
				t.Fatalf("%q: tag = %q, want %q", tc.sql, res[0].tag, tc.tag)
			}
			continue
		}
		if len(res[0].columns) != 1 || res[0].columns[0] != tc.col {
			t.Fatalf("%q: columns = %v, want [%s]", tc.sql, res[0].columns, tc.col)
		}
		if row := firstRow(res[0]); len(row) != 1 || row[0] != tc.wantRow {
			t.Fatalf("%q: row = %v, want [%s]", tc.sql, res[0].rows, tc.wantRow)
		}
	}
}

func TestDeterministicNowViaPG(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	c.simpleQuery("CREATE TABLE t (ts TEXT)")
	res, _ := c.simpleQuery("INSERT INTO t (ts) VALUES (NOW())")
	mustResult(t, res[0])

	// 写路径的 NOW() 被 raft 改写为字面量，读出来是时间字符串
	res, _ = c.simpleQuery("SELECT ts FROM t")
	mustResult(t, res[0])
	ts := firstRow(res[0])[0]
	if len(ts) < 19 || ts[4] != '-' {
		t.Fatalf("ts = %q, want timestamp literal", ts)
	}

	// 读路径的 NOW() 翻译为 SQLite 时间函数
	res, _ = c.simpleQuery("SELECT NOW()")
	mustResult(t, res[0])
	if row := firstRow(res[0]); len(row) != 1 || len(row[0]) < 19 {
		t.Fatalf("SELECT NOW() = %v", res[0].rows)
	}
}

func TestMultiStatementQuery(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	res, err := c.simpleQuery("SELECT 1; SELECT 2")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("results = %d, want 2", len(res))
	}
	if got := firstRow(res[0])[0]; got != "1" {
		t.Fatalf("first = %q", got)
	}
	if got := firstRow(res[1])[0]; got != "2" {
		t.Fatalf("second = %q", got)
	}
}

// TestCatalogQuery 验证 pg_catalog 元查询（psql 元命令的底层 SQL）能执行。
func TestCatalogQuery(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	c.simpleQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	c.simpleQuery("CREATE VIEW v_users AS SELECT id FROM users")

	// 模拟 psql \dt 的底层查询（简化）
	res, _ := c.simpleQuery(`SELECT n.nspname, c.relname, c.relkind,
		pg_catalog.pg_get_userbyid(c.relowner) AS owner
	FROM pg_catalog.pg_class c
	LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relkind IN ('r','v') AND n.nspname = 'public'
	ORDER BY c.relname`)
	if len(res) == 0 {
		t.Fatal("no catalog result")
	}
	mustResult(t, res[0])
	names := map[string]bool{}
	for _, row := range res[0].rows {
		names[row[1]] = true
	}
	if !names["users"] || !names["v_users"] {
		t.Fatalf("catalog rows = %v, want users and v_users", res[0].rows)
	}
	if row := firstRow(res[0]); row[3] != "sqlraft" {
		t.Fatalf("owner = %q, want sqlraft", row[3])
	}
}

// TestCurrentUser 验证 current_user 被翻译为连接用户名。
func TestCurrentUser(t *testing.T) {
	addr, _, _ := startPGServerUsers(t, map[string]string{"alice": "s3cret"})
	c := dialPGAuth(t, addr, "alice", "s3cret")
	defer c.close()

	res, _ := c.simpleQuery("SELECT current_user, session_user")
	mustResult(t, res[0])
	row := firstRow(res[0])
	if len(row) != 2 || row[0] != "alice" || row[1] != "alice" {
		t.Fatalf("row = %v, want [alice alice]", row)
	}
}

func TestDialectTranslation(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	// :: 与 ILIKE 翻译
	res, _ := c.simpleQuery("SELECT 'AbC'::text ILIKE 'abc'")
	mustResult(t, res[0])
	if got := firstRow(res[0])[0]; got != "1" {
		t.Fatalf("ILIKE result = %q, want 1", got)
	}

	// $N 参数翻译（extended protocol 已覆盖），这里验证编号参数绑定
	c.simpleQuery("CREATE TABLE n (a INTEGER, b INTEGER)")
	eres, err := c.extendedQuery("INSERT INTO n (a, b) VALUES ($2, $1)", []string{"10", "20"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := eres.Err(); err != nil {
		t.Fatalf("insert error: %v", err)
	}
	res, _ = c.simpleQuery("SELECT a, b FROM n")
	mustResult(t, res[0])
	if row := firstRow(res[0]); row[0] != "20" || row[1] != "10" {
		t.Fatalf("row = %v, want [20 10]（$2 绑定到 a）", row)
	}
}

// ---------- 跨语句事务 ----------

func TestTransactionAtomicCommit(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	c.simpleQuery("CREATE TABLE tx (id SERIAL PRIMARY KEY, v TEXT)")
	// 一个 Q 消息里完成 BEGIN/INSERT/INSERT/COMMIT，验证同一连接内状态保持
	res, err := c.simpleQuery("BEGIN; INSERT INTO tx (v) VALUES ('a'); INSERT INTO tx (v) VALUES ('b'); COMMIT")
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	if len(res) != 4 {
		t.Fatalf("responses = %d, want 4", len(res))
	}
	if res[0].tag != "BEGIN" || res[3].tag != "COMMIT" {
		t.Fatalf("tags = %v", []string{res[0].tag, res[3].tag})
	}

	res, _ = c.simpleQuery("SELECT count(*) FROM tx")
	mustResult(t, res[0])
	if got := firstRow(res[0])[0]; got != "2" {
		t.Fatalf("count = %q, want 2", got)
	}
}

func TestTransactionRollback(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	c.simpleQuery("CREATE TABLE tx (id SERIAL PRIMARY KEY, v TEXT)")
	res, _ := c.simpleQuery("BEGIN")
	mustResult(t, res[0])
	c.simpleQuery("INSERT INTO tx (v) VALUES ('x')")
	res, _ = c.simpleQuery("ROLLBACK")
	mustResult(t, res[0])

	res, _ = c.simpleQuery("SELECT count(*) FROM tx")
	mustResult(t, res[0])
	if got := firstRow(res[0])[0]; got != "0" {
		t.Fatalf("count = %q, want 0（ROLLBACK 后无数据）", got)
	}
}

func TestTransactionAtomicRollbackOnError(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	c.simpleQuery("CREATE TABLE tx (id INTEGER PRIMARY KEY, v TEXT)")
	c.simpleQuery("INSERT INTO tx VALUES (1, 'a')")
	c.simpleQuery("BEGIN")
	c.simpleQuery("INSERT INTO tx VALUES (2, 'b')")
	// 第二条违反唯一约束：COMMIT 时整段回滚，第一条也不得生效
	res, _ := c.simpleQuery("INSERT INTO tx VALUES (1, 'dup')")
	mustResult(t, res[0]) // 缓冲阶段不报错
	res, _ = c.simpleQuery("COMMIT")
	if err := res[0].Err(); err == nil {
		t.Fatal("COMMIT should fail when a buffered statement violates constraints")
	}

	res, _ = c.simpleQuery("SELECT count(*) FROM tx")
	mustResult(t, res[0])
	if got := firstRow(res[0])[0]; got != "1" {
		t.Fatalf("count = %q, want 1（整段回滚，id=2 也不应存在）", got)
	}
}

func TestTransactionBufferedWriteResponse(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	c.simpleQuery("CREATE TABLE tx (id SERIAL PRIMARY KEY, v TEXT)")
	c.simpleQuery("BEGIN")
	res, _ := c.simpleQuery("INSERT INTO tx (v) VALUES ('x')")
	mustResult(t, res[0])
	if res[0].tag != "INSERT 0 0" {
		t.Fatalf("buffered insert tag = %q, want INSERT 0 0（影响行数在 COMMIT 前不可用）", res[0].tag)
	}
	c.simpleQuery("COMMIT")
}

func TestTransactionParamRendering(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	c.simpleQuery("CREATE TABLE tx (id INTEGER PRIMARY KEY, v TEXT)")
	c.simpleQuery("BEGIN")
	// 参数化语句在事务内被代入字面量缓冲
	res, err := c.extendedQuery("INSERT INTO tx VALUES ($1, $2)", []string{"7", "O'Brien"})
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := res.Err(); err != nil {
		t.Fatalf("insert error: %v", err)
	}
	c.simpleQuery("COMMIT")

	qres, _ := c.simpleQuery("SELECT v FROM tx WHERE id = 7")
	mustResult(t, qres[0])
	if row := firstRow(qres[0]); len(row) != 1 || row[0] != "O'Brien" {
		t.Fatalf("row = %v，want [O'Brien]（单引号转义正确）", qres[0].rows)
	}
}

func TestTransactionDiscardOnClose(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	c.simpleQuery("CREATE TABLE tx (id INTEGER PRIMARY KEY, v TEXT)")
	c.simpleQuery("BEGIN")
	c.simpleQuery("INSERT INTO tx VALUES (1, 'x')")
	c.close() // 未提交即断开：缓冲自动丢弃

	c2 := dialPG(t, addr)
	defer c2.close()
	res, _ := c2.simpleQuery("SELECT count(*) FROM tx")
	mustResult(t, res[0])
	if got := firstRow(res[0])[0]; got != "0" {
		t.Fatalf("count = %q, want 0（断开的未提交事务应丢弃）", got)
	}
}

func TestTransactionNestedBegin(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	res, _ := c.simpleQuery("BEGIN")
	mustResult(t, res[0])
	res, _ = c.simpleQuery("BEGIN")
	if res[0].errCode != "25001" {
		t.Fatalf("nested BEGIN sqlstate = %s, want 25001", res[0].errCode)
	}
	c.simpleQuery("ROLLBACK")
}

// TestTransactionReproPsql 复现 psql 交互模式：同一连接、逐条 Q 消息。
func TestTransactionReproPsql(t *testing.T) {
	addr, _, _ := startPGServer(t)
	c := dialPG(t, addr)
	defer c.close()

	c.simpleQuery("CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT, n INTEGER)")
	res, _ := c.simpleQuery("BEGIN")
	t.Logf("BEGIN tag=%q", res[0].tag)
	res, _ = c.simpleQuery("INSERT INTO t (v, n) VALUES ('ghost3', 101)")
	t.Logf("INSERT tag=%q err=%q", res[0].tag, res[0].errCode)
	res, _ = c.simpleQuery("ROLLBACK")
	t.Logf("ROLLBACK tag=%q", res[0].tag)
	res, _ = c.simpleQuery("SELECT count(*) FROM t")
	if got := firstRow(res[0])[0]; got != "0" {
		t.Fatalf("count = %q, want 0（ROLLBACK 应丢弃缓冲写入）", got)
	}
}

// ---------- 单元测试 ----------

func TestSplitStatements(t *testing.T) {
	cases := []struct {
		sql  string
		want []string
	}{
		{"SELECT 1; SELECT 2", []string{"SELECT 1", "SELECT 2"}},
		{"SELECT ';'", []string{"SELECT ';'"}},
		{"SELECT 1;;SELECT 2;", []string{"SELECT 1", "SELECT 2"}},
		{"SELECT 'a''b'; SELECT 2", []string{"SELECT 'a''b'", "SELECT 2"}},
		{"", nil},
	}
	for _, tc := range cases {
		got := splitStatements(tc.sql)
		if len(got) != len(tc.want) {
			t.Errorf("%q: got %v, want %v", tc.sql, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("%q: got %v, want %v", tc.sql, got, tc.want)
				break
			}
		}
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		sql  string
		kind stmtKind
	}{
		{"SELECT 1", kindRead},
		{"WITH x AS (SELECT 1) SELECT * FROM x", kindRead},
		{"WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", kindWrite},
		{"INSERT INTO t VALUES (1)", kindWrite},
		{"UPDATE t SET a = 1", kindWrite},
		{"CREATE TABLE t (a INT)", kindWrite},
		{"SET search_path = public", kindNoop},
		{"BEGIN", kindNoop},
		{"COMMIT", kindNoop},
		{"PRAGMA table_info(t)", kindRead},
	}
	for _, tc := range cases {
		if got := classify(tc.sql); got != tc.kind {
			t.Errorf("%q: kind = %v, want %v", tc.sql, got, tc.kind)
		}
	}
}

func TestTranslateDialect(t *testing.T) {
	cases := []struct{ in, want string }{
		{"SELECT $1, $2", "SELECT ?1, ?2"},
		{"SELECT 1::int", "SELECT CAST(1 AS INTEGER)"},
		{"SELECT a::text FROM t", "SELECT CAST(a AS TEXT) FROM t"},
		{"SELECT 'x' ILIKE 'X'", "SELECT 'x' LIKE 'X'"},
	}
	for _, tc := range cases {
		got := translateDialect(tc.in)
		if got != tc.want {
			t.Errorf("%q => %q, want %q", tc.in, got, tc.want)
		}
	}

	ddl := []struct{ in, want string }{
		{"CREATE TABLE t (id SERIAL PRIMARY KEY, name VARCHAR(50), ok BOOLEAN)",
			"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT(50), ok INTEGER)"},
		{"CREATE TABLE t (c CHAR(4), b BYTEA, ts TIMESTAMP)",
			"CREATE TABLE t (c TEXT(4), b BLOB, ts TEXT)"},
	}
	for _, tc := range ddl {
		got := translateDdlTypes(tc.in)
		if got != tc.want {
			t.Errorf("%q => %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestTranslateReadFunctions(t *testing.T) {
	got := translateReadFunctions("SELECT NOW()")
	if !strings.Contains(got, "strftime") {
		t.Fatalf("NOW() not translated: %q", got)
	}
	// 字符串内的 NOW() 不翻译
	got = translateReadFunctions("SELECT 'NOW()'")
	if got != "SELECT 'NOW()'" {
		t.Fatalf("string NOW() translated: %q", got)
	}
}

func TestCountParams(t *testing.T) {
	if n := countParams("SELECT $1, $2, $1"); n != 2 {
		t.Fatalf("count = %d, want 2", n)
	}
	if n := countParams("SELECT 1"); n != 0 {
		t.Fatalf("count = %d, want 0", n)
	}
}

func TestPgColumnType(t *testing.T) {
	cases := []struct {
		decl string
		oid  uint32
	}{
		{"INTEGER", oidInt8},
		{"BIGINT", oidInt8},
		{"TEXT", oidText},
		{"VARCHAR", oidText},
		{"REAL", oidFloat8},
		{"BLOB", oidBytea},
		{"BOOLEAN", oidBool},
		{"NUMERIC", oidNumeric},
	}
	for _, tc := range cases {
		if oid, _ := pgColumnType(tc.decl); oid != tc.oid {
			t.Errorf("%s: oid = %d, want %d", tc.decl, oid, tc.oid)
		}
	}
}
