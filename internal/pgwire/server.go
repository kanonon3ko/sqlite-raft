// 服务端主体：连接握手、简单/扩展查询协议、语句路由。
package pgwire

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
	"github.com/kanonon3ko/sqlite-raft/internal/raft"
	"github.com/kanonon3ko/sqlite-raft/internal/store"
)

// Executor 是 pgwire 依赖的写路径接口，由 server.Server 实现。
type Executor interface {
	ExecuteRaw(ctx context.Context, sql string, params []*sqlraftpb.Value) (*raft.ApplyResult, error)
	// ExecuteTx 把多语句事务整段作为一条 Raft 日志原子提交。
	ExecuteTx(ctx context.Context, statements []string) error
}

// maxTxStatements 是单个会话事务缓冲的最大语句数，防止无限增长。
const maxTxStatements = 10000

// txState 是一个会话内未提交事务的缓冲。
type txState struct {
	stmts []string // 已翻译、参数已代入的写语句
}

// Server 是 PostgreSQL wire protocol 服务。
type Server struct {
	executor Executor
	store    *store.Store
	logger   *log.Logger
	dbName   string
	user     string
	started  time.Time
	users    map[string]*credential // 非空时启用 SCRAM-SHA-256 认证
	catalog  *Catalog

	mu      sync.Mutex
	conns   map[uint32]*session
	nextPID uint32
}

// New 创建 PG wire 服务。users 为 "用户名 -> 明文密码"；为空时使用 trust 认证。
func New(executor Executor, st *store.Store, dbName, user string,
	users map[string]string, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	creds := make(map[string]*credential, len(users))
	for u, p := range users {
		creds[u] = newCredential(p)
	}
	srv := &Server{
		executor: executor,
		store:    st,
		logger:   logger,
		dbName:   dbName,
		user:     user,
		started:  time.Now(),
		conns:    make(map[uint32]*session),
		nextPID:  uint32(1000 + rand.Intn(10000)),
		users:    creds,
	}
	if c, err := NewCatalog(st); err == nil {
		srv.catalog = c
	} else {
		logger.Printf("pgwire: catalog disabled: %v", err)
	}
	return srv
}

// Serve 在监听器上接受 PG 连接。
func (s *Server) Serve(lis net.Listener) error {
	for {
		conn, err := lis.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) hostPort(lis net.Listener) (string, string) {
	host, port, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		return "127.0.0.1", ""
	}
	return host, port
}

// handleConn 处理一条 PG 连接。
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	// ---- 启动握手 ----
	r := bufio.NewReader(conn)
	var params map[string]string
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return
		}
		n := int32FromBytes(lenBuf[:])
		if n < 8 || n > 1<<20 {
			s.logger.Printf("pgwire: bad startup length %d", n)
			return
		}
		payload := make([]byte, n-4)
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}
		code := int32FromBytes(payload[:4])
		switch code {
		case sslRequestCode:
			// 不支持 SSL：回 N 后继续等新的 StartupMessage
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return
			}
			continue
		case cancelReqCode:
			if len(payload) >= 12 {
				pid := uint32FromBytes(payload[4:8])
				secret := uint32FromBytes(payload[8:12])
				s.cancelSession(pid, secret)
			}
			return // 取消请求的连接立即关闭
		case protocolVersion:
			params = parseStartupParams(payload[4:])
		default:
			s.logger.Printf("pgwire: unsupported protocol version %d", code)
			return
		}
		break
	}

	sess := s.newSession(conn, r, params)
	s.register(sess)
	defer s.unregister(sess)
	defer sess.cancel()

	if err := sess.handshake(); err != nil {
		s.logger.Printf("pgwire: handshake: %v", err)
		sess.w.Flush() // 让已写入的错误响应（如认证失败）送达客户端
		return
	}
	if err := sess.w.Flush(); err != nil {
		return
	}
	sess.run()
}

func (s *Server) newSession(conn net.Conn, r *bufio.Reader, params map[string]string) *session {
	pid := s.nextPID
	s.nextPID++
	secret := uint32(rand.Int31())
	ctx, cancel := context.WithCancel(context.Background())
	host, port := "127.0.0.1", ""
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		host = addr.IP.String()
		port = strconv.Itoa(addr.Port)
	}
	dbName := s.dbName
	user := s.user
	if v := params["database"]; v != "" {
		dbName = v
	}
	if v := params["user"]; v != "" {
		user = v
	}
	return &session{
		srv:     s,
		conn:    conn,
		r:       r,
		w:       bufio.NewWriter(conn),
		pid:     pid,
		secret:  secret,
		dbName:  dbName,
		user:    user,
		host:    host,
		port:    port,
		startup: params,
		ctx:     ctx,
		cancel:  cancel,
		stmts:   make(map[string]*preparedStmt),
		portals: make(map[string]*boundPortal),
	}
}

func (s *Server) register(sess *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[sess.pid] = sess
}

func (s *Server) unregister(sess *session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, sess.pid)
}

// cancelSession 按 (pid, secret) 取消一条连接上的查询。
func (s *Server) cancelSession(pid, secret uint32) {
	s.mu.Lock()
	sess := s.conns[pid]
	s.mu.Unlock()
	if sess == nil || sess.secret != secret {
		return
	}
	sess.cancel()
	sess.conn.Close()
}

// ---------- 会话 ----------

type preparedStmt struct {
	sql string
}

type boundPortal struct {
	stmt   *preparedStmt
	params []*sqlraftpb.Value
	// 分页执行状态
	cachedRows [][]any
	cachedCols []store.Column
	offset     int
	tag        string
}

type session struct {
	srv     *Server
	conn    net.Conn
	r       *bufio.Reader
	w       *bufio.Writer
	pid     uint32
	secret  uint32
	dbName  string
	user    string
	host    string
	port    string
	startup map[string]string
	ctx     context.Context
	cancel  context.CancelFunc

	stmts   map[string]*preparedStmt
	portals map[string]*boundPortal
	inError bool
	tx      *txState
}

func (s *session) handshake() error {
	if len(s.srv.users) > 0 {
		if err := s.scramHandshake(); err != nil {
			return err
		}
	} else {
		if err := s.write(authOK()); err != nil {
			return err
		}
	}
	statuses := []struct{ k, v string }{
		{"server_version", "16.0 (sqlraft)"},
		{"server_encoding", "UTF8"},
		{"client_encoding", "UTF8"},
		{"DateStyle", "ISO, MDY"},
		{"TimeZone", "UTC"},
		{"integer_datetimes", "on"},
		{"standard_conforming_strings", "on"},
		{"extra_float_digits", "3"},
		{"application_name", s.startup["application_name"]},
	}
	for _, p := range statuses {
		if err := s.write(parameterStatus(p.k, p.v)); err != nil {
			return err
		}
	}
	if err := s.write(backendKeyData(s.pid, s.secret)); err != nil {
		return err
	}
	return s.write(readyForQuery('I'))
}

// scramHandshake 执行 SCRAM-SHA-256 认证交换（RFC 5802）。
func (s *session) scramHandshake() error {
	// AuthenticationSASL：机制列表
	var p buf
	p.i32(10) // SASL
	p.cstr("SCRAM-SHA-256")
	p.u8(0)
	if err := s.write(msg(msgAuthenticationOK, p.b)); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}

	// SASLInitialResponse：mechanism\0 + int32 len + client-first
	m, err := readFrontend(s.r)
	if err != nil {
		return err
	}
	if m.typ != frontPassword {
		return errors.New("scram: expected SASLInitialResponse")
	}
	clientFirst, err := parseSASLInitial(m.body)
	if err != nil {
		s.write(errorResponse(field{'S', "FATAL"}, field{'C', "28000"},
			field{'M', err.Error()}))
		return err
	}

	cred, ok := s.srv.users[s.user]
	if !ok {
		s.write(errorResponse(field{'S', "FATAL"}, field{'C', "28000"},
			field{'M', fmt.Sprintf("password authentication failed for user %q", s.user)}))
		return errors.New("scram: unknown user")
	}

	clientFirstBare := parseClientFirstBare(clientFirst)
	serverFirst, _, err := cred.serverFirst(scramClientNonce(clientFirst))
	if err != nil {
		return err
	}

	// AuthenticationSASLContinue
	var cont buf
	cont.i32(11)
	cont.bytes([]byte(serverFirst)) // SASL 消息是裸字节，无结尾 NUL
	if err := s.write(msg(msgAuthenticationOK, cont.b)); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}

	// SASLResponse：int32 len + client-final
	m2, err := readFrontend(s.r)
	if err != nil {
		return err
	}
	if m2.typ != frontPassword {
		return errors.New("scram: expected SASLResponse")
	}
	clientFinal, err := parseSASLResponse(m2.body)
	if err != nil {
		return err
	}
	serverFinal, err := cred.verifyClientFinal(clientFirstBare, serverFirst, clientFinal)
	if err != nil {
		s.write(errorResponse(field{'S', "FATAL"}, field{'C', "28000"},
			field{'M', fmt.Sprintf("password authentication failed for user %q", s.user)}))
		return err
	}

	// AuthenticationSASLFinal
	var fin buf
	fin.i32(12)
	fin.bytes([]byte(serverFinal))
	if err := s.write(msg(msgAuthenticationOK, fin.b)); err != nil {
		return err
	}
	return s.write(authOK())
}

func (s *session) run() {
	for {
		m, err := readFrontend(s.r)
		if err != nil {
			return
		}
		if s.handle(m) {
			return
		}
		if err := s.w.Flush(); err != nil {
			return
		}
	}
}

// handle 处理一条前端消息；返回 true 表示连接应关闭。
func (s *session) handle(m *frontendMessage) bool {
	if s.inError && m.typ != frontSync && m.typ != frontFlush && m.typ != frontTerminate {
		return m.typ == frontTerminate // 错误状态下忽略其余消息
	}
	switch m.typ {
	case frontQuery:
		s.handleSimpleQuery(m.sql)
	case frontParse:
		s.stmts[m.stmtName] = &preparedStmt{sql: m.query}
		s.write(parseComplete())
	case frontBind:
		stmt := s.stmts[m.stmtName]
		if stmt == nil {
			s.sendError(fmt.Errorf("prepared statement %q does not exist", m.stmtName))
			return false
		}
		params := make([]*sqlraftpb.Value, len(m.params))
		for i, p := range m.params {
			params[i] = bindParamToValue(p)
		}
		s.portals[m.portal] = &boundPortal{stmt: stmt, params: params}
		s.write(bindComplete())
	case frontDescribe:
		s.handleDescribe(m)
	case frontExecute:
		s.handleExecute(m.portal, m.maxRows)
	case frontClose:
		if m.closeType == 'S' {
			delete(s.stmts, m.stmtName)
		} else {
			delete(s.portals, m.stmtName)
		}
		s.write(closeComplete())
	case frontFlush:
		// 每轮消息后统一 flush
	case frontSync:
		s.inError = false
		delete(s.stmts, "")
		delete(s.portals, "")
		s.write(readyForQuery('I'))
	case frontTerminate:
		return true
	}
	return false
}

// handleSimpleQuery 处理一条简单查询（可含多条语句）。
func (s *session) handleSimpleQuery(sql string) {
	stmts := splitStatements(sql)
	if len(stmts) == 0 {
		s.write(emptyQuery())
		s.write(readyForQuery('I'))
		return
	}
	for _, stmt := range stmts {
		if err := s.execStatement(s.ctx, stmt, nil); err != nil {
			s.sendError(err)
			s.inError = false // 简单查询：ReadyForQuery 即同步点
			break             // 简单查询：出错后忽略剩余语句
		}
	}
	s.write(readyForQuery('I'))
}

func (s *session) handleDescribe(m *frontendMessage) {
	if m.describeTarget == 'S' {
		stmt := s.stmts[m.stmtName]
		if stmt == nil {
			s.sendError(fmt.Errorf("prepared statement %q does not exist", m.stmtName))
			return
		}
		// 参数全部声明为 TEXT：让客户端以文本格式发送参数
		n := countParams(stmt.sql)
		oids := make([]uint32, n)
		for i := range oids {
			oids[i] = oidText
		}
		s.write(parameterDescription(oids))
		s.write(s.describeColumns(stmt.sql, nil))
		return
	}
	portal := s.portals[m.stmtName]
	if portal == nil {
		s.sendError(fmt.Errorf("portal %q does not exist", m.stmtName))
		return
	}
	s.write(s.describeColumns(portal.stmt.sql, portal.params))
}

// describeColumns 尽力给出语句的结果列（用于 Describe）。
func (s *session) describeColumns(sql string, params []*sqlraftpb.Value) []byte {
	kw := firstKeyword(sql)
	if kw != "SELECT" && kw != "VALUES" && kw != "WITH" && kw != "PRAGMA" && kw != "EXPLAIN" {
		return noData()
	}
	cols, err := s.dryRunColumns(sql, params)
	if err != nil {
		s.srv.logger.Printf("pgwire: describe dry-run failed for %q: %v", sql, err)
		return noData()
	}
	return rowDescription(cols)
}

// dryRunColumns 不真正读取数据地获取列信息：
// SELECT 类语句包装为 `SELECT * FROM (stmt) LIMIT 0`；PRAGMA/EXPLAIN 直接执行。
func (s *session) dryRunColumns(sql string, params []*sqlraftpb.Value) ([]columnDesc, error) {
	kw := firstKeyword(sql)
	sql = translateDialect(sql)
	var probe string
	switch kw {
	case "SELECT", "VALUES", "WITH":
		probe = "SELECT * FROM (" + strings.TrimSuffix(strings.TrimSpace(sql), ";") + ") LIMIT 0"
	default:
		probe = sql
	}
	// 参数化语句用 NULL 占位符执行 LIMIT 0，只取列信息
	if len(params) == 0 {
		n := countParams(sql)
		params = make([]*sqlraftpb.Value, n)
		for i := range params {
			params[i] = &sqlraftpb.Value{Kind: &sqlraftpb.Value_Null{Null: &sqlraftpb.Null{}}}
		}
	}
	res, err := s.srv.store.QueryRows(s.ctx, probe, params)
	if err != nil {
		return nil, err
	}
	cols := make([]columnDesc, 0, len(res.Columns))
	for _, c := range res.Columns {
		oid, typlen := pgColumnType(c.Type)
		cols = append(cols, columnDesc{
			name:    c.Name,
			typeOID: oid,
			typlen:  typlen,
			typmod:  -1,
			format:  0,
		})
	}
	return cols, nil
}

// handleExecute 执行一个已绑定的 portal。
func (s *session) handleExecute(portalName string, maxRows int32) {
	portal := s.portals[portalName]
	if portal == nil {
		s.sendError(fmt.Errorf("portal %q does not exist", portalName))
		return
	}
	if err := s.execStatement(s.ctx, portal.stmt.sql, portal.params); err != nil {
		s.sendError(err)
		return
	}
	_ = maxRows
}

// execStatement 执行一条语句并写出响应。
func (s *session) execStatement(ctx context.Context, sql string, params []*sqlraftpb.Value) error {
	// 事务控制：BEGIN / COMMIT / ROLLBACK 由会话状态机处理
	if act := txAction(sql); act != "" {
		return s.handleTx(act)
	}

	// 兼容 shim（SET/SHOW/元查询）
	if shim, ok := shimFor(sql, s.dbName, s.user, s.host, s.port, s.pid, s.srv.started); ok {
		if shim.tag != "" {
			return s.write(commandComplete(shim.tag))
		}
		rows := [][]any{{shim.value}}
		cols := []store.Column{{Name: shim.column, Type: pgTypeName(shim.typeOID)}}
		return s.writeRawResult(cols, rows, "SELECT 1")
	}

	kind := classify(sql)
	sql = translateDialect(sql)
	sql = translateDdlTypes(sql)
	switch kind {
	case kindNoop:
		tag := strings.ToUpper(firstKeyword(sql))
		return s.write(commandComplete(tag))
	case kindRead:
		// pg_catalog 元查询路由到内存元数据库
		if s.srv.catalog != nil && isCatalogQuery(sql) {
			prepared := prepareCatalogQuery(sql)
			res, err := s.srv.catalog.Query(ctx, prepared)
			if err != nil {
				return err
			}
			return s.writeRawResult(res.Columns, res.Rows,
				"SELECT "+strconv.Itoa(len(res.Rows)))
		}
		sql = translateReadFunctions(sql)
		sql = replaceSessionIdent(sql, "current_user", s.user)
		sql = replaceSessionIdent(sql, "session_user", s.user)
		res, err := s.srv.store.QueryRows(ctx, sql, params)
		if err != nil {
			return err
		}
		return s.writeRawResult(res.Columns, res.Rows, "SELECT "+strconv.Itoa(len(res.Rows)))
	case kindWrite:
		// 事务内写：缓冲到 COMMIT 时作为单条日志原子提交。
		// 影响行数与 RETURNING 结果在 COMMIT 前不可用，返回占位响应。
		if s.tx != nil {
			rendered, err := renderParams(sql, params)
			if err != nil {
				return err
			}
			if len(s.tx.stmts) >= maxTxStatements {
				return errors.New("transaction too large: statement limit exceeded")
			}
			s.tx.stmts = append(s.tx.stmts, rendered)
			return s.write(commandComplete(writeTag(rendered, 0)))
		}
		res, err := s.srv.executor.ExecuteRaw(ctx, sql, params)
		if err != nil {
			return err
		}
		tag := writeTag(sql, res.RowsAffected)
		if len(res.Rows) > 0 {
			cols := make([]store.Column, len(res.Columns))
			for i, name := range res.Columns {
				typ := ""
				if i < len(res.RowTypes) {
					typ = res.RowTypes[i]
				}
				cols[i] = store.Column{Name: name, Type: typ}
			}
			return s.writeRawResult(cols, res.Rows, tag)
		}
		return s.write(commandComplete(tag))
	}
	return nil
}

// isCatalogQuery 判断语句是否引用 pg_catalog（psql 元命令的特征）。
func isCatalogQuery(sql string) bool {
	st := scanState{}
	for i := 0; i+10 < len(sql); i++ {
		st.feed(sql, i)
		if st.inNormal() && strings.HasPrefix(sql[i:], "pg_catalog.") {
			return true
		}
	}
	return false
}

// replaceSessionIdent 把引号外的会话标识符替换为用户名字面量。
func replaceSessionIdent(sql, ident, user string) string {
	var out strings.Builder
	st := scanState{}
	for i := 0; i < len(sql); i++ {
		st.feed(sql, i)
		if !st.inNormal() {
			out.WriteByte(sql[i])
			continue
		}
		if isLetter(sql[i]) || sql[i] == '_' {
			j := i
			for j < len(sql) && (isLetter(sql[j]) || isDigit(sql[j]) || sql[j] == '_' || sql[j] == '$') {
				j++
			}
			word := sql[i:j]
			if strings.EqualFold(word, ident) {
				out.WriteString("'" + strings.ReplaceAll(user, "'", "''") + "'")
			} else {
				out.WriteString(word)
			}
			i = j - 1
			continue
		}
		out.WriteByte(sql[i])
	}
	return out.String()
}

// txAction 识别事务控制语句：BEGIN/START → BEGIN，COMMIT/END → COMMIT，
// ROLLBACK/ABORT → ROLLBACK；其余返回空串。
func txAction(sql string) string {
	switch firstKeyword(sql) {
	case "BEGIN", "START":
		return "BEGIN"
	case "COMMIT", "END":
		return "COMMIT"
	case "ROLLBACK", "ABORT":
		return "ROLLBACK"
	}
	return ""
}

// handleTx 执行事务状态迁移。
func (s *session) handleTx(act string) error {
	switch act {
	case "BEGIN":
		if s.tx != nil {
			return errors.New("transaction already in progress")
		}
		s.tx = &txState{}
		return s.write(commandComplete("BEGIN"))
	case "COMMIT":
		if s.tx == nil {
			return s.write(commandComplete("COMMIT")) // 无事务：直接成功
		}
		stmts := s.tx.stmts
		s.tx = nil
		if len(stmts) == 0 {
			return s.write(commandComplete("COMMIT"))
		}
		if err := s.srv.executor.ExecuteTx(s.ctx, stmts); err != nil {
			return err
		}
		return s.write(commandComplete("COMMIT"))
	case "ROLLBACK":
		s.tx = nil // 丢弃缓冲：等价于回滚
		return s.write(commandComplete("ROLLBACK"))
	}
	return nil
}

// renderParams 把已翻译 SQL 中的 ?N 占位符替换为字面量，
// 使事务缓冲的语句不依赖参数绑定。
func renderParams(sql string, params []*sqlraftpb.Value) (string, error) {
	var out strings.Builder
	st := scanState{}
	for i := 0; i < len(sql); i++ {
		st.feed(sql, i)
		if !st.inNormal() {
			out.WriteByte(sql[i])
			continue
		}
		if sql[i] == '?' && i+1 < len(sql) && isDigit(sql[i+1]) {
			j := i + 1
			for j < len(sql) && isDigit(sql[j]) {
				j++
			}
			n, err := strconv.Atoi(sql[i+1 : j])
			if err != nil || n < 1 || n > len(params) {
				return "", fmt.Errorf("bind parameter $%d missing", n)
			}
			out.WriteString(literalFor(params[n-1]))
			i = j - 1
			continue
		}
		out.WriteByte(sql[i])
	}
	return out.String(), nil
}

// literalFor 把参数值编码为安全的 SQL 字面量。
func literalFor(v *sqlraftpb.Value) string {
	switch k := v.Kind.(type) {
	case *sqlraftpb.Value_Null:
		return "NULL"
	case *sqlraftpb.Value_I:
		return strconv.FormatInt(k.I, 10)
	case *sqlraftpb.Value_F:
		return strconv.FormatFloat(k.F, 'g', -1, 64)
	case *sqlraftpb.Value_B:
		if k.B {
			return "1"
		}
		return "0"
	case *sqlraftpb.Value_By:
		return "X'" + hex.EncodeToString(k.By) + "'"
	case *sqlraftpb.Value_S:
		return "'" + strings.ReplaceAll(k.S, "'", "''") + "'"
	default:
		return "NULL"
	}
}

// writeRawResult 输出 RowDescription + DataRows + CommandComplete。
func (s *session) writeRawResult(cols []store.Column, rows [][]any, tag string) error {
	descs := make([]columnDesc, len(cols))
	for i, c := range cols {
		oid, typlen := pgColumnType(c.Type)
		descs[i] = columnDesc{name: c.Name, typeOID: oid, typlen: typlen, typmod: -1, format: 0}
	}
	if err := s.write(rowDescription(descs)); err != nil {
		return err
	}
	for _, row := range rows {
		vals := make([][]byte, len(row))
		for i, v := range row {
			var oid uint32 = oidText
			if i < len(descs) {
				oid = descs[i].typeOID
			}
			vals[i] = encodeText(v, oid)
		}
		if err := s.write(dataRow(vals...)); err != nil {
			return err
		}
	}
	return s.write(commandComplete(tag))
}

// sendError 输出 ErrorResponse 并进入错误状态（扩展协议下等 Sync 恢复）。
func (s *session) sendError(err error) {
	s.inError = true
	code, msg, hint := sqlstateFor(err)
	fields := []field{
		{'S', "ERROR"},
		{'V', "ERROR"},
		{'C', code},
		{'M', msg},
	}
	if hint != "" {
		fields = append(fields, field{'H', hint})
	}
	s.write(errorResponse(fields...))
}

func (s *session) write(b []byte) error {
	_, err := s.w.Write(b)
	return err
}

// ---------- 工具 ----------

// sqlstateFor 把底层错误映射为 PG SQLSTATE。
func sqlstateFor(err error) (code, msg, hint string) {
	var notLeader *raft.ErrNotLeader
	if errors.As(err, &notLeader) {
		return "40001", fmt.Sprintf("not leader (leader=%d)", notLeader.Leader),
			"connect to the leader node to execute writes"
	}
	if errors.Is(err, context.Canceled) {
		return "57014", "canceling statement due to user request", ""
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "transaction already in progress"):
		return "25001", s, ""
	case strings.Contains(s, "no such table"):
		return "42P01", s, ""
	case strings.Contains(s, "no such column"):
		return "42703", s, ""
	case strings.Contains(s, "no such function"):
		return "42883", s, ""
	case strings.Contains(s, "table already exists"):
		return "42P07", s, ""
	case strings.Contains(s, "UNIQUE constraint failed"):
		return "23505", s, ""
	case strings.Contains(s, "FOREIGN KEY constraint failed"):
		return "23503", s, ""
	case strings.Contains(s, "NOT NULL constraint failed"):
		return "23502", s, ""
	case strings.Contains(s, "CHECK constraint failed"):
		return "23514", s, ""
	case strings.Contains(s, "database is locked"):
		return "55P03", s, ""
	case strings.Contains(s, "syntax error"), strings.Contains(s, "unrecognized token"):
		return "42601", s, ""
	case strings.Contains(s, "datatype mismatch"):
		return "42804", s, ""
	default:
		return "XX000", s, ""
	}
}

// writeTag 生成写语句的 CommandComplete tag。
func writeTag(sql string, rows int64) string {
	switch strings.ToUpper(firstKeyword(sql)) {
	case "INSERT", "REPLACE":
		return "INSERT 0 " + strconv.FormatInt(rows, 10)
	case "UPDATE":
		return "UPDATE " + strconv.FormatInt(rows, 10)
	case "DELETE":
		return "DELETE " + strconv.FormatInt(rows, 10)
	case "CREATE":
		kw := firstKeywordAfter(sql, "CREATE")
		if kw == "" {
			kw = "TABLE"
		}
		return "CREATE " + strings.ToUpper(kw)
	case "DROP":
		kw := firstKeywordAfter(sql, "DROP")
		return "DROP " + strings.ToUpper(kw)
	case "ALTER":
		return "ALTER TABLE"
	case "TRUNCATE":
		return "TRUNCATE TABLE"
	case "VACUUM":
		return "VACUUM"
	case "ANALYZE":
		return "ANALYZE"
	default:
		return strings.ToUpper(firstKeyword(sql))
	}
}

// firstKeywordAfter 返回关键字之后的第一个关键字（用于 CREATE TABLE → TABLE）。
func firstKeywordAfter(sql, kw string) string {
	up := strings.ToUpper(sql)
	idx := strings.Index(up, kw)
	if idx < 0 {
		return ""
	}
	return firstKeyword(up[idx+len(kw):])
}

// countParams 统计 SQL 中的 $N 或 ?N 参数个数（引号/注释外）。
func countParams(sql string) int {
	maxN := 0
	st := scanState{}
	for i := 0; i < len(sql); i++ {
		st.feed(sql, i)
		if !st.inNormal() || (sql[i] != '$' && sql[i] != '?') {
			continue
		}
		j := i + 1
		for j < len(sql) && isDigit(sql[j]) {
			j++
		}
		if j > i+1 {
			if n, err := strconv.Atoi(sql[i+1 : j]); err == nil && n > maxN {
				maxN = n
			}
		}
	}
	return maxN
}

// pgTypeName 由 OID 反查 SQLite 类型名（供 shim 结果编码）。
func pgTypeName(oid uint32) string {
	switch oid {
	case oidBool:
		return "BOOLEAN"
	case oidInt8:
		return "INTEGER"
	case oidFloat8:
		return "REAL"
	case oidBytea:
		return "BLOB"
	default:
		return "TEXT"
	}
}

func int32FromBytes(b []byte) int32 {
	return int32(uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]))
}

func uint32FromBytes(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

// parseStartupParams 解析启动参数对。
func parseStartupParams(b []byte) map[string]string {
	m := make(map[string]string)
	for len(b) > 0 {
		if b[0] == 0 {
			break // 结尾的 \0 表示参数对结束
		}
		i := 0
		for i < len(b) && b[i] != 0 {
			i++
		}
		if i >= len(b) {
			break
		}
		k := string(b[:i])
		b = b[i+1:]
		i = 0
		for i < len(b) && b[i] != 0 {
			i++
		}
		if i >= len(b) {
			break
		}
		v := string(b[:i])
		b = b[i+1:]
		if k != "" {
			m[k] = v
		}
	}
	return m
}
