// Package pgwire 实现 PostgreSQL 前端/后端协议（v3）的服务端子集，
// 让 psql / JDBC / ORM 等现有工具可以直连 sqlraft。
//
// 支持：
//   - 启动握手（trust 认证）、参数状态、后端密钥；
//   - 简单查询协议（Q）与扩展查询协议（Parse/Bind/Describe/Execute/Sync）；
//   - 文本格式的结果行（format 0）与常见类型 OID 映射；
//   - 取消请求（CancelRequest）。
package pgwire

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

// 协议版本号（v3.0）。
const protocolVersion = 196608

// 特殊启动消息代码。
const (
	sslRequestCode = 80877103
	cancelReqCode  = 80877102
)

// 后端消息类型。
const (
	msgAuthenticationOK     = 'R'
	msgBackendKeyData       = 'K'
	msgParameterStatus      = 'S'
	msgReadyForQuery        = 'Z'
	msgRowDescription       = 'T'
	msgDataRow              = 'D'
	msgCommandComplete      = 'C'
	msgEmptyQueryResponse   = 'I'
	msgErrorResponse        = 'E'
	msgNoticeResponse       = 'N'
	msgParseComplete        = '1'
	msgBindComplete         = '2'
	msgCloseComplete        = '3'
	msgParameterDescription = 't'
	msgNoData               = 'n'
	msgPortalSuspended      = 's'
)

// 前端消息类型。
const (
	frontQuery     = 'Q'
	frontParse     = 'P'
	frontBind      = 'B'
	frontDescribe  = 'D'
	frontExecute   = 'E'
	frontSync      = 'S'
	frontClose     = 'C'
	frontFlush     = 'H'
	frontTerminate = 'X'
	frontPassword  = 'p' // SASLInitialResponse / SASLResponse / PasswordMessage
)

// buf 是带长度前缀的消息编码器。
type buf struct {
	b []byte
}

func (w *buf) u8(v byte)      { w.b = append(w.b, v) }
func (w *buf) i16(v int16)    { w.b = binary.BigEndian.AppendUint16(w.b, uint16(v)) }
func (w *buf) i32(v int32)    { w.b = binary.BigEndian.AppendUint32(w.b, uint32(v)) }
func (w *buf) u32(v uint32)   { w.b = binary.BigEndian.AppendUint32(w.b, v) }
func (w *buf) cstr(s string)  { w.b = append(w.b, s...); w.b = append(w.b, 0) }
func (w *buf) bytes(p []byte) { w.b = append(w.b, p...) }

// msg 构建一条带类型与长度前缀的后端消息。
func msg(typ byte, payload []byte) []byte {
	out := make([]byte, 0, 1+4+len(payload))
	out = append(out, typ)
	out = binary.BigEndian.AppendUint32(out, uint32(len(payload)+4))
	out = append(out, payload...)
	return out
}

func authOK() []byte {
	var p buf
	p.i32(0) // AuthenticationOk
	return msg(msgAuthenticationOK, p.b)
}

func parameterStatus(name, value string) []byte {
	var p buf
	p.cstr(name)
	p.cstr(value)
	return msg(msgParameterStatus, p.b)
}

func backendKeyData(pid, secret uint32) []byte {
	var p buf
	p.i32(int32(pid))
	p.i32(int32(secret))
	return msg(msgBackendKeyData, p.b)
}

func readyForQuery(status byte) []byte {
	return msg(msgReadyForQuery, []byte{status})
}

func commandComplete(tag string) []byte {
	var p buf
	p.cstr(tag)
	return msg(msgCommandComplete, p.b)
}

func emptyQuery() []byte {
	return msg(msgEmptyQueryResponse, nil)
}

func parseComplete() []byte   { return msg(msgParseComplete, nil) }
func bindComplete() []byte    { return msg(msgBindComplete, nil) }
func closeComplete() []byte   { return msg(msgCloseComplete, nil) }
func noData() []byte          { return msg(msgNoData, nil) }
func portalSuspended() []byte { return msg(msgPortalSuspended, nil) }

// field 是 ErrorResponse / NoticeResponse 的一个字段。
type field struct {
	code byte
	text string
}

func errorResponse(fields ...field) []byte {
	var p buf
	for _, f := range fields {
		p.u8(f.code)
		p.cstr(f.text)
	}
	p.u8(0)
	return msg(msgErrorResponse, p.b)
}

// columnDesc 描述结果集的一列（RowDescription 条目）。
type columnDesc struct {
	name     string
	tableOID uint32
	attnum   int16
	typeOID  uint32
	typlen   int16
	typmod   int32
	format   int16
}

func rowDescription(cols []columnDesc) []byte {
	var p buf
	p.i16(int16(len(cols)))
	for _, c := range cols {
		p.cstr(c.name)
		p.i32(int32(c.tableOID))
		p.i16(c.attnum)
		p.i32(int32(c.typeOID))
		p.i16(c.typlen)
		p.i32(c.typmod)
		p.i16(c.format)
	}
	return msg(msgRowDescription, p.b)
}

// dataRow 编码一行数据；nil 值编码为 NULL（长度 -1）。
func dataRow(values ...[]byte) []byte {
	var p buf
	p.i16(int16(len(values)))
	for _, v := range values {
		if v == nil {
			p.i32(-1)
			continue
		}
		p.i32(int32(len(v)))
		p.bytes(v)
	}
	return msg(msgDataRow, p.b)
}

func parameterDescription(typeOIDs []uint32) []byte {
	var p buf
	p.i16(int16(len(typeOIDs)))
	for _, oid := range typeOIDs {
		p.i32(int32(oid))
	}
	return msg(msgParameterDescription, p.b)
}

// ---------- 前端消息读取 ----------

// frontendMessage 是一条已解析的前端消息。
type frontendMessage struct {
	typ  byte
	body []byte // 原始 payload（SASL 等未结构化的消息）
	// 各消息的解析结果
	sql            string // Q
	stmtName       string // P/B/D/C
	query          string // P
	paramOIDs      []uint32
	portal         string // B/E/D/C
	formats        []int16
	params         []paramValue
	describeTarget byte  // D: 'S' 或 'P'
	maxRows        int32 // E
	closeType      byte  // C: 'S' 或 'P'
}

// paramValue 是 Bind 消息中的一个参数值。
type paramValue struct {
	format int16 // 0=text, 1=binary
	value  []byte
	isNull bool
}

// readFrontend 读取一条前端消息；返回 io.EOF 表示连接关闭。
func readFrontend(r io.Reader) (*frontendMessage, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	typ := hdr[0]
	n := binary.BigEndian.Uint32(hdr[1:])
	if n < 4 || n > 64<<20 {
		return nil, fmt.Errorf("pgwire: invalid message length %d", n)
	}
	payload := make([]byte, n-4)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	m := &frontendMessage{typ: typ, body: payload}
	switch typ {
	case frontQuery:
		m.sql = cstring(payload)
	case frontParse:
		m.stmtName, m.query = parseNameQuery(payload)
		m.paramOIDs = parseParamOIDs(payload)
	case frontBind:
		m.portal, m.stmtName = parseBindHeader(payload)
		m.formats, m.params = parseBindBody(payload)
	case frontDescribe:
		if len(payload) >= 1 {
			m.describeTarget = payload[0]
			m.stmtName = cstring(payload[1:])
		}
	case frontExecute:
		m.portal, m.maxRows = parseExecute(payload)
	case frontClose:
		if len(payload) >= 1 {
			m.closeType = payload[0]
			m.stmtName = cstring(payload[1:])
		}
	}
	return m, nil
}

func cstring(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}

func parseNameQuery(b []byte) (string, string) {
	i := 0
	for i < len(b) && b[i] != 0 {
		i++
	}
	if i >= len(b) {
		return string(b), ""
	}
	name := string(b[:i])
	i++
	j := i
	for j < len(b) && b[j] != 0 {
		j++
	}
	if j >= len(b) {
		return name, string(b[i:])
	}
	return name, string(b[i:j])
}

func parseParamOIDs(b []byte) []uint32 {
	// 跳过 name\0query\0，读取 int16 数量与 OID 列表
	i := 0
	for i < len(b) && b[i] != 0 {
		i++
	}
	i++ // name\0
	for i < len(b) && b[i] != 0 {
		i++
	}
	i++ // query\0
	if i+2 > len(b) {
		return nil
	}
	n := int(int16(binary.BigEndian.Uint16(b[i:])))
	i += 2
	if n < 0 {
		return nil
	}
	oids := make([]uint32, n)
	for j := 0; j < n && i+4 <= len(b); j++ {
		oids[j] = binary.BigEndian.Uint32(b[i:])
		i += 4
	}
	return oids
}

func parseBindHeader(b []byte) (portal, stmt string) {
	i := 0
	for i < len(b) && b[i] != 0 {
		i++
	}
	if i >= len(b) {
		return string(b), ""
	}
	portal = string(b[:i])
	i++
	start := i
	for i < len(b) && b[i] != 0 {
		i++
	}
	if i >= len(b) {
		return portal, string(b[start:])
	}
	stmt = string(b[start:i])
	return portal, stmt
}

// parseBindBody 解析 Bind 消息的格式与参数部分。
func parseBindBody(b []byte) ([]int16, []paramValue) {
	// 先定位 stmt\0 之后
	i := 0
	for i < len(b) && b[i] != 0 {
		i++
	}
	i++
	for i < len(b) && b[i] != 0 {
		i++
	}
	i++
	if i+2 > len(b) {
		return nil, nil
	}
	nFormats := int(int16(binary.BigEndian.Uint16(b[i:])))
	i += 2
	formats := make([]int16, nFormats)
	for j := 0; j < nFormats && i+2 <= len(b); j++ {
		formats[j] = int16(binary.BigEndian.Uint16(b[i:]))
		i += 2
	}
	if i+2 > len(b) {
		return formats, nil
	}
	nParams := int(int16(binary.BigEndian.Uint16(b[i:])))
	i += 2
	params := make([]paramValue, nParams)
	for j := 0; j < nParams && i+4 <= len(b); j++ {
		f := int16(0)
		if len(formats) == 1 {
			f = formats[0]
		} else if len(formats) == nParams {
			f = formats[j]
		}
		plen := int32(binary.BigEndian.Uint32(b[i:]))
		i += 4
		if plen < 0 {
			params[j] = paramValue{format: f, isNull: true}
			continue
		}
		if i+int(plen) > len(b) {
			break
		}
		v := make([]byte, plen)
		copy(v, b[i:i+int(plen)])
		i += int(plen)
		params[j] = paramValue{format: f, value: v}
	}
	return formats, params
}

func parseExecute(b []byte) (string, int32) {
	portal := cstring(b)
	if len(b) < 6 {
		return portal, 0
	}
	return portal, int32(binary.BigEndian.Uint32(b[len(b)-4:]))
}

// parseSASLInitial 解析 SASLInitialResponse：mechanism\0 + int32 len + response。
func parseSASLInitial(b []byte) (string, error) {
	i := 0
	for i < len(b) && b[i] != 0 {
		i++
	}
	if i >= len(b) {
		return "", fmt.Errorf("pgwire: malformed SASLInitialResponse")
	}
	mechanism := string(b[:i])
	if mechanism != "SCRAM-SHA-256" {
		return "", fmt.Errorf("pgwire: unsupported SASL mechanism %q", mechanism)
	}
	i++
	if i+4 > len(b) {
		return "", fmt.Errorf("pgwire: SASLInitialResponse missing length")
	}
	n := int32(binary.BigEndian.Uint32(b[i:]))
	i += 4
	if n < 0 || i+int(n) > len(b) {
		return "", fmt.Errorf("pgwire: SASLInitialResponse bad length")
	}
	return string(b[i : i+int(n)]), nil
}

// parseSASLResponse 解析 SASLResponse：int32 len + response。
func parseSASLResponse(b []byte) (string, error) {
	// libpq 实际发送的 SASLResponse 是裸 client-final 字符串（无 Int32 前缀），
	// 与协议文档描述不一致；两种格式都兼容。
	if len(b) >= 2 && b[0] == 'c' && b[1] == '=' {
		return strings.TrimRight(string(b), "\x00"), nil
	}
	if len(b) < 4 {
		return "", fmt.Errorf("pgwire: SASLResponse missing length (len=%d)", len(b))
	}
	n := int32(binary.BigEndian.Uint32(b[:4]))
	if n < 0 || 4+int(n) > len(b) {
		return "", fmt.Errorf("pgwire: SASLResponse bad length (n=%d len=%d head=%x)",
			n, len(b), b[:min(8, len(b))])
	}
	return string(b[4 : 4+int(n)]), nil
}
