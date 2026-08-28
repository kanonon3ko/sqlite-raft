// Package rewrite 负责把客户端 SQL 改写为“确定性命令”：
//
// Raft 状态机要求所有节点按相同顺序执行相同的语句，因此任何依赖
// 本机时钟或随机源的表达式都必须由 Leader 预先求值并写入日志。
// 本包处理：
//
//   - NOW() / CURRENT_TIMESTAMP / CURRENT_DATE / CURRENT_TIME → 统一时间戳字面量；
//   - RANDOM() / RANDOMBLOB(n) → 预生成的随机字面量；
//   - AUTOINCREMENT 的 INSERT → 依据已提交状态显式预分配 ID。
//
// 改写策略是“保守的”：无法可靠识别/改写的语句原样放行（有序重放仍能保证
// 大多数情况一致），能识别的非确定性调用则一律替换为字面量。
package rewrite

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
)

// Schema 提供改写 AUTOINCREMENT 所需的表结构信息，由 store 实现。
type Schema interface {
	// TableInfo 返回表的列信息；表不存在时返回 nil 且不报错。
	TableInfo(table string) (*TableInfo, error)
	// NextAutoIncrement 返回表下一个可用的自增 ID。
	NextAutoIncrement(table string) (int64, error)
}

// TableInfo 描述一张表的列结构。
type TableInfo struct {
	Columns       []string
	AutoIncrement string // 自增列名（无自增列时为空字符串）
}

// Options 控制改写参数：统一时间与随机源。
type Options struct {
	Now  time.Time
	Rand io.Reader // 随机源；nil 时使用 crypto/rand
}

// Result 是一次改写的结果。
type Result struct {
	SQL      string
	Params   []*sqlraftpb.Value
	Sequence map[string]int64 // 表名 -> 本次预分配的起始自增 ID
}

// Rewrite 改写一条写语句。schema 为 nil 时跳过 AUTOINCREMENT 改写。
func Rewrite(sql string, params []*sqlraftpb.Value, schema Schema, opt Options) (*Result, error) {
	if opt.Rand == nil {
		opt.Rand = rand.Reader
	}
	if opt.Now.IsZero() {
		opt.Now = time.Now()
	}

	toks := tokenize(sql)
	rw := &rewriter{
		tokens: toks,
		params: params,
		schema: schema,
		now:    opt.Now.UTC(),
		rand:   opt.Rand,
	}

	out, err := rw.run()
	if err != nil {
		return nil, err
	}
	return &Result{SQL: out, Params: rw.params, Sequence: rw.sequence}, nil
}

type rewriter struct {
	tokens   []token
	params   []*sqlraftpb.Value
	schema   Schema
	now      time.Time
	rand     io.Reader
	sequence map[string]int64
}

func (rw *rewriter) run() (string, error) {
	var sb strings.Builder
	i := 0
	for i < len(rw.tokens) {
		t := rw.tokens[i]
		switch {
		case t.isKeyword("INSERT"):
			if rw.schema != nil && atStatementStart(rw.tokens, i) {
				repl, end, ok, err := rw.rewriteInsert(i)
				if err != nil {
					return "", err
				}
				if ok {
					sb.WriteString(repl)
					i = end
					continue
				}
			}
			sb.WriteString(t.text)
			i++
		case t.isKeyword("NOW"):
			end, ok := matchCall(rw.tokens, i)
			if !ok {
				sb.WriteString(t.text)
				i++
				continue
			}
			// NOW(fsp)：忽略精度参数，统一按微秒时间输出
			sb.WriteString(quoteString(rw.now.Format("2006-01-02 15:04:05.999999")))
			i = end
		case t.isKeyword("CURRENT_TIMESTAMP"):
			sb.WriteString(quoteString(rw.now.Format("2006-01-02 15:04:05")))
			i++
		case t.isKeyword("CURRENT_DATE"):
			sb.WriteString(quoteString(rw.now.Format("2006-01-02")))
			i++
		case t.isKeyword("CURRENT_TIME"):
			sb.WriteString(quoteString(rw.now.Format("15:04:05")))
			i++
		case t.isKeyword("RANDOM"):
			end, ok := matchCall(rw.tokens, i)
			if !ok {
				sb.WriteString(t.text)
				i++
				continue
			}
			if !callHasEmptyArgs(rw.tokens, i, end) {
				// RANDOM(expr) 不是合法 SQLite 语法，原样放行交给执行器报错
				sb.WriteString(t.text)
				i++
				continue
			}
			v, err := rw.randomInt64()
			if err != nil {
				return "", err
			}
			sb.WriteString(strconv.FormatInt(v, 10))
			i = end
		case t.isKeyword("RANDOMBLOB"):
			end, ok := matchCall(rw.tokens, i)
			if !ok {
				sb.WriteString(t.text)
				i++
				continue
			}
			n, consumed, err := rw.randomBlobLen(i, end)
			if err != nil {
				return "", err
			}
			buf := make([]byte, n)
			if _, err := io.ReadFull(rw.rand, buf); err != nil {
				return "", fmt.Errorf("read random bytes: %w", err)
			}
			sb.WriteString("X'" + hex.EncodeToString(buf) + "'")
			i = end
			_ = consumed
		default:
			sb.WriteString(t.text)
			i++
		}
	}
	return sb.String(), nil
}

// randomBlobLen 解析 RANDOMBLOB(expr) 的长度参数。
// 支持整数常量与位置参数 `?`；使用 `?` 时同步从参数列表中移除对应参数。
func (rw *rewriter) randomBlobLen(callStart, callEnd int) (int64, int, error) {
	// 定位 '(' 之后的第一个非空白/注释 token
	inner := -1
	for j := callStart + 1; j < callEnd; j++ {
		if !rw.tokens[j].isSkippable() {
			inner = j
			break
		}
	}
	if inner == -1 || rw.tokens[inner].text != "(" {
		return 0, 0, errors.New("RANDOMBLOB: missing argument")
	}

	arg := -1
	for j := inner + 1; j < callEnd; j++ {
		if !rw.tokens[j].isSkippable() {
			arg = j
			break
		}
	}
	if arg == -1 {
		return 0, 0, errors.New("RANDOMBLOB: missing length argument")
	}

	t := rw.tokens[arg]
	if t.kind == tokIdent {
		if n, err := strconv.ParseInt(t.text, 10, 64); err == nil {
			return n, 1, nil
		}
		return 0, 0, fmt.Errorf("RANDOMBLOB: unsupported length expression %q", t.text)
	}
	if t.text == "?" {
		idx := positionalParamIndex(rw.tokens, arg)
		if idx < 0 || idx >= len(rw.params) {
			return 0, 0, errors.New("RANDOMBLOB: missing parameter for length")
		}
		v := rw.params[idx]
		n, err := valueToInt64(v)
		if err != nil {
			return 0, 0, fmt.Errorf("RANDOMBLOB: %w", err)
		}
		rw.params = append(rw.params[:idx], rw.params[idx+1:]...)
		return n, 1, nil
	}
	return 0, 0, fmt.Errorf("RANDOMBLOB: unsupported length expression %q", t.text)
}

// positionalParamIndex 返回 token 位置 j 处的 `?` 是第几个位置参数（0 起）。
func positionalParamIndex(tokens []token, j int) int {
	n := 0
	for i := 0; i < j; i++ {
		if tokens[i].text == "?" && tokens[i].kind == tokPunct {
			n++
		}
	}
	return n
}

func valueToInt64(v *sqlraftpb.Value) (int64, error) {
	switch k := v.Kind.(type) {
	case *sqlraftpb.Value_I:
		return k.I, nil
	case *sqlraftpb.Value_S:
		n, err := strconv.ParseInt(k.S, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("length parameter is not an integer: %q", k.S)
		}
		return n, nil
	default:
		return 0, errors.New("length parameter must be an integer")
	}
}

// randomInt64 生成与 SQLite random() 一致的带符号 64 位随机整数。
func (rw *rewriter) randomInt64() (int64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(rw.rand, buf[:]); err != nil {
		return 0, fmt.Errorf("read random bytes: %w", err)
	}
	return int64(binary.LittleEndian.Uint64(buf[:])), nil
}

// ---------- INSERT / AUTOINCREMENT 改写 ----------

// rewriteInsert 尝试为 INSERT 语句补充自增 ID，返回改写后的 SQL 与消费到的
// token 位置。ok=false 表示语句形式不受支持，原样放行。
func (rw *rewriter) rewriteInsert(start int) (string, int, bool, error) {
	toks := rw.tokens
	i := skipSpace(toks, start+1) // 跳过 INSERT 与空白

	// INSERT OR REPLACE / OR IGNORE / ...
	if i < len(toks) && toks[i].isKeyword("OR") {
		i = skipSpace(toks, i+1) + 1 // 跳过 OR 与冲突策略关键字
	}
	i = skipSpace(toks, i)
	if i >= len(toks) || !toks[i].isKeyword("INTO") {
		return "", 0, false, nil
	}
	i = skipSpace(toks, i+1)

	table, i, ok := parseTableName(toks, i)
	if !ok {
		return "", 0, false, nil
	}

	// AS alias
	i = skipSpace(toks, i)
	if i < len(toks) && toks[i].isKeyword("AS") {
		i = skipSpace(toks, i+1) + 1
	}

	// 可选列清单
	var columns []string
	hasColumnList := false
	i = skipSpace(toks, i)
	if i < len(toks) && toks[i].text == "(" {
		var end int
		columns, end, ok = parseParenList(toks, i)
		if !ok {
			return "", 0, false, nil
		}
		hasColumnList = true
		i = end
	}

	i = skipSpace(toks, i)
	if i >= len(toks) {
		return "", 0, false, nil
	}

	// DEFAULT VALUES 形式
	if toks[i].isKeyword("DEFAULT") {
		valuesIdx := skipSpace(toks, i+1)
		if valuesIdx >= len(toks) || !toks[valuesIdx].isKeyword("VALUES") {
			return "", 0, false, nil
		}
		info, err := rw.schema.TableInfo(table)
		if err != nil || info == nil || info.AutoIncrement == "" {
			return "", 0, false, nil
		}
		id, err := rw.nextIDs(table, 1)
		if err != nil {
			return "", 0, false, nil
		}
		end := valuesIdx + 1
		return rebuildInsertDefault(toks, start, end, info.AutoIncrement, id[0]), end, true, nil
	}

	if !toks[i].isKeyword("VALUES") {
		return "", 0, false, nil // INSERT ... SELECT 等，原样放行
	}
	rowsEnd, rows, ok := parseValuesRows(toks, i)
	if !ok {
		return "", 0, false, nil
	}

	info, err := rw.schema.TableInfo(table)
	if err != nil || info == nil || info.AutoIncrement == "" {
		return "", 0, false, nil
	}
	autoCol := info.AutoIncrement

	// 语句内部含非确定性函数时整体放行，交给 run() 逐 token 改写，
	// 避免重建后的 SQL 把 RANDOM()/NOW() 原样带进日志。
	if containsNondeterministicCall(toks, start, rowsEnd) {
		return "", 0, false, nil
	}

	// 自增列已显式给出：有序重放下结果天然一致，无需改写
	if hasColumnList && contains(columns, autoCol) {
		return "", 0, false, nil
	}
	// 无列清单的 VALUES 形式：值按表列顺序映射，自增列位置必然已包含，
	// 有序重放下结果天然一致，无需改写。
	if !hasColumnList {
		return "", 0, false, nil
	}

	// 校验行值数量与列数匹配，不匹配时交给执行器报错
	colCount := len(info.Columns)
	if hasColumnList {
		colCount = len(columns)
	}
	for _, r := range rows {
		if rowValueCount(r) != colCount {
			return "", 0, false, nil
		}
	}

	ids, err := rw.nextIDs(table, int64(len(rows)))
	if err != nil {
		return "", 0, false, nil
	}

	return rebuildInsertValues(toks, start, rowsEnd, hasColumnList, columns, rows, autoCol, ids), rowsEnd, true, nil
}

// containsNondeterministicCall 判断 token 区间内是否存在本包负责改写的
// 非确定性函数调用。
func containsNondeterministicCall(toks []token, start, end int) bool {
	for i := start; i < end; i++ {
		t := toks[i]
		switch {
		case t.isKeyword("NOW"), t.isKeyword("RANDOM"), t.isKeyword("RANDOMBLOB"):
			if _, ok := matchCall(toks, i); ok {
				return true
			}
		case t.isKeyword("CURRENT_TIMESTAMP"), t.isKeyword("CURRENT_DATE"), t.isKeyword("CURRENT_TIME"):
			return true
		}
	}
	return false
}

// nextIDs 为表生成 n 个连续自增 ID，并记录到结果 Sequence。
func (rw *rewriter) nextIDs(table string, n int64) ([]int64, error) {
	next, err := rw.schema.NextAutoIncrement(table)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, n)
	for i := int64(0); i < n; i++ {
		ids[i] = next + i
	}
	if rw.sequence == nil {
		rw.sequence = make(map[string]int64)
	}
	rw.sequence[table] = next
	return ids, nil
}

// ---------- 输出重建 ----------

func rebuildInsertDefault(toks []token, start, end int, col string, id int64) string {
	var sb strings.Builder
	i := start
	for i < end {
		t := toks[i]
		if t.isKeyword("DEFAULT") {
			sb.WriteString("(" + quoteIdent(col) + ") VALUES (" + strconv.FormatInt(id, 10) + ")")
			j := skipSpace(toks, i+1) // 定位 VALUES
			i = j + 1                 // 跳过 DEFAULT 与 VALUES
			continue
		}
		sb.WriteString(t.text)
		i++
	}
	return sb.String()
}

func rebuildInsertValues(toks []token, start, end int, hasColumnList bool,
	columns []string, rows [][]token, autoCol string, ids []int64) string {

	var sb strings.Builder
	// 阶段一：INSERT..INTO..table [cols]，在列清单内补自增列
	i := start
	for ; i < end; i++ {
		t := toks[i]
		if hasColumnList && t.text == "(" {
			closeIdx, ok := matchParen(toks, i)
			if ok {
				for j := i; j < closeIdx-1; j++ {
					sb.WriteString(toks[j].text)
				}
				sb.WriteString(", " + quoteIdent(autoCol))
				sb.WriteString(toks[closeIdx-1].text) // 右括号
				i = closeIdx - 1
				continue
			}
		}
		sb.WriteString(t.text)
		if t.isKeyword("VALUES") {
			i++
			break
		}
	}

	// 阶段二：VALUES 行，每行末尾追加对应 ID
	rowIdx := 0
	for ; i < end; i++ {
		t := toks[i]
		if t.text == "(" {
			closeIdx, ok := matchParen(toks, i)
			if !ok {
				sb.WriteString(t.text)
				continue
			}
			for j := i; j < closeIdx-1; j++ {
				sb.WriteString(toks[j].text)
			}
			if rowIdx < len(ids) {
				sb.WriteString(", " + strconv.FormatInt(ids[rowIdx], 10))
				rowIdx++
			}
			sb.WriteString(toks[closeIdx-1].text)
			i = closeIdx - 1
			continue
		}
		sb.WriteString(t.text)
	}
	return sb.String()
}

// ---------- 小工具 ----------

func quoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// matchCall 判断 tokens[i] 是否为 `IDENT(` 形式的调用，返回闭括号后一个位置。
func matchCall(toks []token, i int) (int, bool) {
	if i+1 >= len(toks) {
		return 0, false
	}
	j := i + 1
	for j < len(toks) && toks[j].isSkippable() {
		j++
	}
	if j >= len(toks) || toks[j].text != "(" {
		return 0, false
	}
	end, ok := matchParen(toks, j)
	return end, ok
}

// callHasEmptyArgs 判断 `IDENT(...)` 的参数是否为空。
// start 指向函数名，end 是闭括号后一个位置；括号之间只允许空白。
func callHasEmptyArgs(toks []token, start, end int) bool {
	count := 0
	for i := start + 1; i < end-1; i++ {
		if !toks[i].isSkippable() {
			count++ // 只允许 '(' 本身出现
		}
	}
	return count <= 1
}

// matchParen 从 '(' 位置开始匹配括号，返回闭括号后一个位置。
func matchParen(toks []token, open int) (int, bool) {
	depth := 0
	for i := open; i < len(toks); i++ {
		switch toks[i].text {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				return i + 1, true
			}
		}
	}
	return 0, false
}

// parseTableName 解析 [schema.]table，返回表名与后续位置。
func parseTableName(toks []token, i int) (string, int, bool) {
	name, ok := identText(toks, i)
	if !ok {
		return "", 0, false
	}
	i = skipSpace(toks, i+1)
	if i < len(toks) && toks[i].text == "." {
		j := skipSpace(toks, i+1)
		last, ok := identText(toks, j)
		if !ok {
			return "", 0, false
		}
		name = last
		i = skipSpace(toks, j+1)
	}
	return name, i, true
}

// identText 提取标识符或带引号标识符的文本（去引号）。
func identText(toks []token, i int) (string, bool) {
	if i >= len(toks) {
		return "", false
	}
	t := toks[i]
	switch t.kind {
	case tokIdent:
		return t.text, true
	case tokString:
		// '...' / "..." / `...` / [...]
		if len(t.text) >= 2 {
			return unquoteIdent(t.text), true
		}
	}
	return "", false
}

func unquoteIdent(s string) string {
	if len(s) < 2 {
		return s
	}
	body := s[1 : len(s)-1]
	body = strings.ReplaceAll(body, "''", "'")
	body = strings.ReplaceAll(body, `""`, `"`)
	body = strings.ReplaceAll(body, "``", "`")
	return body
}

// parseParenList 解析 `(a, b, c)` 形式的顶层逗号分隔标识符列表。
func parseParenList(toks []token, open int) ([]string, int, bool) {
	closeIdx, ok := matchParen(toks, open)
	if !ok {
		return nil, 0, false
	}
	var items []string
	depth := 0
	for i := open + 1; i < closeIdx-1; i++ {
		t := toks[i]
		if t.text == "(" {
			depth++
			continue
		}
		if t.text == ")" {
			depth--
			continue
		}
		if depth == 0 && t.text == "," {
			continue
		}
		if depth == 0 && t.kind != tokSpace && t.kind != tokComment {
			if name, ok := identText(toks, i); ok {
				items = append(items, name)
			}
		}
	}
	return items, closeIdx, true
}

// skipSpace 跳过空白与注释 token，返回第一个有效 token 的位置。
func skipSpace(toks []token, i int) int {
	for i < len(toks) && toks[i].isSkippable() {
		i++
	}
	return i
}

// parseValuesRows 解析 `VALUES (..), (..)`，返回每个括号组的 token 切片。
func parseValuesRows(toks []token, valuesIdx int) (int, [][]token, bool) {
	var rows [][]token
	i := valuesIdx + 1
	for {
		for i < len(toks) && toks[i].isSkippable() {
			i++
		}
		if i >= len(toks) || toks[i].text != "(" {
			return 0, nil, false
		}
		closeIdx, ok := matchParen(toks, i)
		if !ok {
			return 0, nil, false
		}
		rows = append(rows, toks[i:closeIdx])
		i = closeIdx
		for i < len(toks) && toks[i].isSkippable() {
			i++
		}
		if i < len(toks) && toks[i].text == "," {
			i++
			continue
		}
		return i, rows, true
	}
}

// rowValueCount 统计一行 VALUES 中顶层逗号分隔的表达式数量
// （行切片包含外层括号，参数表达式中的嵌套括号不会误计）。
func rowValueCount(row []token) int {
	count := 1
	depth := 0
	for i := 1; i < len(row)-1; i++ {
		switch row[i].text {
		case "(":
			depth++
		case ")":
			depth--
		case ",":
			if depth == 0 {
				count++
			}
		}
	}
	return count
}

// ---------- 分词器 ----------

type tokKind int

const (
	tokSpace   tokKind = iota // 空白
	tokComment                // 注释（-- 或 /* */）
	tokIdent                  // 标识符 / 关键字 / 数字
	tokString                 // '..'、".."、`..`、[..]
	tokPunct                  // 单个符号 / 运算符
)

type token struct {
	kind  tokKind
	text  string
	upper string // 仅对标识符有效
}

func (t token) isKeyword(kw string) bool {
	return t.kind == tokIdent && t.upper == kw
}

func (t token) isSkippable() bool {
	return t.kind == tokSpace || t.kind == tokComment
}

func tokenize(sql string) []token {
	var toks []token
	i := 0
	for i < len(sql) {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			j := i + 1
			for j < len(sql) {
				c2 := sql[j]
				if c2 != ' ' && c2 != '\t' && c2 != '\n' && c2 != '\r' && c2 != '\f' && c2 != '\v' {
					break
				}
				j++
			}
			toks = append(toks, token{kind: tokSpace, text: sql[i:j]})
			i = j
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			j := i + 2
			for j < len(sql) && sql[j] != '\n' {
				j++
			}
			toks = append(toks, token{kind: tokComment, text: sql[i:j]})
			i = j
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			j := strings.Index(sql[i+2:], "*/")
			if j < 0 {
				toks = append(toks, token{kind: tokComment, text: sql[i:]})
				i = len(sql)
			} else {
				toks = append(toks, token{kind: tokComment, text: sql[i : i+j+4]})
				i += j + 4
			}
		case c == '\'' || c == '"' || c == '`':
			j, ok := scanQuoted(sql, i, c)
			if !ok {
				j = len(sql)
			}
			toks = append(toks, token{kind: tokString, text: sql[i:j]})
			i = j
		case c == '[':
			j := strings.IndexByte(sql[i:], ']')
			if j < 0 {
				toks = append(toks, token{kind: tokString, text: sql[i:]})
				i = len(sql)
			} else {
				toks = append(toks, token{kind: tokString, text: sql[i : i+j+1]})
				i += j + 1
			}
		case isIdentStart(c):
			j := i + 1
			for j < len(sql) && isIdentPart(sql[j]) {
				j++
			}
			text := sql[i:j]
			toks = append(toks, token{kind: tokIdent, text: text, upper: strings.ToUpper(text)})
			i = j
		default:
			toks = append(toks, token{kind: tokPunct, text: sql[i : i+1]})
			i++
		}
	}
	return toks
}

func scanQuoted(sql string, start int, quote byte) (int, bool) {
	i := start + 1
	for i < len(sql) {
		if sql[i] == quote {
			if i+1 < len(sql) && sql[i+1] == quote {
				i += 2 // 转义引号
				continue
			}
			return i + 1, true
		}
		i++
	}
	return len(sql), false
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func isIdentPart(c byte) bool {
	return c == '_' || c == '$' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// atStatementStart 判断 token 位置 i 之前是否只有空白/注释，
// 确保 INSERT 改写只作用于语句开头的写操作，避免误改触发器内的 INSERT。
func atStatementStart(toks []token, i int) bool {
	for j := 0; j < i; j++ {
		if !toks[j].isSkippable() {
			return false
		}
	}
	return true
}
