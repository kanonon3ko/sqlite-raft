// SQL 方言适配：把 PostgreSQL 风格的语句翻译成 SQLite 可执行的形式。
//
// 处理范围：
//   - $N 参数 → SQLite 的 ?N 编号占位符；
//   - `expr::type` → CAST(expr AS type)（SQLite 不支持 ::）；
//   - ILIKE → LIKE；
//   - CREATE/ALTER TABLE 中的 PG 类型名（SERIAL/VARCHAR/BOOLEAN/BYTEA/...）；
//   - 读路径上 NOW() → SQLite 时间函数（写路径交给 raft 的确定性改写）。
package pgwire

import "strings"

// stmtKind 表示一条语句的路由类别。
type stmtKind int

const (
	kindRead  stmtKind = iota // 本地读（SELECT 等）
	kindWrite                 // 走 Raft 复制的写
	kindNoop                  // 空操作（SET/BEGIN/COMMIT 等）
)

var readKeywords = map[string]bool{
	"SELECT": true, "VALUES": true, "PRAGMA": true, "EXPLAIN": true,
	"SHOW": true, "TABLE": true, "WITH": true, "FETCH": true,
}

var noopKeywords = map[string]bool{
	"SET": true, "RESET": true, "BEGIN": true, "START": true, "COMMIT": true,
	"END": true, "ABORT": true, "ROLLBACK": true, "SAVEPOINT": true,
	"RELEASE": true, "DISCARD": true, "LISTEN": true, "UNLISTEN": true,
	"NOTIFY": true, "DEALLOCATE": true, "PREPARE": true, "DECLARE": true,
	"CLOSE": true, "MOVE": true, "CALL": true,
}

// splitStatements 按分号切分语句，分号位于引号/注释/美元引号内时不计。
func splitStatements(sql string) []string {
	var out []string
	start := 0
	st := scanState{}
	for i := 0; i < len(sql); i++ {
		if st.feed(sql, i) == scanBreak {
			if s := strings.TrimSpace(sql[start:i]); s != "" {
				out = append(out, s)
			}
			start = i + 1
		}
	}
	if s := strings.TrimSpace(sql[start:]); s != "" {
		out = append(out, s)
	}
	return out
}

// firstKeyword 返回语句的第一个关键字（大写），忽略前导空白/注释。
func firstKeyword(sql string) string {
	st := scanState{}
	var kw strings.Builder
	for i := 0; i < len(sql); i++ {
		st.feed(sql, i)
		if st.inNormal() {
			c := sql[i]
			if c == '_' || c == '$' || isLetter(c) {
				for i < len(sql) && (isLetter(sql[i]) || isDigit(sql[i]) || sql[i] == '_' || sql[i] == '$') {
					kw.WriteByte(sql[i])
					i++
				}
				break
			}
			if c == ';' {
				break
			}
		}
	}
	return strings.ToUpper(kw.String())
}

// classify 判断一条语句的路由类别。WITH 开头的语句会解析 CTE 找到主动词。
func classify(stmt string) stmtKind {
	kw := firstKeyword(stmt)
	if kw == "WITH" {
		kw = mainVerbAfterCTE(stmt)
	}
	if readKeywords[kw] {
		return kindRead
	}
	if noopKeywords[kw] {
		return kindNoop
	}
	// 未识别的关键字按写路径处理（错误会在状态机中确定性暴露）
	return kindWrite
}

// mainVerbAfterCTE 解析 `WITH [RECURSIVE] name [(cols)] AS (...), ... <main>`
// 并返回主语句的第一个关键字。
func mainVerbAfterCTE(sql string) string {
	st := scanState{}
	i := 0
	for i < len(sql) {
		st.feed(sql, i)
		if !st.inNormal() {
			i++
			continue
		}
		c := sql[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		break
	}
	// 此时 i 在 WITH 处
	i += len("WITH")
	// 可选 RECURSIVE
	i = skipWord(sql, i, "RECURSIVE")
	i = skipSpaces(sql, i)

	for i < len(sql) {
		// CTE 名（可能带引号）
		if !isIdentStart(sql[i]) && sql[i] != '"' && sql[i] != '`' && sql[i] != '[' {
			break
		}
		i = skipIdent(sql, i)
		i = skipSpaces(sql, i)
		// 可选列清单 (a, b)
		if i < len(sql) && sql[i] == '(' {
			var ok bool
			i, ok = skipParenGroup(sql, i)
			if !ok {
				break
			}
			i = skipSpaces(sql, i)
		}
		// AS
		if strings.HasPrefix(strings.ToUpper(sql[i:]), "AS") && !isIdentPart(byteAt(sql, i+2)) {
			i += 2
		} else {
			break
		}
		i = skipSpaces(sql, i)
		// 子查询括号组
		if i < len(sql) && sql[i] == '(' {
			var ok bool
			i, ok = skipParenGroup(sql, i)
			if !ok {
				break
			}
		} else {
			break
		}
		i = skipSpaces(sql, i)
		if i < len(sql) && sql[i] == ',' {
			i++
			i = skipSpaces(sql, i)
			continue
		}
		// 主动词
		return firstKeyword(sql[i:])
	}
	return ""
}

func skipSpaces(sql string, i int) int {
	for i < len(sql) && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r') {
		i++
	}
	return i
}

// skipWord 在 i 处匹配（不区分大小写）给定单词时跳过。
func skipWord(sql string, i int, word string) int {
	if len(sql) >= i+len(word) && strings.EqualFold(sql[i:i+len(word)], word) {
		j := i + len(word)
		if j >= len(sql) || !isIdentPart(sql[j]) {
			return j
		}
	}
	return i
}

// skipIdent 跳过标识符（字母/数字/下划线/$）。
func skipIdent(sql string, i int) int {
	if sql[i] == '"' || sql[i] == '`' || sql[i] == '[' {
		q := sql[i]
		j := i + 1
		if q == '[' {
			for j < len(sql) && sql[j] != ']' {
				j++
			}
			return j + 1
		}
		for j < len(sql) {
			if sql[j] == q {
				if j+1 < len(sql) && sql[j+1] == q {
					j += 2
					continue
				}
				return j + 1
			}
			j++
		}
		return j
	}
	for i < len(sql) && (isLetter(sql[i]) || isDigit(sql[i]) || sql[i] == '_' || sql[i] == '$') {
		i++
	}
	return i
}

// skipParenGroup 跳过从 i 处 '(' 开始的括号组（处理引号与嵌套）。
func skipParenGroup(sql string, i int) (int, bool) {
	if i >= len(sql) || sql[i] != '(' {
		return i, false
	}
	st := scanState{}
	depth := 0
	for j := i; j < len(sql); j++ {
		if st.feed(sql, j) != scanNone {
			continue
		}
		switch sql[j] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return j + 1, true
			}
		}
	}
	return i, false
}

// translateDialect 做通用的 PG→SQLite 翻译（读写都适用）。
// 先做 :: 转换（基于原始字符串切片，避免输出长度错位），再逐 token 处理
// $N 参数与 ILIKE。
func translateDialect(sql string) string {
	sql = rewriteCasts(sql)
	var out strings.Builder
	st := scanState{}
	for i := 0; i < len(sql); i++ {
		st.feed(sql, i)
		if !st.inNormal() {
			out.WriteByte(sql[i])
			continue
		}
		c := sql[i]
		switch {
		case c == '$':
			// 数字参数 $N → ?N；或美元引号字符串
			if i+1 < len(sql) && isDigit(sql[i+1]) {
				j := i + 1
				for j < len(sql) && isDigit(sql[j]) {
					j++
				}
				out.WriteString("?" + sql[i+1:j])
				i = j - 1
				continue
			}
			if end, ok := scanDollarQuote(sql, i); ok {
				out.WriteString(sql[i:end])
				i = end - 1
				continue
			}
			out.WriteByte(c)
		case isLetter(c) || c == '_':
			j := i
			for j < len(sql) && (isLetter(sql[j]) || isDigit(sql[j]) || sql[j] == '_' || sql[j] == '$') {
				j++
			}
			word := sql[i:j]
			if strings.EqualFold(word, "ILIKE") {
				out.WriteString("LIKE")
			} else {
				out.WriteString(word)
			}
			i = j - 1
		default:
			out.WriteByte(c)
		}
	}
	return out.String()
}

// rewriteCasts 把语句中引号外的 `expr::type` 全部转换为 `CAST(expr AS type)`。
func rewriteCasts(sql string) string {
	var out []byte
	last := 0
	st := scanState{}
	for i := 0; i+1 < len(sql); i++ {
		st.feed(sql, i)
		if !st.inNormal() || sql[i] != ':' || sql[i+1] != ':' {
			continue
		}
		exprStart, typeEnd, typeName, ok := castRange(sql, i)
		if !ok {
			continue
		}
		out = append(out, sql[last:exprStart]...)
		out = append(out, "CAST("+sql[exprStart:i]+" AS "+typeName+")"...)
		i = typeEnd - 1
		last = typeEnd
	}
	out = append(out, sql[last:]...)
	return string(out)
}

// castRange 解析 `expr::type`，返回表达式起点、类型名结束位置与类型名。
func castRange(sql string, i int) (int, int, string, bool) {
	// 1) 解析 :: 之后的类型名
	j := i + 2
	for j < len(sql) && (sql[j] == ' ' || sql[j] == '\t' || sql[j] == '\n' || sql[j] == '\r') {
		j++
	}
	typeName, typeEnd, ok := matchCastType(sql, j)
	if !ok {
		return 0, 0, "", false
	}

	// 2) 向前回溯表达式起点
	k := i - 1
	for k >= 0 && (sql[k] == ' ' || sql[k] == '\t' || sql[k] == '\n' || sql[k] == '\r') {
		k--
	}
	if k < 0 {
		return 0, 0, "", false
	}
	var start int
	switch {
	case sql[k] == ')':
		depth := 0
		m := k
		for ; m >= 0; m-- {
			switch sql[m] {
			case ')':
				depth++
			case '(':
				depth--
				if depth == 0 {
					start = m
					goto found
				}
			}
		}
		return 0, 0, "", false
	case isLetter(sql[k]) || isDigit(sql[k]) || sql[k] == '_' || sql[k] == '\'' || sql[k] == '"' || sql[k] == '`':
		m := k
		for m >= 0 && (isLetter(sql[m]) || isDigit(sql[m]) || sql[m] == '_' || sql[m] == '.' || sql[m] == '$' || sql[m] == '\'' || sql[m] == '"' || sql[m] == '`') {
			m--
		}
		start = m + 1
	default:
		return 0, 0, "", false
	}
found:
	return start, typeEnd, typeName, true
}

// matchCastType 从 j 处读取类型名，只匹配已知 PG 类型集合，
// 避免把后续的 FROM/WHERE 等子句吞进 CAST。
func matchCastType(sql string, j int) (string, int, bool) {
	type word struct {
		text string
		end  int
	}
	var words []word
	pos := j
	for len(words) < 5 && pos < len(sql) {
		// 跳过空白
		for pos < len(sql) && (sql[pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\n' || sql[pos] == '\r') {
			pos++
		}
		if pos >= len(sql) || !isLetter(sql[pos]) {
			break
		}
		start := pos
		for pos < len(sql) && (isLetter(sql[pos]) || isDigit(sql[pos]) || sql[pos] == '_') {
			pos++
		}
		words = append(words, word{text: sql[start:pos], end: pos})
	}
	// 从最长组合开始匹配，优先吸收多词类型名
	for n := len(words); n >= 1; n-- {
		parts := make([]string, n)
		for k := 0; k < n; k++ {
			parts[k] = words[k].text
		}
		if name, ok := castTypes[strings.ToUpper(strings.Join(parts, " "))]; ok {
			return name, words[n-1].end, true
		}
	}
	return "", 0, false
}

// castTypes 是支持的 :: 类型名映射（值 = 输出到 CAST 的类型名）。
var castTypes = map[string]string{
	"TEXT": "TEXT", "VARCHAR": "TEXT", "CHAR": "TEXT", "CHARACTER VARYING": "TEXT",
	"INT": "INTEGER", "INTEGER": "INTEGER", "INT2": "INTEGER", "INT4": "INTEGER",
	"INT8": "INTEGER", "BIGINT": "INTEGER", "SMALLINT": "INTEGER",
	"REAL": "REAL", "FLOAT": "REAL", "FLOAT4": "REAL", "FLOAT8": "REAL",
	"DOUBLE PRECISION": "REAL",
	"NUMERIC":          "NUMERIC", "DECIMAL": "NUMERIC",
	"BOOL": "INTEGER", "BOOLEAN": "INTEGER",
	"BYTEA":     "BLOB",
	"TIMESTAMP": "TEXT", "TIMESTAMPTZ": "TEXT", "DATE": "TEXT", "TIME": "TEXT",
	"TIMESTAMP WITH TIME ZONE": "TEXT", "TIMESTAMP WITHOUT TIME ZONE": "TEXT",
	"UUID": "TEXT", "JSON": "TEXT", "JSONB": "TEXT",
}

// translateReadFunctions 把读路径上的 NOW() 翻译为 SQLite 时间函数。
// 写路径不能调用本函数：NOW() 必须由 raft 的确定性改写处理。
func translateReadFunctions(sql string) string {
	var out strings.Builder
	st := scanState{}
	for i := 0; i < len(sql); i++ {
		st.feed(sql, i)
		if !st.inNormal() {
			out.WriteByte(sql[i])
			continue
		}
		c := sql[i]
		if isLetter(c) || c == '_' {
			j := i
			for j < len(sql) && (isLetter(sql[j]) || isDigit(sql[j]) || sql[j] == '_' || sql[j] == '$') {
				j++
			}
			word := sql[i:j]
			if strings.EqualFold(word, "NOW") && j < len(sql) && sql[j] == '(' {
				// NOW(...) → strftime('%Y-%m-%d %H:%M:%f', 'now')
				out.WriteString("strftime('%Y-%m-%d %H:%M:%f', 'now')")
				// 跳过整个 NOW(...) 调用（含参数）
				if end, ok := skipParenGroup(sql, j); ok {
					i = end - 1
					continue
				}
				i = j
				continue
			}
			out.WriteString(word)
			i = j - 1
			continue
		}
		out.WriteByte(c)
	}
	return out.String()
}

// translateDdlTypes 把 CREATE/ALTER TABLE 中的 PG 类型名改写为 SQLite 类型。
func translateDdlTypes(sql string) string {
	kw := firstKeyword(sql)
	if kw != "CREATE" && kw != "ALTER" {
		return sql
	}
	return rewriteColumnTypes(sql)
}

// rewriteColumnTypes 解析 CREATE/ALTER TABLE 的列定义并改写类型名，
// 只处理“列名之后”的类型位置，避免误改列名或约束中的关键字。
func rewriteColumnTypes(sql string) string {
	toks := ddlTokenize(sql)
	kw := firstKeyword(sql)
	switch kw {
	case "CREATE":
		return rewriteCreateTable(sql, toks)
	case "ALTER":
		return rewriteAlterTable(sql, toks)
	}
	return sql
}

// ddlToken 是 DDL 改写用的轻量 token。
type ddlToken struct {
	text  string
	upper string
	start int
	end   int
}

// ddlTokenize 按引号/注释感知的方式切出词法单元。
func ddlTokenize(sql string) []ddlToken {
	var toks []ddlToken
	st := scanState{}
	for i := 0; i < len(sql); i++ {
		st.feed(sql, i)
		if !st.inNormal() {
			continue
		}
		c := sql[i]
		if c == '\'' || c == '"' || c == '`' || c == '[' || c == ';' ||
			c == '(' || c == ')' || c == ',' {
			toks = append(toks, ddlToken{text: string(c), upper: string(c), start: i, end: i + 1})
			continue
		}
		if isLetter(c) || c == '_' {
			j := i
			for j < len(sql) && (isLetter(sql[j]) || isDigit(sql[j]) || sql[j] == '_' || sql[j] == '$') {
				j++
			}
			toks = append(toks, ddlToken{text: sql[i:j], upper: strings.ToUpper(sql[i:j]), start: i, end: j})
			i = j - 1
			continue
		}
		if isDigit(c) || c == '-' || c == '+' || c == '.' {
			j := i
			for j < len(sql) && (isDigit(sql[j]) || sql[j] == '.' || sql[j] == 'e' || sql[j] == 'E' ||
				sql[j] == '-' || sql[j] == '+') {
				j++
			}
			toks = append(toks, ddlToken{text: sql[i:j], upper: sql[i:j], start: i, end: j})
			i = j - 1
			continue
		}
	}
	return toks
}

// rewriteCreateTable 处理 CREATE [TEMP] TABLE [IF NOT EXISTS] name (cols)。
func rewriteCreateTable(sql string, toks []ddlToken) string {
	// 定位 "TABLE" 关键字
	tableIdx := -1
	for i, t := range toks {
		if t.upper == "TABLE" && i > 0 && (toks[i-1].upper == "CREATE" || toks[i-1].upper == "TEMP" ||
			toks[i-1].upper == "TEMPORARY") {
			tableIdx = i
			break
		}
	}
	if tableIdx < 0 {
		return sql
	}
	// 跳过 IF NOT EXISTS 与表名，找到列定义括号组
	open := -1
	for i := tableIdx + 1; i < len(toks); i++ {
		if toks[i].upper == "IF" {
			i += 3 // IF NOT EXISTS
			continue
		}
		if toks[i].text == "(" {
			open = i
			break
		}
		if toks[i].upper == "AS" {
			return sql // CREATE TABLE ... AS SELECT：无列类型
		}
	}
	if open < 0 {
		return sql
	}
	closeIdx := -1
	depth := 0
	for i := open; i < len(toks); i++ {
		switch toks[i].text {
		case "(":
			depth++
		case ")":
			depth--
			if depth == 0 {
				closeIdx = i
				i = len(toks)
			}
		}
	}
	if closeIdx < 0 {
		return sql
	}

	// 逐段改写列定义（顶层逗号分隔）
	segments := splitTopLevel(toks[open+1 : closeIdx])
	rewritten := make([]string, 0, len(segments))
	for _, seg := range segments {
		rewritten = append(rewritten, rewriteColumnSegment(sql, seg))
	}

	var out strings.Builder
	out.WriteString(sql[:toks[open].start])
	out.WriteString("(")
	for i, seg := range rewritten {
		if i > 0 {
			out.WriteString(", ")
		}
		out.WriteString(seg)
	}
	out.WriteString(")")
	out.WriteString(sql[toks[closeIdx].end:])
	return out.String()
}

// rewriteAlterTable 处理 ALTER TABLE ... ADD COLUMN。
func rewriteAlterTable(sql string, toks []ddlToken) string {
	addIdx := -1
	for i, t := range toks {
		if t.upper == "ADD" && i+1 < len(toks) && toks[i+1].upper == "COLUMN" {
			addIdx = i + 1
			break
		}
	}
	if addIdx < 0 {
		return sql
	}
	// ADD COLUMN 之后的列定义 = 从列名到语句结束（或逗号/分号）
	seg := toks[addIdx+1:]
	if len(seg) == 0 {
		return sql
	}
	rewritten := rewriteColumnSegment(sql, seg)
	return sql[:seg[0].start] + rewritten
}

// rewriteColumnSegment 改写一段列定义中的类型名。
// 段以 ddlToken 切片给出，输出重建的文本。
func rewriteColumnSegment(sql string, seg []ddlToken) string {
	raw := func() string {
		return sql[seg[0].start:seg[len(seg)-1].end]
	}
	// 跳过表约束（PRIMARY KEY / UNIQUE / CHECK / FOREIGN KEY / CONSTRAINT）
	if len(seg) > 0 {
		switch seg[0].upper {
		case "PRIMARY", "UNIQUE", "CHECK", "FOREIGN", "CONSTRAINT":
			return raw()
		}
	}
	if len(seg) < 2 {
		return raw()
	}
	// 类型 = 列名后的连续标识符序列
	typeEnd := 1
	for typeEnd < len(seg) {
		t := seg[typeEnd]
		if t.text == "(" {
			break // 类型参数 (n)
		}
		if t.upper == "PRIMARY" || t.upper == "NOT" || t.upper == "UNIQUE" ||
			t.upper == "CHECK" || t.upper == "DEFAULT" || t.upper == "COLLATE" ||
			t.upper == "REFERENCES" || t.upper == "GENERATED" || t.upper == "AS" ||
			t.upper == "NULL" || t.upper == "CONSTRAINT" || t.upper == "AUTOINCREMENT" {
			break
		}
		typeEnd++
	}
	typeName := make([]string, 0, typeEnd-1)
	for i := 1; i < typeEnd; i++ {
		typeName = append(typeName, seg[i].text)
	}
	translated := translateTypeName(strings.Join(typeName, " "))
	if translated == strings.Join(typeName, " ") {
		return raw() // 无需改写，原样保留
	}

	var out strings.Builder
	out.WriteString(sql[seg[0].start:seg[0].end])
	out.WriteString(" " + translated)
	if typeEnd < len(seg) {
		// 从类型名末尾开始保留原空白与后续约束
		out.WriteString(sql[seg[typeEnd-1].end:seg[len(seg)-1].end])
	}
	return out.String()
}

// translateTypeName 把 PG 类型名映射为 SQLite 类型。
func translateTypeName(name string) string {
	switch strings.ToUpper(name) {
	case "SERIAL", "BIGSERIAL", "SMALLSERIAL":
		return "INTEGER"
	case "VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "CLOB", "CHARACTER",
		"CHARACTER VARYING", "VARYING", "TEXT":
		return "TEXT"
	case "BOOLEAN", "BOOL":
		return "INTEGER"
	case "BYTEA":
		return "BLOB"
	case "TIMESTAMP", "TIMESTAMPTZ", "DATETIME",
		"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITHOUT TIME ZONE":
		return "TEXT"
	case "DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT4", "FLOAT8", "REAL":
		return "REAL"
	case "SMALLINT", "BIGINT", "INT2", "INT4", "INT8", "INTEGER", "INT":
		return "INTEGER"
	case "DECIMAL", "NUMERIC", "DEC", "MONEY":
		return "NUMERIC"
	case "UUID", "JSON", "JSONB":
		return "TEXT"
	}
	return name
}

// splitTopLevel 按顶层逗号切分 token 段。
func splitTopLevel(toks []ddlToken) [][]ddlToken {
	var out [][]ddlToken
	var cur []ddlToken
	depth := 0
	for _, t := range toks {
		switch t.text {
		case "(":
			depth++
		case ")":
			depth--
		case ",":
			if depth == 0 {
				out = append(out, cur)
				cur = nil
				continue
			}
		}
		cur = append(cur, t)
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isIdentStart(c byte) bool {
	return isLetter(c) || c == '_'
}

func isIdentPart(c byte) bool {
	return isLetter(c) || isDigit(c) || c == '_' || c == '$'
}

func byteAt(s string, i int) byte {
	if i >= 0 && i < len(s) {
		return s[i]
	}
	return 0
}

// ---------- 扫描器 ----------

type scanEvent int

const (
	scanNone  scanEvent = iota // 普通状态
	scanBreak                  // 在普通状态遇到 ';'
)

// scanState 跟踪引号/注释/美元引号状态。
type scanState struct {
	mode      byte // 0=普通, '\''=单引号, '"'=双引号, '`'=反引号, '['=方括号, '-'=行注释, '*'=块注释
	dollarTag string
	dollarAt  int
	pos       int // 最近一次 feed 的位置
}

// feed 推进状态机到位置 i，返回事件（scanNone 或 scanBreak）。
func (s *scanState) feed(sql string, i int) scanEvent {
	s.pos = i
	if i < s.dollarAt {
		return scanNone
	}
	c := sql[i]
	switch s.mode {
	case 0:
		switch {
		case c == '\'' || c == '"' || c == '`':
			s.mode = c
		case c == '[':
			s.mode = '['
		case c == '-' && byteAt(sql, i+1) == '-':
			s.mode = '-'
		case c == '/' && byteAt(sql, i+1) == '*':
			s.mode = '*'
			return scanNone
		case c == '$':
			if tag, end, ok := dollarQuoteAt(sql, i); ok {
				s.dollarTag = tag
				s.dollarAt = end
			}
		case c == ';':
			return scanBreak
		}
	case '\'':
		if c == '\'' && byteAt(sql, i+1) == '\'' {
			s.dollarAt = i + 2 // 转义引号：跳过下一字节
		} else if c == '\'' {
			s.mode = 0
		}
	case '"':
		if c == '"' && byteAt(sql, i+1) == '"' {
			s.dollarAt = i + 2
		} else if c == '"' {
			s.mode = 0
		}
	case '`':
		if c == '`' {
			s.mode = 0
		}
	case '[':
		if c == ']' {
			s.mode = 0
		}
	case '-':
		if c == '\n' {
			s.mode = 0
		}
	case '*':
		if c == '*' && byteAt(sql, i+1) == '/' {
			s.mode = 0
			s.dollarAt = i + 2
		}
	}
	return scanNone
}

// inNormal 判断当前位置是否在普通（非引号/注释）状态。
func (s *scanState) inNormal() bool {
	return s.mode == 0 && s.pos >= s.dollarAt
}

// scanDollarQuote 判断 i 处是否开始 `$tag$...`，返回整个区域结束位置。
func scanDollarQuote(sql string, i int) (int, bool) {
	if sql[i] != '$' {
		return 0, false
	}
	tag, end, ok := dollarQuoteAt(sql, i)
	if !ok {
		return 0, false
	}
	_ = tag
	return end, true
}

// dollarQuoteAt 解析 i 处的 $tag$ 开头，返回 tag 与结束位置。
func dollarQuoteAt(sql string, i int) (string, int, bool) {
	j := i + 1
	for j < len(sql) && (isLetter(sql[j]) || isDigit(sql[j]) || sql[j] == '_') {
		j++
	}
	if j >= len(sql) || sql[j] != '$' {
		return "", 0, false
	}
	tag := sql[i : j+1]
	rest := sql[j+1:]
	idx := strings.Index(rest, tag)
	if idx < 0 {
		return "", 0, false
	}
	return tag, j + 1 + idx + len(tag), true
}
