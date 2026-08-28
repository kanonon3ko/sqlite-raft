// 兼容层 shim：拦截客户端常见的“元查询”，直接应答，
// 避免把 pg_catalog 等 PG 专属 SQL 交给 SQLite。
package pgwire

import (
	"strconv"
	"strings"
	"time"
)

// shimInfo 描述一次 shim 应答。
type shimInfo struct {
	// query 非空时返回一行结果
	column  string
	typeOID uint32
	value   string
	// tag 非空时只返回 CommandComplete（SET/BEGIN 等）
	tag string
}

// shimFor 尝试把 PG 元语句映射为本地应答。ok=false 表示无需 shim。
func shimFor(sql, dbName, user, host, port string, pid uint32, started time.Time) (shimInfo, bool) {
	norm := normalizeSQL(sql)
	if norm == "" {
		return shimInfo{}, false
	}

	// 空操作语句
	for _, prefix := range []string{"set ", "reset "} {
		if strings.HasPrefix(norm, prefix) {
			return shimInfo{tag: strings.ToUpper(strings.TrimSpace(strings.SplitN(norm, " ", 2)[0]))}, true
		}
	}
	switch {
	case strings.HasPrefix(norm, "savepoint "):
		return shimInfo{tag: "SAVEPOINT"}, true
	case strings.HasPrefix(norm, "release "):
		return shimInfo{tag: "RELEASE"}, true
	case strings.HasPrefix(norm, "rollback to "):
		return shimInfo{tag: "ROLLBACK"}, true
	case norm == "discard all" || strings.HasPrefix(norm, "discard "):
		return shimInfo{tag: "DISCARD ALL"}, true
	case strings.HasPrefix(norm, "listen ") || strings.HasPrefix(norm, "unlisten ") ||
		strings.HasPrefix(norm, "notify "):
		return shimInfo{tag: strings.ToUpper(strings.SplitN(norm, " ", 2)[0])}, true
	case strings.HasPrefix(norm, "deallocate ") || norm == "deallocate all":
		return shimInfo{tag: "DEALLOCATE"}, true
	case strings.HasPrefix(norm, "prepare "):
		return shimInfo{tag: "PREPARE"}, true
	}

	// SHOW xxx
	if strings.HasPrefix(norm, "show ") {
		name := strings.TrimSpace(strings.TrimPrefix(norm, "show "))
		name = strings.Trim(name, "'\"")
		if v, ok := settingValue(name, user); ok {
			return shimInfo{column: name, typeOID: 25, value: v}, true
		}
		return shimInfo{tag: "SHOW"}, true
	}

	// SELECT 元查询
	switch {
	case strings.Contains(norm, "set_config("):
		return shimInfo{column: "set_config", typeOID: 16, value: "t"}, true
	case strings.Contains(norm, "version()"):
		return shimInfo{column: "version", typeOID: 25, value: "PostgreSQL 16.0 (sqlraft)"}, true
	case strings.Contains(norm, "current_database("):
		return shimInfo{column: "current_database", typeOID: 25, value: dbName}, true
	case strings.Contains(norm, "current_schema("):
		return shimInfo{column: "current_schema", typeOID: 25, value: "public"}, true
	case strings.Contains(norm, "pg_backend_pid("):
		return shimInfo{column: "pg_backend_pid", typeOID: 20, value: strconv.FormatUint(uint64(pid), 10)}, true
	case strings.Contains(norm, "pg_is_in_recovery("):
		return shimInfo{column: "pg_is_in_recovery", typeOID: 16, value: "f"}, true
	case strings.Contains(norm, "inet_server_port("):
		return shimInfo{column: "inet_server_port", typeOID: 20, value: port}, true
	case strings.Contains(norm, "inet_server_addr("):
		return shimInfo{column: "inet_server_addr", typeOID: 25, value: host}, true
	case strings.Contains(norm, "pg_postmaster_start_time("):
		return shimInfo{column: "pg_postmaster_start_time", typeOID: 1114,
			value: started.UTC().Format("2006-01-02 15:04:05")}, true
	case strings.Contains(norm, "current_setting("):
		name := extractSettingArg(norm)
		if v, ok := settingValue(name, user); ok {
			return shimInfo{column: "current_setting", typeOID: 25, value: v}, true
		}
	case norm == "select current_user" || norm == "select session_user" || norm == "select user":
		return shimInfo{column: "current_user", typeOID: 25, value: user}, true
	}
	return shimInfo{}, false
}

// settingValue 返回 PG 运行参数的本地应答。
func settingValue(name, user string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "server_version":
		return "16.0 (sqlraft)", true
	case "server_version_num":
		return "160000", true
	case "server_encoding", "client_encoding":
		return "UTF8", true
	case "datestyle", "datestyles":
		return "ISO, MDY", true
	case "timezone":
		return "UTC", true
	case "integer_datetimes":
		return "on", true
	case "standard_conforming_strings":
		return "on", true
	case "transaction_isolation", "default_transaction_isolation":
		return "read committed", true
	case "default_transaction_read_only", "transaction_read_only":
		return "off", true
	case "max_identifier_length":
		return "63", true
	case "search_path":
		return "public", true
	case "application_name":
		return "", true
	case "statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout":
		return "0", true
	case "extra_float_digits":
		return "3", true
	case "is_superuser":
		return "off", true
	case "session_authorization":
		return user, true
	case "intervalstyle":
		return "postgres", true
	}
	return "", false
}

// extractSettingArg 提取 current_setting('name') 的第一个字符串参数。
func extractSettingArg(norm string) string {
	i := strings.Index(norm, "(")
	if i < 0 {
		return ""
	}
	rest := norm[i+1:]
	j := strings.IndexByte(rest, '\'')
	if j < 0 {
		return ""
	}
	rest = rest[j+1:]
	k := strings.IndexByte(rest, '\'')
	if k < 0 {
		return ""
	}
	return rest[:k]
}

// normalizeSQL 小写化、折叠空白并去掉末尾分号，便于模式匹配。
func normalizeSQL(sql string) string {
	s := strings.TrimSpace(sql)
	s = strings.TrimSuffix(s, ";")
	s = strings.TrimSpace(s)
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}
