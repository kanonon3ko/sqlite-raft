// Package store 提供 SQLite 状态机：把确定性命令应用到本地数据库，并提供查询接口。
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite 驱动，无需 CGO

	"github.com/kanonon3ko/sqlite-raft/gen/logpb"
	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
	"github.com/kanonon3ko/sqlite-raft/internal/rewrite"
)

const metaTable = `CREATE TABLE IF NOT EXISTS sqlraft_meta (
	k TEXT PRIMARY KEY,
	v TEXT NOT NULL
)`

// Column 描述结果集的一列。
type Column struct {
	Name string // 列名
	Type string // SQLite 声明的列类型（DatabaseTypeName）
}

// RawResult 是保留原始 Go 值的结果集（供 PG wire 等按类型编码）。
type RawResult struct {
	Columns []Column
	Rows    [][]any
}

// ApplyOutcome 是一次 Apply 的完整结果，含 RETURNING 返回的行。
type ApplyOutcome struct {
	RowsAffected int64
	LastInsertID int64
	Columns      []Column
	Rows         [][]any // RETURNING 语句返回的行；普通语句为空
}

// Store 封装一个 SQLite 数据库实例。
type Store struct {
	db   *sql.DB
	path string
	mu   sync.Mutex // 保护 db 句柄替换（ApplySnapshot）与所有访问
}

// Open 打开（或创建）指定路径的 SQLite 数据库；path 为空时使用内存库。
func Open(path string) (*Store, error) {
	if path == "" {
		path = ":memory:"
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %q: %w", path, err)
	}
	db.SetMaxOpenConns(1) // SQLite 单写者：串行化写连接

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply %s: %w", p, err)
		}
	}
	if _, err := db.Exec(metaTable); err != nil {
		db.Close()
		return nil, fmt.Errorf("create meta table: %w", err)
	}
	return &Store{db: db, path: path}, nil
}

// Close 关闭底层数据库。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// Snapshot 把当前状态机导出为一致快照字节（VACUUM INTO 临时文件）。
// 调用方必须保证没有并发写入（raft 的压缩流程在 applyMu 下调用）。
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tmp, err := os.CreateTemp("", "sqlraft-snap-*.db")
	if err != nil {
		return nil, fmt.Errorf("create snapshot temp: %w", err)
	}
	tmpPath := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpPath)

	if _, err := s.db.Exec("VACUUM INTO '" + escapeSQLiteString(tmpPath) + "'"); err != nil {
		return nil, fmt.Errorf("vacuum into snapshot: %w", err)
	}
	data, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	return data, nil
}

// ApplySnapshot 用快照字节替换整个状态机（关闭旧连接、原子替换文件、重开）。
// 仅支持文件型数据库（内存库无法替换）。
func (s *Store) ApplySnapshot(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.path == "" {
		return errors.New("snapshot requires a file-backed database")
	}
	tmp := s.path + ".snap.tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write snapshot temp: %w", err)
	}
	if err := s.db.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close old db: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace db: %w", err)
	}
	// 快照来自 VACUUM INTO，不含 WAL/SHM；清理旧残留
	os.Remove(s.path + "-wal")
	os.Remove(s.path + "-shm")

	db, err := sql.Open("sqlite", s.path)
	if err != nil {
		return fmt.Errorf("reopen db: %w", err)
	}
	db.SetMaxOpenConns(1)
	for _, p := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return fmt.Errorf("apply %s: %w", p, err)
		}
	}
	if _, err := db.Exec(metaTable); err != nil {
		db.Close()
		return fmt.Errorf("create meta table: %w", err)
	}
	s.db = db
	return nil
}

func escapeSQLiteString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// ApplyEntry 在单个事务内应用一条日志条目，并原子推进 applied_index 元数据。
// 这样崩溃后恢复时，已应用的日志不会被重复执行。
func (s *Store) ApplyEntry(ctx context.Context, index uint64, entry *logpb.LogEntry) (*ApplyOutcome, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin apply tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // Commit 成功后 Rollback 无副作用

	out := &ApplyOutcome{}
	if exec := entry.GetCommand().GetExec(); exec != nil {
		args := valuesToArgs(exec.Params)
		if hasReturning(exec.Sql) {
			// INSERT/UPDATE/DELETE ... RETURNING：以查询方式执行，
			// 把返回行收集进结果（所有节点计算相同，保持确定性）。
			rows, err := tx.QueryContext(ctx, exec.Sql, args...)
			if err != nil {
				return nil, err
			}
			res := scanRows(rows)
			rows.Close()
			if err := rows.Err(); err != nil {
				return nil, err
			}
			if res.err != nil {
				return nil, res.err
			}
			out.Columns = res.Columns
			out.Rows = res.Rows
			out.RowsAffected = int64(len(res.Rows))
		} else {
			res, err := tx.ExecContext(ctx, exec.Sql, args...)
			if err != nil {
				return nil, err
			}
			if out.RowsAffected, err = res.RowsAffected(); err != nil {
				return nil, err
			}
			if out.LastInsertID, err = res.LastInsertId(); err != nil {
				return nil, err
			}
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO sqlraft_meta (k, v) VALUES ('applied_index', ?)
		 ON CONFLICT(k) DO UPDATE SET v = excluded.v`,
		strconv.FormatUint(index, 10)); err != nil {
		return nil, fmt.Errorf("record applied index: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit apply tx: %w", err)
	}
	return out, nil
}

// AppliedIndex 返回本节点状态机已应用的最高日志索引。
// 恢复时用它作为 applyLoop 的起点，避免重复应用。
func (s *Store) AppliedIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var v string
	err := s.db.QueryRow(
		`SELECT v FROM sqlraft_meta WHERE k = 'applied_index'`).Scan(&v)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read applied index: %w", err)
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse applied index %q: %w", v, err)
	}
	return n, nil
}

// TableInfo 实现 rewrite.Schema：返回表的列结构与自增列。
func (s *Store) TableInfo(table string) (*rewrite.TableInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var createSQL string
	err := s.db.QueryRow(
		`SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?`,
		table).Scan(&createSQL)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query schema of %q: %w", table, err)
	}

	rows, err := s.db.Query(`SELECT name, type, pk FROM pragma_table_info(?)`, table)
	if err != nil {
		return nil, fmt.Errorf("query columns of %q: %w", table, err)
	}
	defer rows.Close()

	info := &rewrite.TableInfo{}
	pkPos := -1
	for rows.Next() {
		var name, typ string
		var pk int
		if err := rows.Scan(&name, &typ, &pk); err != nil {
			return nil, err
		}
		info.Columns = append(info.Columns, name)
		if pk > 0 && strings.Contains(strings.ToUpper(typ), "INTEGER") {
			pkPos = len(info.Columns) - 1
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 仅对显式声明 AUTOINCREMENT 的 INTEGER PRIMARY KEY 做预分配
	if pkPos >= 0 && strings.Contains(strings.ToUpper(createSQL), "AUTOINCREMENT") {
		info.AutoIncrement = info.Columns[pkPos]
	}
	return info, nil
}

// NextAutoIncrement 返回表下一个可用的自增 ID（基于 sqlite_sequence）。
func (s *Store) NextAutoIncrement(table string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var seq sql.NullInt64
	err := s.db.QueryRow(
		`SELECT seq FROM sqlite_sequence WHERE name = ?`, table).Scan(&seq)
	if err == sql.ErrNoRows {
		return 1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read sqlite_sequence for %q: %w", table, err)
	}
	if !seq.Valid {
		return 1, nil
	}
	return seq.Int64 + 1, nil
}

// QueryRows 执行一次查询，返回原始 Go 值与列信息。
func (s *Store) QueryRows(ctx context.Context, sql string, params []*sqlraftpb.Value) (*RawResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, sql, valuesToArgs(params)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanRows(rows).result()
}

// Query 执行一次查询，返回列信息与结果行（proto 表示）。
func (s *Store) Query(ctx context.Context, sql string, params []*sqlraftpb.Value) ([]*sqlraftpb.Column, []*sqlraftpb.Row, error) {
	res, err := s.QueryRows(ctx, sql, params)
	if err != nil {
		return nil, nil, err
	}

	columns := make([]*sqlraftpb.Column, len(res.Columns))
	for i, c := range res.Columns {
		columns[i] = &sqlraftpb.Column{Name: c.Name, Type: c.Type}
	}
	result := make([]*sqlraftpb.Row, 0, len(res.Rows))
	for _, raw := range res.Rows {
		row := &sqlraftpb.Row{}
		for _, v := range raw {
			row.Values = append(row.Values, anyToValue(v))
		}
		result = append(result, row)
	}
	return columns, result, nil
}

// scanResult 是 scanRows 的中间结果。
type scanResult struct {
	Columns []Column
	Rows    [][]any
	err     error
}

func (r *scanResult) result() (*RawResult, error) {
	if r.err != nil {
		return nil, r.err
	}
	return &RawResult{Columns: r.Columns, Rows: r.Rows}, nil
}

// scanRows 读取 rows 的全部数据。
func scanRows(rows *sql.Rows) *scanResult {
	colNames, err := rows.Columns()
	if err != nil {
		return &scanResult{err: err}
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return &scanResult{err: err}
	}
	res := &scanResult{}
	for i := range colNames {
		typ := ""
		if colTypes[i] != nil {
			typ = colTypes[i].DatabaseTypeName()
		}
		res.Columns = append(res.Columns, Column{Name: colNames[i], Type: typ})
	}
	for rows.Next() {
		raw := make([]any, len(colNames))
		ptrs := make([]any, len(colNames))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return &scanResult{err: err}
		}
		res.Rows = append(res.Rows, raw)
	}
	return res
}

// hasReturning 判断语句是否包含 RETURNING 关键字（引号/注释外）。
func hasReturning(sql string) bool {
	inSingle, inDouble, inComment := false, false, false
	depth := 0
	for i := 0; i+9 <= len(sql); i++ {
		c := sql[i]
		switch {
		case inComment:
			if c == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				inComment = false
				i++
			}
			continue
		case inSingle:
			if c == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++
				} else {
					inSingle = false
				}
			}
			continue
		case inDouble:
			if c == '"' {
				if i+1 < len(sql) && sql[i+1] == '"' {
					i++
				} else {
					inDouble = false
				}
			}
			continue
		}
		switch {
		case c == '\'':
			inSingle = true
		case c == '"':
			inDouble = true
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			inComment = true
			i++
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && (sql[i] == 'R' || sql[i] == 'r') &&
			(i == 0 || isWordBoundary(sql[i-1])) &&
			wordAt(sql, i) == "RETURNING" &&
			(i+9 >= len(sql) || isWordBoundary(sql[i+9])):
			return true
		}
	}
	return false
}

func wordAt(s string, i int) string {
	end := i
	for end < len(s) && ((s[end] >= 'a' && s[end] <= 'z') || (s[end] >= 'A' && s[end] <= 'Z') || s[end] == '_') {
		end++
	}
	return strings.ToUpper(s[i:end])
}

func isWordBoundary(b byte) bool {
	return !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_')
}

// valuesToArgs 把 proto Value 转换为 database/sql 参数。
func valuesToArgs(params []*sqlraftpb.Value) []any {
	if len(params) == 0 {
		return nil
	}
	args := make([]any, 0, len(params))
	for _, v := range params {
		args = append(args, valueToAny(v))
	}
	return args
}

func valueToAny(v *sqlraftpb.Value) any {
	if v == nil {
		return nil
	}
	switch k := v.Kind.(type) {
	case *sqlraftpb.Value_S:
		return k.S
	case *sqlraftpb.Value_I:
		return k.I
	case *sqlraftpb.Value_F:
		return k.F
	case *sqlraftpb.Value_B:
		return k.B
	case *sqlraftpb.Value_By:
		return k.By
	default:
		return nil
	}
}

func anyToValue(v any) *sqlraftpb.Value {
	switch t := v.(type) {
	case nil:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_Null{Null: &sqlraftpb.Null{}}}
	case int64:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_I{I: t}}
	case float64:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_F{F: t}}
	case bool:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_B{B: t}}
	case []byte:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_By{By: t}}
	case string:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_S{S: t}}
	case time.Time:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_S{S: t.Format(time.RFC3339Nano)}}
	default:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_S{S: fmt.Sprintf("%v", t)}}
	}
}
