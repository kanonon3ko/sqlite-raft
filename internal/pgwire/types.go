// PG 类型 OID 与值编码。
package pgwire

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
)

// 常用 PG 类型 OID。
const (
	oidBool      = 16
	oidInt8      = 20
	oidFloat8    = 701
	oidBytea     = 17
	oidText      = 25
	oidNumeric   = 1700
	oidDate      = 1082
	oidTimestamp = 1114
	oidUUID      = 2950
	oidJSON      = 114
	oidJSONB     = 3802
)

// pgColumnType 根据 SQLite 声明的列类型推导 PG OID 与 typlen。
func pgColumnType(declared string) (oid uint32, typlen int16) {
	t := strings.ToUpper(strings.TrimSpace(declared))
	switch {
	case t == "":
		return oidText, -1
	case strings.Contains(t, "BOOL"):
		return oidBool, 1
	case strings.Contains(t, "INT"):
		return oidInt8, 8
	case strings.Contains(t, "REAL"), strings.Contains(t, "FLOA"), strings.Contains(t, "DOUB"):
		return oidFloat8, 8
	case strings.Contains(t, "BLOB"), strings.Contains(t, "BYTEA"):
		return oidBytea, -1
	case t == "DATE":
		return oidDate, 4
	case strings.Contains(t, "TIME"):
		return oidTimestamp, 8
	case strings.Contains(t, "NUMERIC"), strings.Contains(t, "DEC"):
		return oidNumeric, -1
	case strings.Contains(t, "JSONB"):
		return oidJSONB, -1
	case strings.Contains(t, "JSON"):
		return oidJSON, -1
	case strings.Contains(t, "UUID"):
		return oidUUID, 16
	default:
		return oidText, -1
	}
}

// encodeText 把原始值编码为 PG 文本格式；nil 表示 NULL。
func encodeText(v any, oid uint32) []byte {
	switch t := v.(type) {
	case nil:
		return nil
	case bool:
		if t {
			return []byte("t")
		}
		return []byte("f")
	case int64:
		if oid == oidBool {
			if t != 0 {
				return []byte("t")
			}
			return []byte("f")
		}
		return strconv.AppendInt(nil, t, 10)
	case int:
		return strconv.AppendInt(nil, int64(t), 10)
	case float64:
		if oid == oidBool {
			if t != 0 {
				return []byte("t")
			}
			return []byte("f")
		}
		return strconv.AppendFloat(nil, t, 'g', -1, 64)
	case string:
		return []byte(t)
	case []byte:
		if oid == oidBytea || oid == oidText {
			return []byte(`\x` + hex.EncodeToString(t))
		}
		return []byte(`\x` + hex.EncodeToString(t))
	case time.Time:
		return []byte(t.UTC().Format("2006-01-02 15:04:05.999999"))
	default:
		return []byte(fmt.Sprintf("%v", t))
	}
}

// bindParamToValue 把 Bind 参数转换为 SQLite 绑定值。
// 参数以文本格式（format 0）为主；二进制格式做基础解码兜底。
func bindParamToValue(p paramValue) *sqlraftpb.Value {
	if p.isNull {
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_Null{Null: &sqlraftpb.Null{}}}
	}
	if p.format == 0 {
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_S{S: string(p.value)}}
	}
	return binaryParamToValue(p.value)
}

func binaryParamToValue(b []byte) *sqlraftpb.Value {
	switch len(b) {
	case 1:
		if b[0] == 0 {
			return &sqlraftpb.Value{Kind: &sqlraftpb.Value_B{B: false}}
		}
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_B{B: true}}
	case 2:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_I{I: int64(int16(binary.BigEndian.Uint16(b)))}}
	case 4:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_I{I: int64(int32(binary.BigEndian.Uint32(b)))}}
	case 8:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_I{I: int64(binary.BigEndian.Uint64(b))}}
	default:
		return &sqlraftpb.Value{Kind: &sqlraftpb.Value_S{S: string(b)}}
	}
}
