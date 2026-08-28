package pgwire

import "testing"

func TestPrepareCatalogQuery(t *testing.T) {
	sql := `SELECT c.relname, CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' END AS typ
FROM pg_catalog.pg_class c
WHERE c.relname IN ('users','v_active')
  AND c.relkind !~ '^t'
  AND pg_catalog.pg_table_is_visible(c.oid)
  AND c.relname COLLATE pg_catalog.default = 'users'
ORDER BY 1`
	got := prepareCatalogQuery(sql)
	want := `SELECT c.relname, CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' END AS typ
FROM pg_class c
WHERE c.relname IN ('users','v_active')
  AND c.relkind NOT REGEXP '^t'
  AND pg_table_is_visible(c.oid)
  AND c.relname = 'users'
ORDER BY 1`
	if got != want {
		t.Errorf("prepareCatalogQuery mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func TestPrepareCatalogQuerySecondPhase(t *testing.T) {
	sql := `SELECT c.relchecks, c.relkind, c.relhasindex, c.relhasrules, c.relhastriggers,
c.relrowsecurity, c.relforcerowsecurity, false AS relhasoids, c.relispartition,
'', c.reltablespace,
CASE WHEN c.reloftype = 0 THEN '' ELSE c.reloftype::pg_catalog.regtype::pg_catalog.text END,
c.relpersistence, c.relreplident, am.amname
FROM pg_catalog.pg_class c
 LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid)
LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid)
WHERE c.oid = '16386';`
	got := prepareCatalogQuery(sql)
	t.Logf("prepared: %s", got)
	if got != "" {
		// 基本健全性：不应包含 :: 与 pg_catalog. 前缀
		if containsStr(got, "::") {
			t.Errorf("cast not stripped: %s", got)
		}
		if containsStr(got, "pg_catalog.") {
			t.Errorf("schema prefix not stripped: %s", got)
		}
	}
}

func containsStr(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}

func TestStripArrayIndex(t *testing.T) {
	sql := `SELECT a.attname FROM pg_constraint c JOIN pg_attribute a
	ON (a.attrelid = c.conrelid AND a.attnum = c.conkey[1])
	WHERE c.contype = 'n'`
	got := stripArrayIndex(sql)
	if containsStr(got, "conkey[1]") {
		t.Fatalf("array index not stripped: %s", got)
	}
	t.Logf("stripped: %s", got)
}
