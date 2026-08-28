// pg_catalog 兼容层：为 psql 的元命令（\dt / \l / \dn / \du / \d 等）
// 提供内存元数据库，并把 PG 特有的语法改写为 SQLite 可执行形式。
package pgwire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
	sqlite "modernc.org/sqlite"

	"github.com/kanonon3ko/sqlite-raft/gen/sqlraftpb"
	"github.com/kanonon3ko/sqlite-raft/internal/store"
)

var (
	registerOnce sync.Once
)

// Catalog 是 pg_catalog 元数据库。
type Catalog struct {
	db   *sql.DB
	main *store.Store
}

// NewCatalog 创建元数据库并注册兼容函数。
func NewCatalog(main *store.Store) (*Catalog, error) {
	// 必须在打开任何连接之前注册函数（modernc 只对新连接生效）
	registerCatalogFunctions()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	c := &Catalog{db: db, main: main}
	if err := c.createSchema(); err != nil {
		db.Close()
		return nil, err
	}
	return c, nil
}

func (c *Catalog) Close() error { return c.db.Close() }

// Query 同步元数据后执行查询。
func (c *Catalog) Query(ctx context.Context, sql string) (*store.RawResult, error) {
	if err := c.sync(ctx); err != nil {
		return nil, err
	}
	rows, err := c.db.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	res := &store.RawResult{}
	for i := range colNames {
		typ := ""
		if colTypes[i] != nil {
			typ = colTypes[i].DatabaseTypeName()
		}
		res.Columns = append(res.Columns, store.Column{Name: colNames[i], Type: typ})
	}
	for rows.Next() {
		raw := make([]any, len(colNames))
		ptrs := make([]any, len(colNames))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		res.Rows = append(res.Rows, raw)
	}
	return res, rows.Err()
}

// createSchema 创建 psql 元命令所需的 pg_catalog 表。
func (c *Catalog) createSchema() error {
	stmts := []string{
		`CREATE TABLE pg_namespace (oid INTEGER PRIMARY KEY, nspname TEXT, nspowner INTEGER)`,
		`CREATE TABLE pg_class (
			oid INTEGER PRIMARY KEY, relname TEXT, relnamespace INTEGER,
			relkind TEXT, relowner INTEGER, relam INTEGER,
			relchecks INTEGER, relhasindex INTEGER, relhasrules INTEGER,
			relhastriggers INTEGER, relrowsecurity INTEGER,
			relforcerowsecurity INTEGER, relispartition INTEGER,
			reltablespace INTEGER, reloftype INTEGER, relpersistence TEXT,
			relreplident TEXT, reltoastrelid INTEGER, reloptions TEXT,
			relpartbound TEXT)`,
		`CREATE TABLE pg_attribute (
			attrelid INTEGER, attname TEXT, atttypid INTEGER, attstattarget INTEGER,
			attlen INTEGER, attnum INTEGER, attndims INTEGER, attcacheoff INTEGER,
			atttypmod INTEGER, attbyval INTEGER, attalign TEXT, attstorage TEXT,
			attcompression TEXT, attnotnull INTEGER, atthasdef INTEGER,
			atthasmissing INTEGER, attidentity TEXT, attgenerated TEXT,
			attisdropped INTEGER, attislocal INTEGER, attinhcount INTEGER,
			attcollation INTEGER, attacl TEXT, attoptions TEXT, attfdwoptions TEXT,
			attmissingval TEXT)`,
		`CREATE TABLE pg_type (
			oid INTEGER PRIMARY KEY, typname TEXT, typnamespace INTEGER,
			typowner INTEGER, typlen INTEGER, typbyval INTEGER, typtype TEXT,
			typcategory TEXT, typispreferred INTEGER, typisdefined INTEGER,
			typdelim TEXT, typrelid INTEGER, typelem INTEGER, typarray INTEGER,
			typinput TEXT, typoutput TEXT, typreceive TEXT, typsend TEXT,
			typmodin TEXT, typmodout TEXT, typanalyze TEXT, typalign TEXT,
			typstorage TEXT, typnotnull INTEGER, typbasetype INTEGER,
			typtypmod INTEGER, typndims INTEGER, typcollation INTEGER,
			typdefaultbin TEXT, typdefault TEXT, typacl TEXT)`,
		`CREATE TABLE pg_database (
			oid INTEGER PRIMARY KEY, datname TEXT, datdba INTEGER, encoding INTEGER,
			datlocprovider TEXT, datcollate TEXT, datctype TEXT,
			daticulocale TEXT, daticurules TEXT, datistemplate INTEGER,
			datallowconn INTEGER, datconnlimit INTEGER, datfrozenxid INTEGER,
			datminmxid INTEGER, dattablespace INTEGER, datacl TEXT)`,
		`CREATE TABLE pg_roles (
			oid INTEGER PRIMARY KEY, rolname TEXT, rolsuper INTEGER,
			rolinherit INTEGER, rolcreaterole INTEGER, rolcreatedb INTEGER,
			rolcanlogin INTEGER, rolconnlimit INTEGER, rolvaliduntil TEXT,
			rolreplication INTEGER, rolbypassrls INTEGER)`,
		`CREATE TABLE pg_index (
			indexrelid INTEGER, indrelid INTEGER, indnatts INTEGER,
			indnkeyatts INTEGER, indisunique INTEGER, indisprimary INTEGER,
			indisexclusion INTEGER, indimmediate INTEGER, indisclustered INTEGER,
			indisvalid INTEGER, indisready INTEGER, indislive INTEGER,
			indisreplident INTEGER, indkey TEXT, indcollation TEXT,
			indclass TEXT, indoption TEXT, indexprs TEXT, indpred TEXT)`,
		`CREATE TABLE pg_constraint (
			oid INTEGER PRIMARY KEY, conname TEXT, connamespace INTEGER,
			contype TEXT, condeferrable INTEGER, condeferred INTEGER,
			convalidated INTEGER, conrelid INTEGER, contypid INTEGER,
			conindid INTEGER, conparentid INTEGER, confrelid INTEGER,
			confupdtype TEXT, confdeltype TEXT, confmatchtype TEXT,
			conislocal INTEGER, coninhcount INTEGER, connoinherit INTEGER,
			conkey TEXT, confkey TEXT, conpfeqop TEXT, conppeqop TEXT,
			conffeqop TEXT, conexclop TEXT, conbin TEXT)`,
		`CREATE TABLE pg_description (objoid INTEGER, classoid INTEGER,
			objsubid INTEGER, description TEXT)`,
		`CREATE TABLE pg_proc (
			oid INTEGER PRIMARY KEY, proname TEXT, pronamespace INTEGER,
			proowner INTEGER, prolang INTEGER, procost REAL, prorows REAL,
			provariadic INTEGER, protransform TEXT, prokind TEXT,
			prosecdef INTEGER, proleakproof INTEGER, proisstrict INTEGER,
			proretset INTEGER, provolatile TEXT, proparallel TEXT,
			pronargs INTEGER, pronargdefaults INTEGER, prorettype INTEGER,
			proargtypes TEXT, proallargtypes TEXT, proargmodes TEXT,
			proargnames TEXT, proargdefaults TEXT, protrftypes TEXT,
			prosrc TEXT, probin TEXT, prosqlbody TEXT, proconfig TEXT,
			proacl TEXT)`,
		`CREATE TABLE pg_statistic_ext (
			oid INTEGER PRIMARY KEY, stxrelid INTEGER, stxname TEXT,
			stxnamespace INTEGER, stxowner INTEGER, stxstattarget INTEGER,
			stxkeys TEXT, stxkind TEXT, stxexprs TEXT)`,
		`CREATE TABLE pg_statistic_ext_data (
			stxoid INTEGER, stxdndistinct TEXT, stxddependencies TEXT,
			stxdexpr TEXT)`,
		`CREATE TABLE pg_partitioned_table (
			partrelid INTEGER, partstrat TEXT, partnatts INTEGER,
			partdefid INTEGER, partattrs TEXT, partclass TEXT,
			partcollation TEXT, partexprs TEXT)`,
		`CREATE TABLE pg_enum (oid INTEGER PRIMARY KEY, enumtypid INTEGER,
			enumsortorder REAL, enumlabel TEXT)`,
		`CREATE TABLE pg_range (rngtypid INTEGER, rngsubtype INTEGER,
			rngmultitypid INTEGER, rngcollation INTEGER, rngsubopc INTEGER,
			rngcanonical TEXT, rngsubdiff TEXT)`,
		`CREATE TABLE pg_operator (oid INTEGER PRIMARY KEY, oprname TEXT,
			oprnamespace INTEGER, oprowner INTEGER, oprkind TEXT,
			oprcanmerge INTEGER, oprcanhash INTEGER, oprleft INTEGER,
			oprright INTEGER, oprresult INTEGER, oprcom INTEGER, oprnegate INTEGER,
			oprcode TEXT, oprrest TEXT, oprjoin TEXT)`,
		`CREATE TABLE pg_opclass (oid INTEGER PRIMARY KEY, opcmethod INTEGER,
			opcname TEXT, opcnamespace INTEGER, opcowner INTEGER, opcfamily INTEGER,
			opcintype INTEGER, opcdefault INTEGER, opckeytype INTEGER)`,
		`CREATE TABLE pg_amop (oid INTEGER PRIMARY KEY, amopfamily INTEGER,
			amoplefttype INTEGER, amoprighttype INTEGER, amopstrategy INTEGER,
			amoppurpose TEXT, amoppopr INTEGER, amopmethod INTEGER,
			amopsortfamily INTEGER)`,
		`CREATE TABLE pg_amproc (oid INTEGER PRIMARY KEY, amprocfamily INTEGER,
			amproclefttype INTEGER, amprocrighttype INTEGER, amprocnum INTEGER,
			amproc TEXT)`,
		`CREATE TABLE pg_cast (oid INTEGER PRIMARY KEY, castsource INTEGER,
			casttarget INTEGER, castfunc INTEGER, castcontext TEXT,
			castmethod TEXT)`,
		`CREATE TABLE pg_conversion (oid INTEGER PRIMARY KEY, conname TEXT,
			connamespace INTEGER, conowner INTEGER, conforencoding INTEGER,
			contoencoding INTEGER, conproc TEXT, condefault INTEGER)`,
		`CREATE TABLE pg_depend (classid INTEGER, objid INTEGER, objsubid INTEGER,
			refclassid INTEGER, refobjid INTEGER, refobjsubid INTEGER,
			deptype TEXT)`,
		`CREATE TABLE pg_shdepend (dbid INTEGER, classid INTEGER, objid INTEGER,
			objsubid INTEGER, refclassid INTEGER, refobjid INTEGER,
			refobjsubid INTEGER, deptype TEXT)`,
		`CREATE TABLE pg_foreign_data_wrapper (oid INTEGER PRIMARY KEY,
			fdwname TEXT, fdwowner INTEGER, fdwhandler TEXT, fdwvalidator TEXT,
			fdwacl TEXT, fdwoptions TEXT)`,
		`CREATE TABLE pg_foreign_server (oid INTEGER PRIMARY KEY, srvname TEXT,
			srvowner INTEGER, srvfdw INTEGER, srvtype TEXT, srvversion TEXT,
			srvacl TEXT, srvoptions TEXT)`,
		`CREATE TABLE pg_foreign_table (ftrelid INTEGER, ftserver INTEGER,
			ftoptions TEXT)`,
		`CREATE TABLE pg_user_mapping (oid INTEGER PRIMARY KEY, umuser INTEGER,
			umserver INTEGER, umoptions TEXT)`,
		`CREATE TABLE pg_ts_config (oid INTEGER PRIMARY KEY, cfgname TEXT,
			cfgnamespace INTEGER, cfgowner INTEGER, cfgparser INTEGER)`,
		`CREATE TABLE pg_ts_dict (oid INTEGER PRIMARY KEY, dictname TEXT,
			dictnamespace INTEGER, dictowner INTEGER, dicttemplate INTEGER,
			dictinitoption TEXT)`,
		`CREATE TABLE pg_ts_parser (oid INTEGER PRIMARY KEY, prsname TEXT,
			prsnamespace INTEGER, prsstart TEXT, prstoken TEXT, prsend TEXT,
			prsheadline TEXT, prslextype TEXT)`,
		`CREATE TABLE pg_ts_template (oid INTEGER PRIMARY KEY, tmplname TEXT,
			tmplnamespace INTEGER, tmplinit TEXT, tmpllexize TEXT)`,
		`CREATE TABLE pg_event_trigger (oid INTEGER PRIMARY KEY, evtname TEXT,
			evtevent TEXT, evtowner INTEGER, evtfoid INTEGER, evtenabled TEXT,
			evttags TEXT)`,
		`CREATE TABLE pg_largeobject (loid INTEGER, pageno INTEGER, data TEXT)`,
		`CREATE TABLE pg_largeobject_metadata (oid INTEGER PRIMARY KEY,
			lomowner INTEGER, lomacl TEXT)`,
		`CREATE TABLE pg_replication_origin (roident INTEGER, roname TEXT)`,
		`CREATE TABLE pg_seclabel (objoid INTEGER, classoid INTEGER,
			objsubid INTEGER, provider TEXT, label TEXT)`,
		`CREATE TABLE pg_shseclabel (objoid INTEGER, classoid INTEGER,
			objsubid INTEGER, provider TEXT, label TEXT)`,
		`CREATE TABLE pg_shdescription (objoid INTEGER, classoid INTEGER,
			objsubid INTEGER, description TEXT)`,
		`CREATE TABLE pg_subscription (oid INTEGER PRIMARY KEY, subdbid INTEGER,
			subname TEXT, subowner INTEGER, subenabled INTEGER, subbinary INTEGER,
			substream INTEGER, subtwophasestate TEXT, subdisableonerr INTEGER,
			subpasswordrequired INTEGER, subrunasowner INTEGER, subconninfo TEXT,
			subslotname TEXT, subsynccommit TEXT, subpublications TEXT)`,
		`CREATE TABLE pg_subscription_rel (srsubid INTEGER, srrelid INTEGER,
			srsubstate TEXT, srsublsn TEXT)`,
		`CREATE TABLE pg_publication (oid INTEGER PRIMARY KEY, pubname TEXT,
			pubowner INTEGER, puballtables INTEGER, pubinsert INTEGER,
			pubupdate INTEGER, pubdelete INTEGER, pubtruncate INTEGER,
			pubviaroot INTEGER)`,
		`CREATE TABLE pg_publication_rel (
			oid INTEGER PRIMARY KEY, prpubid INTEGER, prrelid INTEGER,
			prqual TEXT, prattrs TEXT)`,
		`CREATE TABLE pg_publication_namespace (
			oid INTEGER PRIMARY KEY, pnpubid INTEGER, pnnspid INTEGER)`,
		`CREATE TABLE pg_stat_activity (
			datid INTEGER, datname TEXT, pid INTEGER, leader_pid INTEGER,
			usesysid INTEGER, usename TEXT, application_name TEXT,
			client_addr TEXT, client_hostname TEXT, client_port INTEGER,
			backend_start TEXT, xact_start TEXT, query_start TEXT,
			state_change TEXT, wait_event_type TEXT, wait_event TEXT,
			state TEXT, backend_xid TEXT, backend_xmin TEXT, query_id INTEGER,
			query TEXT, backend_type TEXT)`,
		`CREATE TABLE pg_stat_database (
			datid INTEGER, datname TEXT, numbackends INTEGER,
			xact_commit INTEGER, xact_rollback INTEGER, blks_read INTEGER,
			blks_hit INTEGER, tup_returned INTEGER, tup_fetched INTEGER,
			tup_inserted INTEGER, tup_updated INTEGER, tup_deleted INTEGER,
			conflicts INTEGER, temp_files INTEGER, temp_bytes INTEGER,
			deadlocks INTEGER, checksum_failures INTEGER,
			checksum_last_failure TEXT, blk_read_time REAL, blk_write_time REAL,
			session_time REAL, active_time REAL, idle_in_transaction_time REAL,
			sessions INTEGER)`,
		`CREATE TABLE pg_stat_all_tables (
			relid INTEGER, schemaname TEXT, relname TEXT, seq_scan INTEGER,
			seq_tup_read INTEGER, idx_scan INTEGER, idx_tup_fetch INTEGER,
			n_tup_ins INTEGER, n_tup_upd INTEGER, n_tup_del INTEGER,
			n_tup_hot_upd INTEGER, n_live_tup INTEGER, n_dead_tup INTEGER,
			n_mod_since_analyze INTEGER, n_ins_since_vacuum INTEGER,
			last_vacuum TEXT, last_autovacuum TEXT, last_analyze TEXT,
			last_autoanalyze TEXT, vacuum_count INTEGER,
			autovacuum_count INTEGER, analyze_count INTEGER,
			autoanalyze_count INTEGER)`,
		`CREATE TABLE pg_stat_all_indexes (
			relid INTEGER, indexrelid INTEGER, schemaname TEXT, relname TEXT,
			indexrelname TEXT, idx_scan INTEGER, idx_tup_read INTEGER,
			idx_tup_fetch INTEGER)`,
		`CREATE TABLE pg_statio_all_tables (
			relid INTEGER, schemaname TEXT, relname TEXT, heap_blks_read INTEGER,
			heap_blks_hit INTEGER, idx_blks_read INTEGER, idx_blks_hit INTEGER,
			toast_blks_read INTEGER, toast_blks_hit INTEGER,
			tidx_blks_read INTEGER, tidx_blks_hit INTEGER)`,
		`CREATE TABLE pg_stat_replication (
			pid INTEGER, usesysid INTEGER, usename TEXT, application_name TEXT,
			client_addr TEXT, client_hostname TEXT, client_port INTEGER,
			backend_start TEXT, backend_xmin TEXT, state TEXT, sent_lsn TEXT,
			write_lsn TEXT, flush_lsn TEXT, replay_lsn TEXT, write_lag TEXT,
			flush_lag TEXT, replay_lag TEXT, sync_priority INTEGER,
			sync_state TEXT, reply_time TEXT)`,
		`CREATE TABLE pg_stat_subscription (
			subid INTEGER, subname TEXT, pid INTEGER, relid INTEGER,
			received_lsn TEXT, last_msg_send_time TEXT, last_msg_receipt_time TEXT,
			latest_end_lsn TEXT, latest_end_time TEXT)`,
		`CREATE TABLE pg_stat_wal_receiver (
			pid INTEGER, status TEXT, receive_start_lsn TEXT,
			receive_start_tli INTEGER, written_lsn TEXT, flushed_lsn TEXT,
			received_tli INTEGER, last_msg_send_time TEXT,
			last_msg_receipt_time TEXT, latest_end_lsn TEXT, latest_end_time TEXT,
			slot_name TEXT, sender_host TEXT, sender_port INTEGER, conninfo TEXT)`,
		`CREATE TABLE pg_stat_ssl (pid INTEGER, ssl INTEGER, version TEXT,
			cipher TEXT, bits INTEGER, compression INTEGER, client_dn TEXT,
			client_serial INTEGER, issuer_dn TEXT)`,
		`CREATE TABLE pg_replication_slots (
			slot_name TEXT, plugin TEXT, slot_type TEXT, datoid INTEGER,
			database TEXT, temporary INTEGER, active INTEGER, active_pid INTEGER,
			xmin TEXT, catalog_xmin TEXT, restart_lsn TEXT, confirmed_flush_lsn TEXT,
			wal_status TEXT, safe_wal_size INTEGER, two_phase INTEGER)`,
		`CREATE TABLE pg_locks (
			locktype TEXT, database INTEGER, relation INTEGER, page INTEGER,
			tuple INTEGER, virtualxid TEXT, transactionid TEXT, classid INTEGER,
			objid INTEGER, objsubid INTEGER, virtualtransaction TEXT, pid INTEGER,
			mode TEXT, granted INTEGER, fastpath INTEGER, waitstart TEXT)`,
		`CREATE TABLE pg_prepared_statements (
			name TEXT, statement TEXT, prepare_time TEXT, parameter_types TEXT,
			result_types TEXT, from_sql INTEGER, generic_plans INTEGER,
			custom_plans INTEGER)`,
		`CREATE TABLE pg_tables (schemaname TEXT, tablename TEXT,
			tableowner TEXT, tablespace TEXT, hasindexes INTEGER,
			hasrules INTEGER, hastriggers INTEGER, rowsecurity INTEGER)`,
		`CREATE TABLE pg_views (schemaname TEXT, viewname TEXT, viewowner TEXT,
			definition TEXT)`,
		`CREATE TABLE pg_indexes (schemaname TEXT, tablename TEXT,
			indexname TEXT, tablespace TEXT, indexdef TEXT)`,
		`CREATE TABLE pg_sequences (schemaname TEXT, sequencename TEXT,
			sequenceowner TEXT, data_type TEXT, start_value INTEGER,
			min_value INTEGER, max_value INTEGER, increment_by INTEGER,
			cycle INTEGER, cache_size INTEGER, last_value INTEGER)`,
		`CREATE TABLE pg_matviews (schemaname TEXT, matviewname TEXT,
			matviewowner TEXT, tablespace TEXT, hasindexes INTEGER,
			ispopulated INTEGER, definition TEXT)`,
		`CREATE TABLE pg_user (usename TEXT, usesysid INTEGER, usecreatedb INTEGER,
			usesuper INTEGER, userepl INTEGER, usebypassrls INTEGER, passwd TEXT,
			valuntil TEXT, useconfig TEXT)`,
		`CREATE TABLE pg_shadow (usename TEXT, usesysid INTEGER, usecreatedb INTEGER,
			usesuper INTEGER, userepl INTEGER, usebypassrls INTEGER, passwd TEXT,
			valuntil TEXT, useconfig TEXT)`,
		`CREATE TABLE pg_group (groname TEXT, grosysid INTEGER, grolist TEXT)`,
		`CREATE TABLE pg_available_extensions (name TEXT, default_version TEXT,
			installed_version TEXT, comment TEXT)`,
		`CREATE TABLE pg_available_extension_versions (name TEXT, version TEXT,
			installed INTEGER, superuser INTEGER, trusted INTEGER, relocatable INTEGER,
			schema TEXT, requires TEXT, comment TEXT)`,
		`CREATE TABLE pg_timezone_names (name TEXT, abbrev TEXT, utc_offset TEXT,
			is_dst INTEGER)`,
		`CREATE TABLE pg_timezone_abbrevs (abbrev TEXT, utc_offset TEXT,
			is_dst INTEGER)`,
		`CREATE TABLE pg_file_settings (name TEXT, setting TEXT, unit TEXT,
			applied INTEGER, error TEXT)`,
		`CREATE TABLE pg_hba_file_rules (line_number INTEGER, type TEXT,
			database TEXT, user_name TEXT, address TEXT, netmask TEXT, auth_method TEXT,
			options TEXT, error TEXT)`,
		`CREATE TABLE pg_ident_file_mappings (line_number INTEGER, map_name TEXT,
			sys_name TEXT, pg_username TEXT, error TEXT)`,
		`CREATE TABLE pg_config (name TEXT, setting TEXT)`,
		`CREATE TABLE pg_shmem_allocations (name TEXT, off INTEGER, size INTEGER,
			allocated_size INTEGER)`,
		`CREATE TABLE pg_backend_memory_contexts (name TEXT, ident TEXT,
			parent TEXT, level INTEGER, total_bytes INTEGER, total_nblocks INTEGER,
			free_bytes INTEGER, free_chunks INTEGER, used_bytes INTEGER)`,
		`CREATE TABLE pg_stat_gssapi (pid INTEGER, gss_authenticated INTEGER,
			principal TEXT, encrypted INTEGER, credentials_delegated INTEGER)`,
		`CREATE TABLE pg_stat_progress_vacuum (pid INTEGER, datid INTEGER,
			datname TEXT, relid INTEGER, phase TEXT, heap_blks_total INTEGER,
			heap_blks_scanned INTEGER, heap_blks_vacuumed INTEGER,
			index_vacuum_count INTEGER, max_dead_tuples INTEGER, num_dead_tuples INTEGER)`,
		`CREATE TABLE pg_stat_progress_create_index (pid INTEGER, datid INTEGER,
			datname TEXT, relid INTEGER, index_relid INTEGER, command TEXT,
			phase TEXT, lockers_total INTEGER, lockers_done INTEGER,
			current_locker_pid INTEGER, blocks_total INTEGER, blocks_done INTEGER,
			tuples_total INTEGER, tuples_done INTEGER, partitions_total INTEGER,
			partitions_done INTEGER)`,
		`CREATE TABLE pg_stat_progress_copy (pid INTEGER, datid INTEGER,
			datname TEXT, relid INTEGER, command TEXT, type TEXT, bytes_processed INTEGER,
			bytes_total INTEGER, tuples_processed INTEGER, tuples_excluded INTEGER)`,
		`CREATE TABLE pg_stat_progress_analyze (pid INTEGER, datid INTEGER,
			datname TEXT, relid INTEGER, phase TEXT, sample_blks_total INTEGER,
			sample_blks_scanned INTEGER, ext_stats_total INTEGER,
			ext_stats_computed INTEGER, child_tables_total INTEGER,
			child_tables_done INTEGER, current_child_table_relid INTEGER)`,
		`CREATE TABLE pg_stat_progress_cluster (pid INTEGER, datid INTEGER,
			datname TEXT, relid INTEGER, command TEXT, phase TEXT,
			cluster_index_relid INTEGER, heap_tuples_scanned INTEGER,
			heap_tuples_written INTEGER, heap_blks_total INTEGER,
			heap_blks_scanned INTEGER, index_rebuild_count INTEGER)`,
		`CREATE TABLE pg_stat_progress_basebackup (pid INTEGER, phase TEXT,
			backup_total INTEGER, backup_streamed INTEGER, tablespaces_total INTEGER,
			tablespaces_streamed INTEGER)`,
		`CREATE TABLE pg_authid (oid INTEGER PRIMARY KEY, rolname TEXT,
			rolsuper INTEGER, rolinherit INTEGER, rolcreaterole INTEGER,
			rolcreatedb INTEGER, rolcanlogin INTEGER, rolreplication INTEGER,
			rolconnlimit INTEGER, rolpassword TEXT, rolvaliduntil TEXT,
			rolbypassrls INTEGER, rolconfig TEXT)`,
		`CREATE TABLE pg_auth_members (roleid INTEGER, grantor INTEGER,
			member INTEGER, admin_option INTEGER, inherit_option INTEGER,
			set_option INTEGER)`,
		`CREATE TABLE pg_aggregate (aggfnoid INTEGER, aggkind TEXT,
			aggnumdirectargs INTEGER, aggtransfn TEXT, aggfinalfn TEXT,
			aggcombinefn TEXT, aggserialfn TEXT, aggdeserialfn TEXT,
			aggmtransfn TEXT, aggminvtransfn TEXT, aggmtranstype INTEGER,
			aggtransspace INTEGER, aggfinalextra INTEGER,
			aggmfinalextra INTEGER, aggfinalmodify TEXT, aggmfinalmodify TEXT,
			aggsortop INTEGER, aggtranstype INTEGER, agginitval TEXT,
			aggminitval TEXT, aggorderby INTEGER, aggargs TEXT)`,
		`CREATE TABLE pg_am (oid INTEGER PRIMARY KEY, amname TEXT)`,
		`CREATE TABLE pg_sequence (seqrelid INTEGER, seqtypid INTEGER,
			seqstart INTEGER, seqincrement INTEGER, seqmax INTEGER,
			seqmin INTEGER, seqcache INTEGER, seqcycle INTEGER)`,
		`CREATE TABLE pg_attrdef (oid INTEGER PRIMARY KEY, adrelid INTEGER,
			adnum INTEGER, adbin TEXT, adsrc TEXT)`,
		`CREATE TABLE pg_collation (oid INTEGER PRIMARY KEY, collname TEXT,
			collnamespace INTEGER, collowner INTEGER, collprovider TEXT,
			collisdeterministic INTEGER, collencoding INTEGER, collcollate TEXT,
			collctype TEXT, colliculocale TEXT, collicurules TEXT,
			colllocale TEXT, collversion TEXT)`,
		`CREATE TABLE pg_settings (name TEXT, setting TEXT, unit TEXT,
			category TEXT, short_desc TEXT, extra_desc TEXT, context TEXT,
			vartype TEXT, source TEXT, min_val TEXT, max_val TEXT,
			enumvals TEXT, boot_val TEXT, reset_val TEXT, sourcefile TEXT,
			sourceline INTEGER, pending_restart INTEGER)`,
		`CREATE TABLE pg_policy (oid INTEGER PRIMARY KEY, polname TEXT,
			polrelid INTEGER, polcmd TEXT, polpermissive INTEGER, polroles TEXT,
			polqual TEXT, polwithcheck TEXT)`,
		`CREATE TABLE pg_inherits (inhrelid INTEGER, inhparent INTEGER,
			inhseqno INTEGER, inhdetachpending INTEGER)`,
		`CREATE TABLE pg_rewrite (oid INTEGER PRIMARY KEY, rulename TEXT,
			ev_class INTEGER, ev_type TEXT, ev_enabled TEXT, is_instead INTEGER,
			ev_qual TEXT, ev_action TEXT)`,
		`CREATE TABLE pg_trigger (oid INTEGER PRIMARY KEY, tgrelid INTEGER,
			tgname TEXT, tgfoid INTEGER, tgtype INTEGER, tgenabled TEXT,
			tgisinternal INTEGER, tgconstrrelid INTEGER, tgconstrindid INTEGER,
			tgconstraint INTEGER, tgdeferrable INTEGER, tginitdeferred INTEGER,
			tgnargs INTEGER, tgattr TEXT, tgargs TEXT, tgqual TEXT,
			tgoldtable TEXT, tgnewtable TEXT)`,
		`CREATE TABLE pg_tablespace (oid INTEGER PRIMARY KEY, spcname TEXT,
			spcowner INTEGER, spcacl TEXT, spcoptions TEXT)`,
		`CREATE TABLE pg_stat_user_tables (relid INTEGER, schemaname TEXT,
			relname TEXT, seq_scan INTEGER, seq_tup_read INTEGER,
			idx_scan INTEGER, idx_tup_fetch INTEGER, n_tup_ins INTEGER,
			n_tup_upd INTEGER, n_tup_del INTEGER, n_tup_hot_upd INTEGER,
			n_live_tup INTEGER, n_dead_tup INTEGER, n_mod_since_analyze INTEGER,
			last_vacuum TEXT, last_autovacuum TEXT, last_analyze TEXT,
			last_autoanalyze TEXT, vacuum_count INTEGER, autovacuum_count INTEGER,
			analyze_count INTEGER, autoanalyze_count INTEGER)`,
	}
	for _, s := range stmts {
		if _, err := c.db.Exec(s); err != nil {
			return fmt.Errorf("create catalog table: %w", err)
		}
	}
	base := []string{
		`INSERT INTO pg_namespace VALUES (11, 'pg_catalog', 10),
			(99, 'pg_toast', 10), (2200, 'public', 10), (13000, 'information_schema', 10)`,
		`INSERT INTO pg_type (oid, typname, typnamespace, typowner, typlen, typbyval,
			typtype, typcategory, typispreferred, typisdefined, typdelim, typrelid,
			typelem, typarray) VALUES
			(16, 'bool', 11, 10, 1, 1, 'b', 'B', 0, 1, ',', 0, 0, 0),
			(20, 'int8', 11, 10, 8, 1, 'b', 'N', 0, 1, ',', 0, 0, 0),
			(21, 'int2', 11, 10, 2, 1, 'b', 'N', 0, 1, ',', 0, 0, 0),
			(23, 'int4', 11, 10, 4, 1, 'b', 'N', 0, 1, ',', 0, 0, 0),
			(25, 'text', 11, 10, -1, 0, 'b', 'S', 1, 1, ',', 0, 0, 0),
			(701, 'float8', 11, 10, 8, 1, 'b', 'N', 0, 1, ',', 0, 0, 0),
			(1043, 'varchar', 11, 10, -1, 0, 'b', 'S', 0, 1, ',', 0, 0, 0)`,
		`INSERT INTO pg_database (oid, datname, datdba, encoding, datlocprovider,
			datcollate, datctype, daticulocale, daticurules, datistemplate,
			datallowconn, datconnlimit, datfrozenxid, datminmxid, dattablespace)
			VALUES (1, 'sqlraft', 10, 6, 'c', 'C', 'C', '', '', 0, 1, -1, 0, 0, 1663)`,
		`INSERT INTO pg_roles (oid, rolname, rolsuper, rolinherit, rolcreaterole,
			rolcreatedb, rolcanlogin, rolconnlimit, rolvaliduntil, rolreplication,
			rolbypassrls) VALUES (10, 'sqlraft', 't', 't', 't', 't', 't', -1, NULL, 'f', 'f')`,
		`INSERT INTO pg_collation (oid, collname, collnamespace, collowner,
			collprovider, collisdeterministic, collencoding, collcollate,
			collctype, colliculocale, collicurules, colllocale, collversion)
			VALUES (100, 'default', 11, 10, 'c', 1, -1, 'C', 'C', '', '', '', NULL)`,
	}
	for _, s := range base {
		if _, err := c.db.Exec(s); err != nil {
			return fmt.Errorf("seed catalog: %w", err)
		}
	}
	return nil
}

// sync 从主状态机同步对象与列信息到元数据库。
func (c *Catalog) sync(ctx context.Context) error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, t := range []string{"pg_class", "pg_attribute", "pg_index", "pg_constraint",
		"pg_sequence", "pg_attrdef"} {
		if _, err := tx.Exec("DELETE FROM " + t); err != nil {
			return err
		}
	}

	objs, err := c.main.QueryRows(ctx,
		"SELECT name, type, tbl_name FROM sqlite_master WHERE type IN ('table','view','index') ORDER BY name", nil)
	if err != nil {
		return err
	}
	oid := int64(16384)
	for _, r := range objs.Rows {
		name := asString(r[0])
		kind := asString(r[1])
		if name == "sqlraft_meta" || strings.HasPrefix(name, "sqlite_") {
			continue // 内部元表不对外暴露
		}
		relkind := "r"
		switch kind {
		case "view":
			relkind = "v"
		case "index":
			relkind = "i"
		}
		if _, err := tx.Exec(
			`INSERT INTO pg_class (oid, relname, relnamespace, relkind, relowner,
				relam, relchecks, relhasindex, relhasrules, relhastriggers,
				relrowsecurity, relforcerowsecurity, relispartition,
				reltablespace, reloftype, relpersistence, relreplident,
				reltoastrelid, reloptions, relpartbound) VALUES (?, ?, 2200, ?, 10, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 'p', 'd', 0, NULL, NULL)`,
			oid, name, relkind); err != nil {
			return err
		}
		if kind == "table" && !strings.HasPrefix(name, "sqlite_") {
			if err := c.syncColumns(ctx, tx, oid, name); err != nil {
				return err
			}
		}
		// 索引：填充 pg_index（关联所属表，供 \di 显示）
		if kind == "index" {
			if err := c.syncIndex(ctx, tx, oid, name, asString(r[2])); err != nil {
				return err
			}
		}
		oid++
	}
	return tx.Commit()
}

// syncIndex 为索引填充 pg_index 行。
func (c *Catalog) syncIndex(ctx context.Context, tx *sql.Tx, indexOID int64, indexName, tblName string) error {
	var tableOID int64
	err := tx.QueryRow("SELECT oid FROM pg_class WHERE relname = ?", tblName).Scan(&tableOID)
	if err != nil {
		return nil // 所属表不可见（如 sqlite_ 内部表），忽略
	}
	_, err = tx.Exec(
		`INSERT INTO pg_index (indexrelid, indrelid, indnatts, indnkeyatts,
			indisunique, indisprimary, indisexclusion, indimmediate,
			indisclustered, indisvalid, indisready, indislive,
			indisreplident, indkey, indcollation, indclass, indoption) VALUES
			(?, ?, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, '1', '0', '0', '0')`,
		indexOID, tableOID)
	return err
}

// syncColumns 用 PRAGMA table_info 填充 pg_attribute。
func (c *Catalog) syncColumns(ctx context.Context, tx *sql.Tx, relOID int64, table string) error {
	cols, err := c.main.QueryRows(ctx,
		"SELECT cid, name, type, [notnull], dflt_value, pk FROM pragma_table_info(?)",
		[]*sqlraftpb.Value{{Kind: &sqlraftpb.Value_S{S: table}}})
	if err != nil {
		return err
	}
	for _, r := range cols.Rows {
		attnum := asInt64(r[0])
		name := asString(r[1])
		typ := asString(r[2])
		notnull := asInt64(r[3])
		dflt := r[4]
		pk := asInt64(r[5])
		oid := pgTypeOIDForName(typ)
		attnotnull := int64(0)
		if notnull != 0 || pk > 0 {
			attnotnull = 1
		}
		hasdef := int64(0)
		if dflt != nil {
			hasdef = 1
		}
		identity := ""
		if pk > 0 {
			identity = "a"
		}
		if _, err := tx.Exec(
			`INSERT INTO pg_attribute (attrelid, attname, atttypid, attstattarget,
				attlen, attnum, attndims, attcacheoff, atttypmod, attbyval,
				attalign, attstorage, attcompression, attnotnull, atthasdef,
				atthasmissing, attidentity, attgenerated, attisdropped,
				attislocal, attinhcount, attcollation) VALUES
				(?, ?, ?, 0, ?, ?, 0, -1, -1, 0, 'i', 'p', '', ?, ?, 0, ?, '', 0, 1, 0, 0)`,
			relOID, name, oid, pgTypeLen(oid), attnum+1, attnotnull, hasdef, identity); err != nil {
			return err
		}
		if dflt != nil {
			if _, err := tx.Exec(
				"INSERT INTO pg_attrdef (oid, adrelid, adnum, adbin, adsrc) VALUES (?, ?, ?, NULL, ?)",
				relOID*100+attnum+20000, relOID, attnum+1, toString2(dflt)); err != nil {
				return err
			}
		}
	}
	return nil
}

func toString2(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

// registerCatalogFunctions 注册 pg_catalog 兼容函数（进程级 SQLite 函数）。
func registerCatalogFunctions() {
	registerOnce.Do(func() {
		reg := func(name string, n int32, fn func(args []driver.Value) (driver.Value, error)) {
			sqlite.MustRegisterScalarFunction(name, n,
				func(ctx *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
					return fn(args)
				})
		}
		reg("pg_get_userbyid", 1, func(args []driver.Value) (driver.Value, error) {
			return "sqlraft", nil
		})
		reg("pg_table_is_visible", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(1), nil
		})
		reg("pg_encoding_to_char", 1, func(args []driver.Value) (driver.Value, error) {
			return "UTF8", nil
		})
		reg("array_length", -1, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("array_to_string", -1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("REGEXP", 2, func(args []driver.Value) (driver.Value, error) {
			if len(args) < 2 {
				return int64(0), nil
			}
			pattern := toString(args[0])
			value := toString(args[1])
			ok, err := regexp.MatchString(pattern, value)
			if err != nil {
				return int64(0), nil
			}
			if ok {
				return int64(1), nil
			}
			return int64(0), nil
		})
		reg("pg_get_expr", -1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_indexdef", -1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_constraintdef", -1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_serial_sequence", 2, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("format_type", 2, func(args []driver.Value) (driver.Value, error) {
			oid := asInt64(args[0])
			switch oid {
			case 16:
				return "boolean", nil
			case 20:
				return "bigint", nil
			case 21:
				return "smallint", nil
			case 23:
				return "integer", nil
			case 25:
				return "text", nil
			case 17:
				return "bytea", nil
			case 701:
				return "double precision", nil
			case 1043:
				return "character varying", nil
			case 1700:
				return "numeric", nil
			case 1114:
				return "timestamp without time zone", nil
			default:
				return "text", nil
			}
		})
		reg("pg_size_pretty", 1, func(args []driver.Value) (driver.Value, error) {
			return "0 bytes", nil
		})
		reg("pg_relation_size", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_partition_ancestors", 1, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("col_description", 2, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("obj_description", 2, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("shobj_description", 2, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("pg_total_relation_size", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_get_viewdef", 1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_ruledef", 1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_triggerdef", 1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_partkeydef", 1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_statisticsobjdef", 1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_statisticsobjdef_columns", 1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_replication_slots", 0, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("pg_stat_get_numscans", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_tuples_returned", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_tuples_fetched", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_tuples_inserted", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_tuples_updated", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_tuples_deleted", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_live_tuples", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_dead_tuples", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_mod_since_analyze", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_ins_since_vacuum", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_last_vacuum_time", 1, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("pg_stat_get_last_autovacuum_time", 1, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("pg_stat_get_last_analyze_time", 1, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("pg_stat_get_last_autoanalyze_time", 1, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("pg_stat_get_vacuum_count", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_autovacuum_count", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_analyze_count", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_autoanalyze_count", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_stat_get_xact_numscans", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("pg_relation_is_publishable", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(0), nil
		})
		reg("array_upper", 2, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("string_agg", 2, func(args []driver.Value) (driver.Value, error) {
			return nil, nil
		})
		reg("pg_get_function_result", 1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_function_arguments", 1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_function_identity_arguments", 1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_functiondef", 1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_get_function_arguments_default", 1, func(args []driver.Value) (driver.Value, error) {
			return "", nil
		})
		reg("pg_function_is_visible", 1, func(args []driver.Value) (driver.Value, error) {
			return int64(1), nil
		})
	})
}

func toString(v driver.Value) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}

// prepareCatalogQuery 把 PG 元查询语法改写为 SQLite 可执行形式。
func prepareCatalogQuery(sql string) string {
	s := sql
	s = strings.ReplaceAll(s, "OPERATOR(pg_catalog.~)", "REGEXP")
	s = strings.ReplaceAll(s, "OPERATOR(pg_catalog.!)", "NOT REGEXP")
	s = strings.ReplaceAll(s, " COLLATE pg_catalog.default", "")
	s = rewriteEStrings(s)
	s = stripSchemaPrefix(s, "pg_catalog.")
	s = rewriteRegexOps(s)
	s = stripCasts(s)
	s = rewriteAny(s)
	s = stripArraySubqueries(s)
	s = rewriteGenerateSeries(s)
	s = stripArrayIndex(s)
	return s
}

// rewriteAny 把 PG 的数组比较 `x = any(...)` 改写为 SQLite 的 IN。
var anyRE = regexp.MustCompile(`(?i)=\s*any\s*\(`)

func rewriteAny(sql string) string {
	return anyRE.ReplaceAllString(sql, "IN (")
}

// stripArraySubqueries 删除 `array(select ... from pg_catalog.unnest(...))`
// 形式的 PG 数组构造（SQLite 不支持）。
// 匹配 array(select ... from unnest(...) x) 形式的 PG 数组构造。
var arraySubqueryRE = regexp.MustCompile(`array\(select[^)]*\([^)]*\)[^)]*\)`)

func stripArraySubqueries(sql string) string {
	return arraySubqueryRE.ReplaceAllString(sql, "NULL")
}

// generateSeriesRE 匹配 `FROM generate_series(...) s`（一层嵌套括号）。
var generateSeriesRE = regexp.MustCompile(`(?i)generate_series\([^)]*\([^)]*\)[^)]*\)\s+s`)

func rewriteGenerateSeries(sql string) string {
	return generateSeriesRE.ReplaceAllString(sql, "(SELECT 0 AS generate_series) s")
}

// stripArrayIndex 把 PG 数组下标 `prattrs[s]` 置为 NULL（SQLite 无下标语法）。
var arrayIndexRE = regexp.MustCompile(`(?i)[a-z_][\w.]*\[\w+\]`)

func stripArrayIndex(sql string) string {
	return arrayIndexRE.ReplaceAllString(sql, "NULL")
}

// stripCasts 删除引号外的 `expr::type` 转换（catalog 查询不需要类型转换语义）。
func stripCasts(sql string) string {
	var out strings.Builder
	st := scanState{}
	for i := 0; i < len(sql); i++ {
		st.feed(sql, i)
		if !st.inNormal() || sql[i] != ':' || i+1 >= len(sql) || sql[i+1] != ':' {
			out.WriteByte(sql[i])
			continue
		}
		// 跳过 :: 后的类型名（标识符与点；不含空格，避免吞掉后续关键字）
		j := i + 2
		for j < len(sql) {
			c := sql[j]
			if isLetter(c) || isDigit(c) || c == '_' || c == '.' {
				j++
				continue
			}
			break
		}
		i = j - 1
	}
	return out.String()
}

// rewriteEStrings 把 E'...' 前缀去掉（SQLite 无 E 字符串）。
func rewriteEStrings(sql string) string {
	var out strings.Builder
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		// 只有独立的 E 前缀（前面不是标识符字符）才跳过；
		// 否则 'table' 这类以 e' 结尾的字符串会被误吞。
		if (c == 'E' || c == 'e') && i+1 < len(sql) && sql[i+1] == '\'' &&
			(i == 0 || !isIdentPart(sql[i-1])) {
			continue // 跳过 E 前缀
		}
		out.WriteByte(c)
	}
	return out.String()
}

// stripSchemaPrefix 去掉引号外的 schema 前缀（pg_catalog.pg_class → pg_class）。
func stripSchemaPrefix(sql, prefix string) string {
	var out strings.Builder
	st := scanState{}
	for i := 0; i < len(sql); i++ {
		st.feed(sql, i)
		if st.inNormal() && strings.HasPrefix(sql[i:], prefix) {
			i += len(prefix) - 1
			continue
		}
		out.WriteByte(sql[i])
	}
	return out.String()
}

// rewriteRegexOps 把 !~ / ~ 操作符改写为 SQLite 的 NOT REGEXP / REGEXP。
func rewriteRegexOps(sql string) string {
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
		case c == '!' && i+1 < len(sql) && sql[i+1] == '~':
			out.WriteString("NOT REGEXP")
			i++
		case c == '~' && (i == 0 || !isIdentPart(sql[i-1])):
			out.WriteString("REGEXP")
		default:
			out.WriteByte(c)
		}
	}
	return out.String()
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func asInt64(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	}
	return 0
}

// pgTypeOIDForName 把 SQLite 类型名映射为 PG 类型 OID（元数据用）。
func pgTypeOIDForName(name string) int64 {
	n := strings.ToUpper(name)
	switch {
	case strings.Contains(n, "INT"):
		return 20
	case strings.Contains(n, "BOOL"):
		return 16
	case strings.Contains(n, "REAL"), strings.Contains(n, "FLOA"), strings.Contains(n, "DOUB"):
		return 701
	case strings.Contains(n, "BLOB"), strings.Contains(n, "BYTEA"):
		return 17
	case strings.Contains(n, "NUMERIC"), strings.Contains(n, "DEC"):
		return 1700
	case strings.Contains(n, "DATE"), strings.Contains(n, "TIME"):
		return 1114
	default:
		return 25
	}
}

func pgTypeLen(oid int64) int64 {
	switch oid {
	case 16, 21:
		return 1
	case 20:
		return 8
	case 23:
		return 4
	case 701:
		return 8
	default:
		return -1
	}
}

var _ = regexp.MustCompile
var _ = strconv.Itoa
