//! DuckBridge as a loadable DuckDB extension.
//!
//! ```sql
//! LOAD 'duckbridge.duckdb_extension';
//! -- SQL Server -> this DuckDB database
//! SELECT * FROM bridge_import('mssql://sa:pw@sql01:1433/Sales', parallel := 8);
//! -- run the team's T-SQL against DuckDB (SQL Server compatibility prelude applied)
//! SELECT * FROM bridge_run('UPDATE dbo.orders SET total = qty * price WHERE ...');
//! -- ship the result back: live load + restorable .bak
//! SELECT * FROM bridge_export('mssql://sa:pw@sql01:1433/Sales_new', bak := '/backups/Sales_new.bak', fetch := 'docker:mssql');
//! -- translate text
//! SELECT bridge_transpile('tsql', 'duckdb', 'SELECT TOP 5 GETDATE(), ISNULL(a, 0) FROM t');
//! ```
//!
//! The extension talks to the database that loaded it through a fresh connection per call,
//! so nothing here depends on the caller's transaction.

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use duckbridge::api::{self, Row};
use duckbridge::transpile::{self, Dialect};
use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::ffi;
use duckdb::types::DuckString;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::Connection;

/// DuckDB release this build is pinned to (unstable C API). DuckDB's extension-ci-tools pass the
/// target version in `DUCKDB_EXTENSION_MIN_DUCKDB_VERSION`; the local Makefile build uses the default.
const DUCKDB_VERSION: &str = match option_env!("DUCKDB_EXTENSION_MIN_DUCKDB_VERSION") { Some(v) => v, None => "v1.5.6" };
const VECTOR_SIZE: usize = 2048;

/// Connections to the database that loaded us.
///
/// DuckDB frees the raw database handle it lends to the entry point as soon as loading completes,
/// so connections cannot be created later. We open a pool of them up front; every call borrows
/// one (and worker threads borrow more through the core's connection provider), so nothing here
/// touches the caller's own transaction.
const POOL_SIZE: usize = 64;
static POOL: Mutex<Vec<Connection>> = Mutex::new(Vec::new());

struct Pool;
impl duckbridge::core::conn::ConnProvider for Pool {
    fn acquire(&self) -> Result<Connection> {
        POOL.lock().map_err(|_| anyhow!("duckbridge: connection pool poisoned"))?.pop()
            .ok_or_else(|| anyhow!("duckbridge: all {POOL_SIZE} pooled DuckDB connections are in use; lower parallel :="))
    }
    fn release(&self, con: Connection) { if let Ok(mut p) = POOL.lock() { p.push(con); } }
}

/// A pooled connection that goes back to the pool on drop.
struct Pooled(Option<Connection>);
impl Drop for Pooled { fn drop(&mut self) { if let Some(c) = self.0.take() { duckbridge::core::conn::ConnProvider::release(&Pool, c); } } }
impl std::ops::Deref for Pooled { type Target = Connection; fn deref(&self) -> &Connection { self.0.as_ref().unwrap() } }

fn connect() -> Result<Pooled> { Ok(Pooled(Some(duckbridge::core::conn::ConnProvider::acquire(&Pool)?))) }

// ------------------------------------------------------------------ helpers

fn s_param(bind: &BindInfo, name: &str) -> Option<String> {
    bind.get_named_parameter(name).filter(|v| !v.is_null()).map(|v| v.to_string())
}
fn i_param(bind: &BindInfo, name: &str, default: i64) -> i64 {
    bind.get_named_parameter(name).filter(|v| !v.is_null()).map(|v| v.to_int64()).unwrap_or(default)
}
fn b_param(bind: &BindInfo, name: &str, default: bool) -> bool {
    bind.get_named_parameter(name).filter(|v| !v.is_null()).map(|v| v.to_bool()).unwrap_or(default)
}

fn add_result_columns(bind: &BindInfo) {
    bind.add_result_column("object", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("kind", LogicalTypeHandle::from(LogicalTypeId::Varchar));
    bind.add_result_column("rows", LogicalTypeHandle::from(LogicalTypeId::Bigint));
    bind.add_result_column("note", LogicalTypeHandle::from(LogicalTypeId::Varchar));
}

/// Shared init state: the operation runs once on the first `func` call, its rows are then
/// paged out VECTOR_SIZE at a time.
pub struct Paged {
    rows: Mutex<Option<Vec<Row>>>,
    cursor: AtomicUsize,
}

impl Paged {
    fn new() -> Paged { Paged { rows: Mutex::new(None), cursor: AtomicUsize::new(0) } }

    fn emit(&self, output: &mut DataChunkHandle, run: impl FnOnce() -> Result<Vec<Row>>) -> Result<(), Box<dyn Error>> {
        let mut guard = self.rows.lock().map_err(|_| "duckbridge: state poisoned")?;
        if guard.is_none() { *guard = Some(run()?); }
        let rows = guard.as_ref().unwrap();
        let start = self.cursor.load(Ordering::Relaxed).min(rows.len());
        let end = (start + VECTOR_SIZE).min(rows.len());
        let n = end - start;
        if n > 0 {
            let object = output.flat_vector(0);
            let kind = output.flat_vector(1);
            let mut count = output.flat_vector(2);
            let note = output.flat_vector(3);
            let counts = unsafe { count.as_mut_slice::<i64>() };
            for (i, r) in rows[start..end].iter().enumerate() {
                object.insert(i, r.object.as_str());
                kind.insert(i, r.kind.as_str());
                counts[i] = r.rows;
                note.insert(i, r.note.as_str());
            }
        }
        self.cursor.store(end, Ordering::Relaxed);
        output.set_len(n);
        Ok(())
    }
}

fn varchar() -> LogicalTypeHandle { LogicalTypeHandle::from(LogicalTypeId::Varchar) }
fn bigint() -> LogicalTypeHandle { LogicalTypeHandle::from(LogicalTypeId::Bigint) }
fn boolean() -> LogicalTypeHandle { LogicalTypeHandle::from(LogicalTypeId::Boolean) }
fn named(items: &[(&str, LogicalTypeHandle)]) -> Option<Vec<(String, LogicalTypeHandle)>> {
    Some(items.iter().map(|(n, t)| (n.to_string(), LogicalTypeHandle::from(t.id()))).collect())
}

// ------------------------------------------------------------------ bridge_import

pub struct ImportBind { dsn: String, req: api::ImportRequest }
struct ImportVTab;

impl VTab for ImportVTab {
    type InitData = Paged;
    type BindData = ImportBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        add_result_columns(bind);
        let d = api::ImportRequest::default();
        let req = api::ImportRequest {
            tables: s_param(bind, "tables").map(|s| api::csv_list(&s)).unwrap_or_default(),
            exclude: s_param(bind, "exclude").map(|s| api::csv_list(&s)).unwrap_or_default(),
            parallel: i_param(bind, "parallel", 0).max(0) as usize,
            split_threshold: i_param(bind, "split_threshold", d.split_threshold),
            batch_rows: i_param(bind, "batch_rows", d.batch_rows as i64).max(1) as usize,
            schema_map: s_param(bind, "schema_map").unwrap_or_default(),
            nolock: b_param(bind, "nolock", false),
            schema_only: b_param(bind, "schema_only", false),
            keys_after_load: b_param(bind, "keys_after_load", false),
            overwrite: b_param(bind, "overwrite", false),
        };
        Ok(ImportBind { dsn: bind.get_parameter(0).to_string(), req })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> { Ok(Paged::new()) }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let b = func.get_bind_data();
        func.get_init_data().emit(output, || {
            let dsn = api::parse_dsn(&b.dsn)?;
            let con = connect()?;
            api::import_mssql(&con, &dsn, &b.req)
        })
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> { Some(vec![varchar()]) }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        named(&[
            ("tables", varchar()), ("exclude", varchar()), ("parallel", bigint()), ("split_threshold", bigint()),
            ("batch_rows", bigint()), ("schema_map", varchar()), ("nolock", boolean()), ("schema_only", boolean()),
            ("keys_after_load", boolean()), ("overwrite", boolean()),
        ])
    }
}

// ------------------------------------------------------------------ bridge_export

pub struct ExportBind { target: String, req: api::ExportRequest, bak: Option<api::BakRequest> }
struct ExportVTab;

impl VTab for ExportVTab {
    type InitData = Paged;
    type BindData = ExportBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        add_result_columns(bind);
        let d = api::ExportRequest::default();
        let req = api::ExportRequest {
            database: s_param(bind, "database"),
            tables: s_param(bind, "tables").map(|s| api::csv_list(&s)).unwrap_or_default(),
            exclude: s_param(bind, "exclude").map(|s| api::csv_list(&s)).unwrap_or_default(),
            schema_map: s_param(bind, "schema_map").unwrap_or_default(),
            parallel: i_param(bind, "parallel", 0).max(0) as usize,
            drop_existing: b_param(bind, "drop_existing", false),
            schema_only: b_param(bind, "schema_only", false),
            target_version: i_param(bind, "target_version", 2019) as u16,
            string_mode: s_param(bind, "string_mode").unwrap_or(d.string_mode),
            method: s_param(bind, "method").unwrap_or(d.method),
            pk_inline: b_param(bind, "pk_inline", false),
            order_by_key: b_param(bind, "order_by_key", false),
            split_threshold: i_param(bind, "split_threshold", d.split_threshold),
            batch_rows: i_param(bind, "batch_rows", d.batch_rows as i64).max(1) as usize,
            create_database: b_param(bind, "create_database", false),
            parquet_compression: s_param(bind, "parquet_compression").unwrap_or(d.parquet_compression),
            source_label: "duckdb".into(),
        };
        let bd = api::BakRequest::default();
        let wants_bak = bind.get_named_parameter("bak").is_some() || b_param(bind, "verify_restore", false) || s_param(bind, "backup_to_url").is_some();
        let bak = wants_bak.then(|| api::BakRequest {
            out: s_param(bind, "bak").unwrap_or_default(),
            backup_dir: s_param(bind, "backup_dir").unwrap_or(bd.backup_dir),
            fetch: s_param(bind, "fetch_via").unwrap_or(bd.fetch),
            stripes: i_param(bind, "stripes", 1).max(1) as usize,
            compression: b_param(bind, "compression", true),
            verify_restore: b_param(bind, "verify_restore", false),
            drop_staging: b_param(bind, "drop_staging", false),
            to_url: s_param(bind, "backup_to_url"),
        });
        Ok(ExportBind { target: bind.get_parameter(0).to_string(), req, bak })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> { Ok(Paged::new()) }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let b = func.get_bind_data();
        func.get_init_data().emit(output, || {
            let con = connect()?;
            let t = b.target.trim();
            if let Some(p) = t.strip_prefix("script:") { return api::export_script(&con, p, &b.req); }
            if let Some(p) = t.strip_prefix("parquet:") { return api::export_parquet(&con, p, &b.req); }
            if t.starts_with("postgres://") || t.starts_with("postgresql://") || t.contains("host=") {
                return api::export_postgres(&con, t, &b.req);
            }
            let dsn = api::parse_dsn(t)?;
            match dsn.scheme.as_str() {
                "mssql" | "sqlserver" => api::export_mssql(&con, &dsn, &b.req, b.bak.as_ref()),
                other => Err(anyhow!("unsupported export target {other}://  (use mssql://, postgres://, parquet:<dir>, script:<file>)")),
            }
        })
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> { Some(vec![varchar()]) }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        named(&[
            ("database", varchar()), ("tables", varchar()), ("exclude", varchar()), ("schema_map", varchar()),
            ("parallel", bigint()), ("drop_existing", boolean()), ("schema_only", boolean()), ("target_version", bigint()),
            ("string_mode", varchar()), ("method", varchar()), ("pk_inline", boolean()), ("order_by_key", boolean()),
            ("split_threshold", bigint()), ("batch_rows", bigint()), ("create_database", boolean()),
            ("parquet_compression", varchar()),
            ("bak", varchar()), ("backup_dir", varchar()), ("fetch_via", varchar()), ("stripes", bigint()),
            ("compression", boolean()), ("verify_restore", boolean()), ("drop_staging", boolean()), ("backup_to_url", varchar()),
        ])
    }
}

// ------------------------------------------------------------------ bridge_run

pub struct RunBind { sql: String, from: Dialect, nocase: bool, schema_map: String, continue_on_error: bool }
struct RunVTab;

impl VTab for RunVTab {
    type InitData = Paged;
    type BindData = RunBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        add_result_columns(bind);
        let from_s = s_param(bind, "from").unwrap_or_else(|| "tsql".into());
        let from = Dialect::parse(&from_s).ok_or_else(|| format!("unknown dialect {from_s:?} (tsql, duckdb, postgres)"))?;
        Ok(RunBind {
            sql: bind.get_parameter(0).to_string(),
            from,
            nocase: b_param(bind, "nocase", true),
            schema_map: s_param(bind, "schema_map").unwrap_or_default(),
            continue_on_error: b_param(bind, "continue_on_error", false),
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> { Ok(Paged::new()) }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let b = func.get_bind_data();
        func.get_init_data().emit(output, || {
            let con = connect()?;
            let r = api::run_script(&con, &b.sql, b.from, b.nocase, &b.schema_map, b.continue_on_error);
            let _ = con.execute_batch("RESET integer_division; RESET default_null_order; RESET TimeZone; RESET default_collation;");
            r
        })
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> { Some(vec![varchar()]) }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        named(&[("from", varchar()), ("nocase", boolean()), ("schema_map", varchar()), ("continue_on_error", boolean())])
    }
}

// ------------------------------------------------------------------ bridge_transpile

struct TranspileScalar;

fn read_strings(input: &mut DataChunkHandle, col: usize) -> Vec<Option<String>> {
    let v = input.flat_vector(col);
    let n = input.len();
    let vals = unsafe { v.as_slice_with_len::<ffi::duckdb_string_t>(n) };
    (0..n).map(|i| if v.row_is_null(i as u64) { None } else { let mut x = vals[i]; Some(DuckString::new(&mut x).as_str().to_string()) }).collect()
}

impl VScalar for TranspileScalar {
    type State = ();

    fn invoke(_: &Self::State, input: &mut DataChunkHandle, output: &mut dyn WritableVector) -> Result<(), Box<dyn Error>> {
        let n = input.len();
        let from = read_strings(input, 0);
        let to = read_strings(input, 1);
        let sql = read_strings(input, 2);
        let version: Vec<Option<i64>> = if input.num_columns() > 3 {
            let v = input.flat_vector(3);
            let vals = unsafe { v.as_slice_with_len::<i64>(n) };
            (0..n).map(|i| if v.row_is_null(i as u64) { None } else { Some(vals[i]) }).collect()
        } else { vec![None; n] };
        let mut out = output.flat_vector();
        for i in 0..n {
            let (Some(f), Some(t), Some(s)) = (&from[i], &to[i], &sql[i]) else { out.set_null(i); continue; };
            let src = Dialect::parse(f).ok_or_else(|| format!("unknown source dialect {f:?}"))?;
            let dst = Dialect::parse(t).ok_or_else(|| format!("unknown target dialect {t:?}"))?;
            let default: &[(&str, &str)] = match (src, dst) { (Dialect::Tsql, _) => &[("dbo", "main")], (_, Dialect::Tsql) => &[("main", "dbo")], _ => &[("main", "public")] };
            let sm = api::parse_schema_map("", default);
            let opts = transpile::Options { target_version: version[i].unwrap_or(2019) as u16, prelude: false, nocase: true, float_division: true };
            let tr = transpile::translate_with(s, src, dst, &sm, &opts)?;
            let text = if tr.warnings.is_empty() { tr.sql } else {
                let mut w: String = tr.warnings.iter().map(|w| format!("-- WARNING: {w}\n")).collect();
                w.push_str(&tr.sql);
                w
            };
            out.insert(i, text.as_str());
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![
            ScalarFunctionSignature::exact(vec![varchar(), varchar(), varchar()], varchar()),
            ScalarFunctionSignature::exact(vec![varchar(), varchar(), varchar(), bigint()], varchar()),
        ]
    }
}

/// `bridge_prelude(nocase)`: the SET statements that make a DuckDB session behave like SQL Server.
struct PreludeScalar;

impl VScalar for PreludeScalar {
    type State = ();

    fn invoke(_: &Self::State, input: &mut DataChunkHandle, output: &mut dyn WritableVector) -> Result<(), Box<dyn Error>> {
        let n = input.len();
        let v = input.flat_vector(0);
        let vals = unsafe { v.as_slice_with_len::<bool>(n) };
        let out = output.flat_vector();
        for i in 0..n {
            let nocase = if v.row_is_null(i as u64) { true } else { vals[i] };
            let text = transpile::duckdb_prelude(nocase).iter().map(|p| p.trim().trim_end_matches(';').to_string() + ";").collect::<Vec<_>>().join("\n");
            out.insert(i, text.as_str());
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(vec![boolean()], varchar())]
    }
}

// ------------------------------------------------------------------ entrypoint

unsafe fn init_internal(info: ffi::duckdb_extension_info, access: *const ffi::duckdb_extension_access) -> std::result::Result<bool, Box<dyn Error>> {
    if !ffi::duckdb_rs_extension_api_init(info, access, DUCKDB_VERSION)? { return Ok(false); }
    let get_database = (*access).get_database.ok_or("get_database is null in duckdb_extension_access")?;
    let db_ptr = get_database(info);
    if db_ptr.is_null() { return Ok(false); }
    let db: ffi::duckdb_database = *db_ptr;
    let con = Connection::open_from_raw(db.cast())?;
    {
        let mut pool = POOL.lock().map_err(|_| "duckbridge: connection pool poisoned")?;
        for _ in 0..POOL_SIZE { pool.push(Connection::open_from_raw(db.cast())?); }
    }
    duckbridge::core::conn::set_provider(Box::new(Pool));
    con.register_table_function::<ImportVTab>("bridge_import")?;
    con.register_table_function::<ExportVTab>("bridge_export")?;
    con.register_table_function::<RunVTab>("bridge_run")?;
    con.register_scalar_function::<TranspileScalar>("bridge_transpile")?;
    con.register_scalar_function::<PreludeScalar>("bridge_prelude")?;
    drop(con);
    Ok(true)
}

/// Entry point called by DuckDB: `<extension name>_init_c_api`.
///
/// # Safety
/// Called by DuckDB with valid pointers when the extension is loaded.
#[no_mangle]
pub unsafe extern "C" fn duckbridge_init_c_api(info: ffi::duckdb_extension_info, access: *const ffi::duckdb_extension_access) -> bool {
    match init_internal(info, access) {
        Ok(v) => v,
        Err(e) => {
            if let Some(set_error) = (*access).set_error {
                let msg = std::ffi::CString::new(format!("duckbridge: {e}")).unwrap_or_else(|_| c"duckbridge: initialisation failed".to_owned());
                set_error(info, msg.as_ptr());
            }
            false
        }
    }
}
