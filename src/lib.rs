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
//!
//! Layout (prepared for a second DuckDB backend, see docs/handoff-duck-layer.md):
//! * this file: the backend-neutral part. It only calls the core library (`api::*`,
//!   `transpile::*`, `core::conn`, `core::duck`) and sees the host's function-call parameters
//!   through the [`Params`] trait: connection pool, function signatures, parameter parsing into
//!   `ImportRequest` / `ExportRequest` / `RunRequest` / `BakRequest`, the operations themselves,
//!   result paging and the text of the scalar functions.
//! * `duckdb_v1.rs`: the duckdb-rs glue (DuckDB 1.x C API): `VTab` / `VScalar` impls, vector
//!   reads/writes, the `Params` impl over `BindInfo`, and the `duckbridge_init_c_api` entry point.
//! * `duckdb_neo.rs` (feature `neo`, built from extension/neo/Cargo.toml): the duckdb-neo glue
//!   (DuckDB 2.0 v2 C API). The v2 API gives an extension no connection to the database that loaded
//!   it, so on 2.0 everything is a function DuckDB calls: `bridge_scan` / `bridge_tables` (table
//!   functions, rows out), `COPY ... TO 'mssql://...' (FORMAT duckbridge)` (rows in),
//!   `PRAGMA bridge_import` / `PRAGMA bridge_run` (SQL text DuckDB runs). Their backend-neutral
//!   parts (URL and option parsing, the scripts, the COPY target) are the "DuckDB 2.0 functions"
//!   section at the end of this file; docs/plan-2.0-extension.md has the design.

// The `neo` glue does not use the connection pool and the 1.x table-function plumbing, the v1 glue
// does not use the DuckDB 2.0 function helpers.
#![cfg_attr(feature = "neo", allow(dead_code))]

use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use duckbridge::api::{self, Row};
use duckbridge::core::duck::{self, Connection};
use duckbridge::transpile::{self, Dialect};

#[cfg(not(feature = "neo"))]
mod duckdb_v1;
#[cfg(feature = "neo")]
mod duckdb_neo;

/// Rows per output chunk (DuckDB's standard vector size).
const VECTOR_SIZE: usize = 2048;

// ------------------------------------------------------------------ connection pool

/// Connections to the database that loaded us.
///
/// DuckDB frees the raw database handle it lends to the entry point as soon as loading completes,
/// so connections cannot be created later. The entry point opens a pool of them up front; every
/// call borrows one (and worker threads borrow more through the core's connection provider), so
/// nothing here touches the caller's own transaction.
const POOL_SIZE: usize = 64;
/// bulk_load uses a second pooled connection per worker, so parallel is capped at half the pool.
const MAX_PARALLEL: usize = POOL_SIZE / 2;
static POOL: Mutex<Vec<Connection>> = Mutex::new(Vec::new());

struct Pool;
impl duckbridge::core::conn::ConnProvider for Pool {
    fn acquire(&self) -> Result<Connection> {
        POOL.lock().map_err(|_| anyhow!("duckbridge: connection pool poisoned"))?.pop()
            .ok_or_else(|| anyhow!("duckbridge: all {POOL_SIZE} pooled DuckDB connections are in use; lower parallel :="))
    }
    fn release(&self, con: Connection) { if let Ok(mut p) = POOL.lock() { p.push(con); } }
}

/// Fill the pool with `POOL_SIZE` connections from `open` and install it as the core's
/// connection provider (called once by the entry point).
fn install_pool(mut open: impl FnMut() -> std::result::Result<Connection, Box<dyn Error>>) -> std::result::Result<(), Box<dyn Error>> {
    {
        let mut pool = POOL.lock().map_err(|_| "duckbridge: connection pool poisoned")?;
        for _ in 0..POOL_SIZE { pool.push(open()?); }
    }
    duckbridge::core::conn::set_provider(Box::new(Pool));
    Ok(())
}

/// A pooled connection that goes back to the pool on drop.
struct Pooled(Option<Connection>);
impl Drop for Pooled { fn drop(&mut self) { if let Some(c) = self.0.take() { duckbridge::core::conn::ConnProvider::release(&Pool, c); } } }
impl std::ops::Deref for Pooled { type Target = Connection; fn deref(&self) -> &Connection { self.0.as_ref().unwrap() } }

fn connect() -> Result<Pooled> { Ok(Pooled(Some(duckbridge::core::conn::ConnProvider::acquire(&Pool)?))) }

// ------------------------------------------------------------------ parameters

/// Logical type of a function parameter or result column.
#[derive(Debug, Clone, Copy)]
enum Ty { Varchar, Bigint, Boolean }

/// The parameters of one table-function call, as the backend's bind info exposes them.
trait Params {
    /// Positional parameter `i` as text.
    fn positional(&self, i: usize) -> String;
    /// Whether the named parameter was given at all (even as NULL).
    fn given(&self, name: &str) -> bool;
    /// Named parameter as text; `None` when absent or NULL.
    fn text(&self, name: &str) -> Option<String>;
    /// Named parameter as BIGINT; `None` when absent or NULL.
    fn int(&self, name: &str) -> Option<i64>;
    /// Named parameter as BOOLEAN; `None` when absent or NULL.
    fn boolean(&self, name: &str) -> Option<bool>;
}

fn s_param(p: &dyn Params, name: &str) -> Option<String> { p.text(name) }
fn i_param(p: &dyn Params, name: &str, default: i64) -> i64 { p.int(name).unwrap_or(default) }
fn b_param(p: &dyn Params, name: &str, default: bool) -> bool { p.boolean(name).unwrap_or(default) }

/// Result columns of the three table functions.
const RESULT_COLUMNS: &[(&str, Ty)] = &[("object", Ty::Varchar), ("kind", Ty::Varchar), ("rows", Ty::Bigint), ("note", Ty::Varchar)];

// ------------------------------------------------------------------ result paging

/// Shared init state: the operation runs once on the first call, its rows are then
/// paged out VECTOR_SIZE at a time.
pub struct Paged {
    rows: Mutex<Option<Vec<Row>>>,
    cursor: AtomicUsize,
}

impl Paged {
    fn new() -> Paged { Paged { rows: Mutex::new(None), cursor: AtomicUsize::new(0) } }

    /// Run the operation on the first call, then give `write` the next page (at most VECTOR_SIZE
    /// rows; empty once everything was handed out).
    fn next_page(&self, run: impl FnOnce() -> Result<Vec<Row>>, write: impl FnOnce(&[Row])) -> std::result::Result<(), Box<dyn Error>> {
        let mut guard = self.rows.lock().map_err(|_| "duckbridge: state poisoned")?;
        if guard.is_none() { *guard = Some(run()?); }
        let rows = guard.as_ref().unwrap();
        let start = self.cursor.load(Ordering::Relaxed).min(rows.len());
        let end = (start + VECTOR_SIZE).min(rows.len());
        write(&rows[start..end]);
        self.cursor.store(end, Ordering::Relaxed);
        Ok(())
    }
}

// ------------------------------------------------------------------ bridge_import

const IMPORT_PARAMS: &[(&str, Ty)] = &[
    ("tables", Ty::Varchar), ("exclude", Ty::Varchar), ("parallel", Ty::Bigint), ("split_threshold", Ty::Bigint),
    ("batch_rows", Ty::Bigint), ("schema_map", Ty::Varchar), ("nolock", Ty::Boolean), ("schema_only", Ty::Boolean),
    ("keys_after_load", Ty::Boolean), ("overwrite", Ty::Boolean),
    ("isolation", Ty::Varchar), ("lock_timeout", Ty::Bigint), ("lock_check", Ty::Boolean), ("lock_wait", Ty::Bigint), ("on_lock", Ty::Varchar),
    ("max_rereads", Ty::Bigint), ("manifest", Ty::Varchar), ("readonly", Ty::Boolean), ("wait_for", Ty::Varchar), ("wait_timeout", Ty::Bigint),
    ("log_file", Ty::Varchar), ("summary_file", Ty::Varchar),
];

pub struct ImportBind { dsn: String, req: api::ImportRequest }

fn import_bind(p: &dyn Params) -> ImportBind {
    let d = api::ImportRequest::default();
    let req = api::ImportRequest {
        tables: s_param(p, "tables").map(|s| api::csv_list(&s)).unwrap_or_default(),
        exclude: s_param(p, "exclude").map(|s| api::csv_list(&s)).unwrap_or_default(),
        parallel: i_param(p, "parallel", 0).max(0) as usize,
        split_threshold: i_param(p, "split_threshold", d.split_threshold),
        batch_rows: i_param(p, "batch_rows", d.batch_rows as i64).max(1) as usize,
        schema_map: s_param(p, "schema_map").unwrap_or_default(),
        nolock: b_param(p, "nolock", false),
        schema_only: b_param(p, "schema_only", false),
        keys_after_load: b_param(p, "keys_after_load", false),
        overwrite: b_param(p, "overwrite", false),
        isolation: s_param(p, "isolation").unwrap_or(d.isolation),
        lock_timeout_ms: i_param(p, "lock_timeout", d.lock_timeout_ms),
        lock_check: b_param(p, "lock_check", true),
        lock_wait_s: i_param(p, "lock_wait", d.lock_wait_s as i64).max(0) as u64,
        on_lock: s_param(p, "on_lock").unwrap_or(d.on_lock),
        max_rereads: i_param(p, "max_rereads", d.max_rereads as i64).max(0) as u32,
        manifest: s_param(p, "manifest"),
        readonly: b_param(p, "readonly", false),
        wait_for: s_param(p, "wait_for"),
        wait_timeout_s: i_param(p, "wait_timeout", d.wait_timeout_s as i64).max(0) as u64,
        log_file: s_param(p, "log_file"),
        summary_file: s_param(p, "summary_file"),
    };
    ImportBind { dsn: p.positional(0), req }
}

fn import_run(b: &ImportBind) -> Result<Vec<Row>> {
    let dsn = api::parse_dsn(&b.dsn)?;
    let con = connect()?;
    api::import_mssql(&con, &dsn, &b.req)
}

// ------------------------------------------------------------------ bridge_export

const EXPORT_PARAMS: &[(&str, Ty)] = &[
    ("database", Ty::Varchar), ("tables", Ty::Varchar), ("exclude", Ty::Varchar), ("schema_map", Ty::Varchar),
    ("parallel", Ty::Bigint), ("drop_existing", Ty::Boolean), ("schema_only", Ty::Boolean), ("target_version", Ty::Bigint),
    ("string_mode", Ty::Varchar), ("method", Ty::Varchar), ("pk_inline", Ty::Boolean), ("order_by_key", Ty::Boolean),
    ("split_threshold", Ty::Bigint), ("batch_rows", Ty::Bigint), ("create_database", Ty::Boolean),
    ("disposable_db_pattern", Ty::Varchar), ("compat_level", Ty::Bigint), ("recovery", Ty::Varchar),
    ("plan", Ty::Boolean), ("log_file", Ty::Varchar), ("summary_file", Ty::Varchar), ("string_type", Ty::Varchar), ("string_sizes", Ty::Varchar),
    ("into_existing", Ty::Boolean), ("ignore_extra_columns", Ty::Boolean), ("allow_rescale", Ty::Boolean), ("check_only", Ty::Boolean), ("check_pk", Ty::Varchar),
    ("split", Ty::Varchar), ("strategy", Ty::Varchar), ("max_jobs_per_table", Ty::Bigint), ("state_dir", Ty::Varchar), ("replace_partitions", Ty::Boolean), ("drop_old", Ty::Boolean),
    ("verify", Ty::Varchar), ("sample", Ty::Bigint), ("source_parquet", Ty::Varchar),
    ("parquet_compression", Ty::Varchar),
    ("bak", Ty::Varchar), ("backup_dir", Ty::Varchar), ("fetch_via", Ty::Varchar), ("stripes", Ty::Bigint),
    ("compression", Ty::Boolean), ("verify_restore", Ty::Boolean), ("drop_staging", Ty::Boolean), ("backup_to_url", Ty::Varchar),
];

pub struct ExportBind { target: String, req: api::ExportRequest, bak: Option<api::BakRequest> }

fn export_bind(p: &dyn Params) -> ExportBind {
    let d = api::ExportRequest::default();
    let req = api::ExportRequest {
        database: s_param(p, "database"),
        tables: s_param(p, "tables").map(|s| api::csv_list(&s)).unwrap_or_default(),
        exclude: s_param(p, "exclude").map(|s| api::csv_list(&s)).unwrap_or_default(),
        schema_map: s_param(p, "schema_map").unwrap_or_default(),
        parallel: (i_param(p, "parallel", 0).max(0) as usize).clamp(0, MAX_PARALLEL).max(if i_param(p, "parallel", 0) == 0 { 0 } else { 1 }),
        drop_existing: b_param(p, "drop_existing", false),
        schema_only: b_param(p, "schema_only", false),
        target_version: i_param(p, "target_version", 2019) as u16,
        string_mode: s_param(p, "string_mode").unwrap_or(d.string_mode),
        string_type: s_param(p, "string_type").unwrap_or(d.string_type),
        string_sizes: s_param(p, "string_sizes").unwrap_or(d.string_sizes),
        method: s_param(p, "method").unwrap_or(d.method),
        pk_inline: b_param(p, "pk_inline", false),
        order_by_key: b_param(p, "order_by_key", false),
        split_threshold: i_param(p, "split_threshold", d.split_threshold),
        batch_rows: i_param(p, "batch_rows", d.batch_rows as i64).max(1) as usize,
        create_database: b_param(p, "create_database", false),
        parquet_compression: s_param(p, "parquet_compression").unwrap_or(d.parquet_compression),
        source_label: "duckdb".into(),
        disposable_db_pattern: s_param(p, "disposable_db_pattern"),
        compat_level: p.given("compat_level").then(|| i_param(p, "compat_level", 0) as u16),
        recovery: s_param(p, "recovery"),
        plan: b_param(p, "plan", false),
        log_file: s_param(p, "log_file"),
        summary_file: s_param(p, "summary_file"),
        progress_seconds: d.progress_seconds,
        into_existing: b_param(p, "into_existing", false),
        ignore_extra_columns: b_param(p, "ignore_extra_columns", false),
        allow_rescale: b_param(p, "allow_rescale", false),
        check_only: b_param(p, "check_only", false),
        check_pk: s_param(p, "check_pk").unwrap_or_default(),
        splits: s_param(p, "split").unwrap_or_default(),
        strategies: s_param(p, "strategy").unwrap_or_default(),
        max_jobs_per_table: i_param(p, "max_jobs_per_table", 0).max(0) as usize,
        state_dir: s_param(p, "state_dir"),
        replace_partitions: b_param(p, "replace_partitions", false),
        drop_old: b_param(p, "drop_old", false),
        verify: s_param(p, "verify").unwrap_or(d.verify),
        sample: i_param(p, "sample", d.sample as i64).max(0) as usize,
        source_parquet: s_param(p, "source_parquet").unwrap_or_default(),
    };
    let bd = api::BakRequest::default();
    let wants_bak = p.given("bak") || b_param(p, "verify_restore", false) || s_param(p, "backup_to_url").is_some();
    let bak = wants_bak.then(|| api::BakRequest {
        out: s_param(p, "bak").unwrap_or_default(),
        backup_dir: s_param(p, "backup_dir").unwrap_or(bd.backup_dir),
        fetch: s_param(p, "fetch_via").unwrap_or(bd.fetch),
        stripes: i_param(p, "stripes", 1).max(1) as usize,
        compression: b_param(p, "compression", true),
        verify_restore: b_param(p, "verify_restore", false),
        drop_staging: b_param(p, "drop_staging", false),
        to_url: s_param(p, "backup_to_url"),
    });
    ExportBind { target: p.positional(0), req, bak }
}

fn export_run(b: &ExportBind) -> Result<Vec<Row>> {
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
}

// ------------------------------------------------------------------ bridge_run

const RUN_PARAMS: &[(&str, Ty)] = &[
    ("from", Ty::Varchar), ("nocase", Ty::Boolean), ("nocase_mode", Ty::Varchar), ("strict", Ty::Boolean), ("mssql_math", Ty::Boolean),
    ("types", Ty::Varchar), ("schema_map", Ty::Varchar), ("continue_on_error", Ty::Boolean),
];

pub struct RunBind { sql: String, req: api::RunRequest }

fn run_bind(p: &dyn Params) -> std::result::Result<RunBind, Box<dyn Error>> {
    let from_s = s_param(p, "from").unwrap_or_else(|| "tsql".into());
    let from = Dialect::parse(&from_s).ok_or_else(|| format!("unknown dialect {from_s:?} (tsql, duckdb, postgres)"))?;
    // nocase := true/false keeps working (rewrite / off); nocase_mode := 'global' | 'rewrite' | 'off' is explicit
    let nocase = s_param(p, "nocase_mode").unwrap_or_else(|| if b_param(p, "nocase", true) { "rewrite".into() } else { "off".into() });
    Ok(RunBind {
        sql: p.positional(0),
        req: api::RunRequest {
            from,
            nocase,
            strict: b_param(p, "strict", false),
            mssql_math: b_param(p, "mssql_math", false),
            types: s_param(p, "types").unwrap_or_default(),
            schema_map: s_param(p, "schema_map").unwrap_or_default(),
            continue_on_error: b_param(p, "continue_on_error", false),
        },
    })
}

fn run_run(b: &RunBind) -> Result<Vec<Row>> {
    let con = connect()?;
    let r = api::run_script(&con, &b.sql, &b.req);
    let _ = duck::exec(&con, "RESET integer_division; RESET default_null_order; RESET TimeZone; RESET default_collation;");
    r
}

// ------------------------------------------------------------------ bridge_transpile / bridge_prelude

/// `bridge_transpile(from, to, sql [, target_version])` for one row.
fn transpile_text(from: &str, to: &str, sql: &str, version: Option<i64>) -> std::result::Result<String, Box<dyn Error>> {
    let src = Dialect::parse(from).ok_or_else(|| format!("unknown source dialect {from:?}"))?;
    let dst = Dialect::parse(to).ok_or_else(|| format!("unknown target dialect {to:?}"))?;
    let default: &[(&str, &str)] = match (src, dst) { (Dialect::Tsql, _) => &[("dbo", "main")], (_, Dialect::Tsql) => &[("main", "dbo")], _ => &[("main", "public")] };
    let sm = api::parse_schema_map("", default);
    let opts = transpile::Options { target_version: version.unwrap_or(2019) as u16, prelude: false, float_division: true, ..transpile::Options::default() };
    let tr = transpile::translate_with(sql, src, dst, &sm, &opts)?;
    Ok(if tr.warnings.is_empty() { tr.sql } else {
        let mut w: String = tr.warnings.iter().map(|w| format!("-- WARNING: {w}\n")).collect();
        w.push_str(&tr.sql);
        w
    })
}

/// `bridge_prelude(nocase)`: the SET statements that make a DuckDB session behave like SQL Server.
fn prelude_text(nocase: bool) -> String {
    transpile::duckdb_prelude_for(if nocase { transpile::NocaseMode::Rewrite } else { transpile::NocaseMode::Off }).iter().map(|p| p.trim().trim_end_matches(';').to_string() + ";").collect::<Vec<_>>().join("\n")
}

// ------------------------------------------------------------------ DuckDB 2.0 functions (backend-neutral parts)

#[cfg_attr(not(feature = "neo"), allow(dead_code))]
pub(crate) mod v2 {
    use anyhow::{anyhow, bail, Context, Result};
    use duckbridge::api;
    use duckbridge::core::duck::arrow::record_batch::RecordBatch;
    use duckbridge::core::typemap::{quote_duck, StringType};
    use duckbridge::mssql::bulk::{self, BulkOptions, BulkSink};
    use duckbridge::mssql::convert::Converter;
    use duckbridge::mssql::fingerprint::{Isolation, ReadSafety};
    use duckbridge::mssql::import::ImportOptions;
    use duckbridge::mssql::scan::{self, ScanPlan, ScriptOptions};
    use duckbridge::mssql::{self, bracket, MssqlTarget};
    use duckbridge::transpile::{self, Dialect};

    /// A SQL Server database named by an `mssql://user:password@host:port/Database` URL.
    pub struct DbUrl {
        pub target: MssqlTarget,
        pub database: String,
    }

    /// Parse `text` (`what` names the function in errors). The database part is required.
    pub fn db_url(text: &str, what: &str) -> Result<DbUrl> {
        let dsn = api::parse_dsn(text.trim()).with_context(|| format!("{what}: bad URL"))?;
        if dsn.scheme != "mssql" && dsn.scheme != "sqlserver" { bail!("{what} expects an mssql:// URL, got {}://", dsn.scheme); }
        let database = scan::require_database(dsn.database.as_deref(), what)?;
        Ok(DbUrl { target: dsn.mssql_target(Some(&database)), database })
    }

    // -------------------------------------------------------------- bridge_scan

    /// The named parameters of `bridge_scan`, in declaration order (the glue declares them in this
    /// order; DuckDB passes them after the positional ones, defaults filled in).
    pub const SCAN_NAMED: &[&str] = &["nolock", "batch_rows", "lock_timeout", "isolation", "readonly"];
    pub const SCAN_DEFAULT_BATCH: i64 = 50_000;
    pub const SCAN_DEFAULT_LOCK_MS: i64 = 1000;

    /// `bridge_scan`'s named arguments.
    #[derive(Debug, Clone)]
    pub struct ScanArgs {
        pub nolock: bool,
        pub batch_rows: i64,
        pub lock_timeout_ms: i64,
        pub isolation: String,
        pub readonly: bool,
    }

    impl Default for ScanArgs {
        fn default() -> Self { ScanArgs { nolock: false, batch_rows: SCAN_DEFAULT_BATCH, lock_timeout_ms: SCAN_DEFAULT_LOCK_MS, isolation: "read-committed".into(), readonly: false } }
    }

    impl ScanArgs {
        /// The R6 session settings (NOLOCK reads as READ UNCOMMITTED, as the importer's `--nolock`).
        pub fn safety(&self) -> Result<ReadSafety> {
            let mut s = ReadSafety { lock_timeout_ms: self.lock_timeout_ms, isolation: Isolation::parse(&self.isolation)?, readonly_intent: self.readonly, ..ReadSafety::default() };
            if self.nolock { s.isolation = Isolation::ReadUncommitted; }
            Ok(s)
        }
        /// The non-default arguments as `, name := value` SQL (for generated `bridge_scan` calls).
        pub fn sql(&self) -> String {
            let d = ScanArgs::default();
            let mut out = String::new();
            if self.nolock { out.push_str(", nolock := true"); }
            if self.batch_rows != d.batch_rows { out.push_str(&format!(", batch_rows := {}", self.batch_rows)); }
            if self.lock_timeout_ms != d.lock_timeout_ms { out.push_str(&format!(", lock_timeout := {}", self.lock_timeout_ms)); }
            if Isolation::parse(&self.isolation).ok() != Some(Isolation::ReadCommitted) { out.push_str(&format!(", isolation := {}", scan::duck_str(&self.isolation))); }
            if self.readonly { out.push_str(", readonly := true"); }
            out
        }
    }

    /// A bound `bridge_scan`: where to read, how, and the resolved columns / SELECT.
    pub struct ScanBind {
        pub url: DbUrl,
        pub safety: ReadSafety,
        pub source: String,
        pub plan: ScanPlan,
        pub batch_rows: usize,
    }

    /// Resolve `bridge_scan(dsn, source, ...)` (connects and describes the source).
    pub fn scan_bind(dsn: &str, source: &str, a: &ScanArgs) -> Result<ScanBind> {
        let url = db_url(dsn, "bridge_scan")?;
        let safety = a.safety()?;
        if a.batch_rows < 1 { bail!("bridge_scan: batch_rows must be at least 1, got {}", a.batch_rows); }
        let plan = scan::plan(&url.target, &url.database, &safety, source, a.nolock).with_context(|| format!("bridge_scan {}", source.trim()))?;
        Ok(ScanBind { url, safety, source: source.trim().to_string(), plan, batch_rows: a.batch_rows as usize })
    }

    // -------------------------------------------------------------- bridge_tables / PRAGMA bridge_import

    /// One row of `bridge_tables`.
    pub struct TableRow {
        pub schema: String,
        pub name: String,
        pub kind: &'static str,
        pub rows: i64,
        pub columns: i64,
        pub pk: String,
        pub import_sql: String,
    }

    fn import_options(tables: &str, exclude: &str, schema_map: &str, keys_after_load: bool, nolock: bool) -> ImportOptions {
        ImportOptions {
            keys_after_load, include: api::csv_list(tables), exclude: api::csv_list(exclude), parallel: 1, split_threshold: i64::MAX, batch_rows: 1,
            schema_map: api::parse_schema_map(schema_map, &[("dbo", "main")]), nolock,
        }
    }

    /// `bridge_tables(dsn, tables, exclude, schema_map)`: the tables and views of the database, each
    /// table with the `CREATE TABLE ... AS FROM bridge_scan(...)` that copies it.
    pub fn tables_rows(dsn: &str, tables: &str, exclude: &str, schema_map: &str, a: &ScanArgs) -> Result<Vec<TableRow>> {
        let url = db_url(dsn, "bridge_tables")?;
        let o = import_options(tables, exclude, schema_map, false, a.nolock);
        let (ts, vs, _) = scan::catalog(&url.target, &url.database, &a.safety()?, &o)?;
        let sm = |s: &str| o.schema_map.get(s).cloned().unwrap_or_else(|| s.to_string());
        let mut out: Vec<TableRow> = ts.iter().map(|t| TableRow {
            schema: t.schema.clone(), name: t.name.clone(), kind: "table", rows: t.row_count, columns: t.columns.len() as i64, pk: t.pk.join(","),
            import_sql: format!("CREATE TABLE {}.{} AS FROM bridge_scan({}, {}{})", quote_duck(&sm(&t.schema)), quote_duck(&t.name), scan::duck_str(dsn.trim()),
                scan::duck_str(&format!("{}.{}", bracket(&t.schema), bracket(&t.name))), a.sql()),
        }).collect();
        out.extend(vs.iter().map(|v| TableRow { schema: v.schema.clone(), name: v.name.clone(), kind: "view", rows: 0, columns: 0, pk: String::new(), import_sql: String::new() }));
        Ok(out)
    }

    /// `PRAGMA bridge_import(dsn, ...)`'s arguments.
    #[derive(Debug, Clone)]
    pub struct ImportArgs {
        pub tables: String,
        pub exclude: String,
        pub schema_map: String,
        pub keys: bool,
        pub keys_after_load: bool,
        pub overwrite: bool,
        pub views: bool,
        pub schema_only: bool,
        pub scan: ScanArgs,
    }

    /// The script `PRAGMA bridge_import` hands DuckDB (see `scan::import_script`).
    pub fn import_sql(dsn: &str, a: &ImportArgs) -> Result<String> {
        let url = db_url(dsn, "bridge_import")?;
        let o = import_options(&a.tables, &a.exclude, &a.schema_map, a.keys_after_load, a.scan.nolock);
        let (ts, vs, mut warnings) = scan::catalog(&url.target, &url.database, &a.scan.safety()?, &o)?;
        if ts.is_empty() && vs.is_empty() && (!o.include.is_empty() || !o.exclude.is_empty()) {
            bail!("bridge_import: no table or view of {} matches tables = {:?} / exclude = {:?}", url.database, a.tables, a.exclude);
        }
        let s = ScriptOptions { keys: a.keys, overwrite: a.overwrite, views: a.views, schema_only: a.schema_only, scan_args: a.scan.sql() };
        Ok(scan::import_script(dsn.trim(), &ts, &vs, &o, &s, &mut warnings))
    }

    // -------------------------------------------------------------- PRAGMA bridge_run

    /// `PRAGMA bridge_run(sql, ...)`'s arguments.
    #[derive(Debug, Clone)]
    pub struct RunArgs {
        pub from: String,
        pub nocase_mode: String,
        pub strict: bool,
        pub mssql_math: bool,
        pub types: String,
        pub schema_map: String,
        pub reset: bool,
    }

    /// The prelude settings of a T-SQL `bridge_run` as (name, value) pairs. DuckDB applies
    /// `integer_division` when it *parses* a statement, and it parses a pragma's whole script before
    /// running any of it, so the glue sets these on the session in the pragma callback (before DuckDB
    /// parses the script); the script repeats them as `SET SESSION` lines.
    pub fn run_settings(a: &RunArgs) -> Result<Vec<(String, String)>> {
        if Dialect::parse(&a.from) != Some(Dialect::Tsql) { return Ok(vec![]); }
        let mode = api::nocase_mode(&a.nocase_mode)?;
        Ok(transpile::duckdb_prelude_for(mode).iter().filter_map(|p| {
            let (k, v) = p.trim().trim_end_matches(';').strip_prefix("SET ")?.split_once('=')?;
            Some((k.trim().to_string(), v.trim().trim_matches('\'').to_string()))
        }).collect())
    }

    /// The DuckDB script for `sql`: translator warnings as comments, the SQL Server compatibility
    /// prelude (`SET SESSION ...`; for T-SQL), the math macros (`mssql_math`), the translated
    /// statements, and with `reset` the RESETs of the prelude settings. Strict mode with warnings is an
    /// error (nothing runs).
    pub fn run_sql(sql: &str, a: &RunArgs) -> Result<String> {
        let from = Dialect::parse(&a.from).ok_or_else(|| anyhow!("bridge_run: unknown dialect {:?} (tsql, duckdb, postgres)", a.from))?;
        let mode = api::nocase_mode(&a.nocase_mode)?;
        let sm = api::parse_schema_map(&a.schema_map, &[("dbo", "main")]);
        let opts = transpile::Options { prelude: false, nocase_mode: Some(mode), strict: a.strict, mssql_math: a.mssql_math, types: transpile::parse_types(&a.types), ..transpile::Options::default() };
        let tr = match transpile::translate_with(sql, from, Dialect::DuckDb, &sm, &opts) {
            Ok(t) => t,
            Err(e @ transpile::TranslateError::Strict(_)) => bail!("bridge_run strict: the script has translator warnings; nothing was executed\n{e}"),
            Err(e) => return Err(e.into()),
        };
        // comments: each merges into the statement after it
        let mut out: Vec<String> = tr.warnings.iter().map(|w| format!("-- WARNING: {}", w.to_string().replace('\n', " "))).collect();
        if from == Dialect::Tsql {
            for p in transpile::duckdb_prelude_for(mode) {
                let p = p.trim().trim_end_matches(';');
                out.push(p.strip_prefix("SET ").map(|rest| format!("SET SESSION {rest}")).unwrap_or_else(|| p.to_string()));
            }
            if a.mssql_math { out.extend(duckbridge::mathcompat::macros().into_iter().map(|m| m.trim().trim_end_matches(';').to_string())); }
        }
        let stmts: Vec<String> = transpile::tokens::statements(&transpile::tokens::tokenize(&tr.sql, false)).iter()
            .map(|t| transpile::tokens::join(t).trim().trim_end_matches(';').trim().to_string())
            .filter(|s| !s.is_empty() && !s.lines().all(|l| l.trim().is_empty() || l.trim_start().starts_with("--")))
            .collect();
        let Some(last) = stmts.len().checked_sub(1) else { bail!("bridge_run: the script has no statements") };
        for (i, s) in stmts.into_iter().enumerate() {
            // DuckDB 2.0 serves a pragma's statements as one result and refuses a row-returning
            // statement followed by others: earlier SELECTs (no effect on the database) are left out
            if i < last && returns_rows(&s) {
                out.push(format!("-- skipped, only the last statement's rows are returned: {}", s.split_whitespace().collect::<Vec<_>>().join(" ").chars().take(200).collect::<String>()));
                continue;
            }
            if i == last && a.reset && returns_rows(&s) {
                // keep the rows while the settings are reset, then return them
                out.push(format!("CREATE OR REPLACE TEMP TABLE {RUN_RESULT} AS {s}"));
                out.extend(RESETS.map(String::from));
                out.push(format!("SELECT * FROM {RUN_RESULT}"));
                continue;
            }
            out.push(s);
            if i == last && a.reset { out.extend(RESETS.map(String::from)); }
        }
        Ok(out.join(";\n") + ";")
    }

    /// The prelude settings `reset` restores.
    const RESETS: [&str; 4] = ["RESET integer_division", "RESET default_null_order", "RESET TimeZone", "RESET default_collation"];
    /// Temporary table holding the last statement's rows with `reset = true`.
    const RUN_RESULT: &str = "__bridge_run_result";

    /// Whether a DuckDB statement returns rows (SELECT, WITH ... SELECT, FROM ..., VALUES, SHOW, ...).
    fn returns_rows(s: &str) -> bool {
        let words: Vec<String> = s.split(|c: char| !(c.is_alphanumeric() || c == '_')).filter(|w| !w.is_empty()).map(|w| w.to_ascii_uppercase()).collect();
        match words.first().map(String::as_str) {
            Some("SELECT" | "FROM" | "VALUES" | "SHOW" | "DESCRIBE" | "SUMMARIZE" | "EXPLAIN" | "TABLE" | "PIVOT" | "UNPIVOT") => true,
            Some("WITH") => !words.iter().any(|w| matches!(w.as_str(), "INSERT" | "UPDATE" | "DELETE" | "MERGE")),
            _ => false,
        }
    }

    // -------------------------------------------------------------- COPY ... TO 'mssql://...' (FORMAT duckbridge)

    /// The COPY options DuckBridge reads (DuckDB's own, like BATCH_SIZE or USE_TMP_FILE, never reach
    /// the format; `ORDER_BY` is one of DuckDB's, so the bulk ORDER hint is `SORT_BY`).
    pub const COPY_OPTIONS: &[&str] = &["create", "drop_existing", "keep_identity", "table_lock", "batch_rows", "check_constraints", "sort_by", "string_type", "target_version", "table"];

    /// Parsed COPY options.
    #[derive(Debug, Clone)]
    pub struct CopyOptions {
        pub create: bool,
        pub drop_existing: bool,
        pub keep_identity: bool,
        pub table_lock: bool,
        pub batch_rows: usize,
        pub check_constraints: bool,
        pub sort_by: Option<String>,
        pub string_type: StringType,
        pub target_version: u16,
        pub table: Option<String>,
    }

    fn flag(name: &str, v: &str) -> Result<bool> {
        match v.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" | "" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            o => bail!("COPY option {}: expected true or false, got {o:?}", name.to_uppercase()),
        }
    }

    fn number(name: &str, v: &str) -> Result<i64> { v.trim().parse::<i64>().map_err(|_| anyhow!("COPY option {}: expected a number, got {v:?}", name.to_uppercase())) }

    /// Parse (name, value text) pairs; unknown names are errors listing the known ones.
    pub fn copy_options(opts: &[(String, String)]) -> Result<CopyOptions> {
        let mut o = CopyOptions { create: false, drop_existing: false, keep_identity: false, table_lock: true, batch_rows: 100_000, check_constraints: true, sort_by: None,
            string_type: StringType::Nvarchar, target_version: 2019, table: None };
        for (name, v) in opts {
            let n = name.to_ascii_lowercase();
            match n.as_str() {
                "create" | "create_table" => o.create = flag(name, v)?,
                "drop_existing" => o.drop_existing = flag(name, v)?,
                "keep_identity" => o.keep_identity = flag(name, v)?,
                "table_lock" => o.table_lock = flag(name, v)?,
                "check_constraints" => o.check_constraints = flag(name, v)?,
                "batch_rows" => { let b = number(name, v)?; if b < 1 { bail!("COPY option BATCH_ROWS must be at least 1, got {b}"); } o.batch_rows = b as usize; }
                "target_version" => { let t = number(name, v)?; if !(2016..=2030).contains(&t) { bail!("COPY option TARGET_VERSION: expected a SQL Server version such as 2019, got {t}"); } o.target_version = t as u16; }
                "sort_by" => o.sort_by = Some(v.clone()).filter(|s| !s.trim().is_empty()),
                "string_type" => o.string_type = StringType::parse(v)?,
                "table" => o.table = Some(v.trim().to_string()).filter(|s| !s.is_empty()),
                _ => bail!("unknown COPY option {} for FORMAT duckbridge (known: {}; ORDER_BY is DuckDB's own option, use SORT_BY for the bulk ORDER hint)", name.to_uppercase(), COPY_OPTIONS.join(", ").to_uppercase()),
            }
        }
        Ok(o)
    }

    /// DuckDB types COPY cannot send as such (nested): the column must be converted in the query.
    fn unsupported_type(duck_type: &str) -> bool {
        let t = duck_type.trim().to_ascii_uppercase();
        t.ends_with(']') || ["STRUCT", "MAP", "UNION", "VARIANT", "LIST"].iter().any(|p| t.starts_with(p))
    }

    /// A bound COPY: target, table, input columns (name, DuckDB type), options.
    pub struct CopyBind {
        pub url: DbUrl,
        pub schema: String,
        pub table: String,
        pub columns: Vec<(String, String)>,
        pub o: CopyOptions,
    }

    impl CopyBind {
        pub fn table_ref(&self) -> String { format!("{}.{}", bracket(&self.schema), bracket(&self.table)) }
        pub fn label(&self) -> String { format!("{}.{}.{}", self.url.database, self.schema, self.table) }
    }

    /// Parse `COPY ... TO '<path>' (FORMAT duckbridge, ...)`: path
    /// `mssql://user:pw@host:port/Database/schema.table[?params]` (the `TABLE` option overrides the
    /// last segment), the options, and the input columns (nested types are refused, naming the column).
    pub fn copy_bind(path: &str, opts: &[(String, String)], columns: Vec<(String, String)>) -> Result<CopyBind> {
        let o = copy_options(opts)?;
        let dsn = api::parse_dsn(path.trim()).context("COPY TO: bad mssql:// path")?;
        if dsn.scheme != "mssql" && dsn.scheme != "sqlserver" { bail!("COPY ... (FORMAT duckbridge) expects an mssql:// path, got {}://", dsn.scheme); }
        let db_part = scan::require_database(dsn.database.as_deref(), "COPY TO")?;
        let (database, table_part) = match db_part.split_once('/') { Some((d, t)) => (d.to_string(), Some(t.to_string())), None => (db_part.clone(), None) };
        let spec = o.table.clone().or(table_part.filter(|t| !t.is_empty()))
            .ok_or_else(|| anyhow!("COPY TO: name the table, mssql://user:password@host:port/{database}/schema.table, or give the TABLE option"))?;
        let (schema, table) = scan::parse_table_name(&spec).ok_or_else(|| anyhow!("COPY TO: {spec:?} is not a schema.table name"))?;
        if database.trim().is_empty() { bail!("COPY TO: the path must name the database"); }
        for (name, t) in &columns {
            if unsupported_type(t) { bail!("COPY TO {schema}.{table}: column {name} has type {t}, which has no SQL Server counterpart; convert it in the query (to_json({name}) or CAST({name} AS VARCHAR))"); }
        }
        if columns.is_empty() { bail!("COPY TO {schema}.{table}: the input has no columns"); }
        Ok(CopyBind { url: DbUrl { target: dsn.mssql_target(Some(&database)), database }, schema, table, columns, o })
    }

    /// An open COPY target: the converters (one per input column) and the bulk writer.
    pub struct CopyTarget {
        pub table_ref: String,
        pub names: Vec<String>,
        pub convs: Vec<Converter>,
        pub sink: BulkSink,
        pub created: bool,
    }

    /// Prepare the target of a bound COPY: drop it (`DROP_EXISTING`), create it when missing and
    /// `CREATE`, map the input columns onto the table's (by name, case-insensitively), build the
    /// converters and start the bulk insert. The database is never created.
    pub fn copy_open(b: &CopyBind) -> Result<CopyTarget> {
        use duckbridge::mssql::target::read_target_tables;
        duckbridge::core::typemap::set_mssql_version(b.o.target_version);
        let table_ref = b.table_ref();
        let label = b.label();
        let rt = mssql::runtime()?;
        let (target_cols, created) = rt.block_on(async {
            let mut c = b.url.target.connect(&b.url.database).await.with_context(|| format!("COPY TO {label}: connecting to database {} on {}:{}", b.url.database, b.url.target.host, b.url.target.port))?;
            let lit = mssql::nstr(&table_ref);
            if b.o.drop_existing {
                mssql::exec_batch_as(&mut c, "target", &label, &format!("IF OBJECT_ID({lit}, N'U') IS NOT NULL DROP TABLE {table_ref}")).await.with_context(|| format!("COPY TO {label}: dropping the table"))?;
            }
            let exists = c.simple_query(format!("SELECT CAST(CASE WHEN OBJECT_ID({lit}, N'U') IS NULL THEN 0 ELSE 1 END AS INT)")).await?.into_first_result().await?
                .first().and_then(|r| r.get::<i32, _>(0)) == Some(1);
            let mut created = false;
            if !exists {
                if !(b.o.create || b.o.drop_existing) { bail!("COPY TO {label}: table {}.{} does not exist in database {} (add CREATE true to create it)", b.schema, b.table, b.url.database); }
                let plan = bulk::create_table_ddl(&b.schema, &b.table, &b.columns, b.o.string_type);
                for w in &plan.warnings { duckbridge::core::runlog::note(&format!("COPY TO {label}: {w}")); }
                for stmt in &plan.ddl { mssql::exec_batch_as(&mut c, "target", &label, stmt).await.with_context(|| format!("COPY TO {label}: creating the table"))?; }
                created = true;
            }
            if duckbridge::core::runlog::planning() { return Ok((vec![], created)); }
            let t = read_target_tables(&mut c, &[(b.schema.clone(), b.table.clone())]).await?.remove(0);
            Ok::<_, anyhow::Error>((t.columns, created))
        })?;
        let mut names = Vec::with_capacity(b.columns.len());
        let mut convs = Vec::with_capacity(b.columns.len());
        for (name, duck_type) in &b.columns {
            let tc = target_cols.iter().find(|c| c.name.eq_ignore_ascii_case(name)).ok_or_else(|| anyhow!("COPY TO {label}: column {name} of the input is not a column of {}.{} (its columns: {})",
                b.schema, b.table, target_cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(", ")))?;
            if tc.is_computed { bail!("COPY TO {label}: column {} is computed and cannot be loaded", tc.name); }
            convs.push(Converter::for_target(tc, duck_type).with_context(|| format!("COPY TO {label}: column {name}"))?);
            names.push(tc.name.clone());
        }
        let o = BulkOptions { batch_rows: b.o.batch_rows, keep_identity: b.o.keep_identity, table_lock: b.o.table_lock, prefetch_batches: 4, order_by: b.o.sort_by.clone(), check_constraints: b.o.check_constraints };
        let sink = BulkSink::open(&b.url.target, &b.url.database, &table_ref, &names, &convs, &o).with_context(|| format!("COPY TO {label}"))?;
        Ok(CopyTarget { table_ref, names, convs, sink, created })
    }

    /// Convert one batch of COPY input for the target (`first_row` = rows before it).
    pub fn copy_rows(t_ref: &str, names: &[String], convs: &[Converter], batch: &RecordBatch, first_row: i64) -> Result<bulk::TdsRows> {
        bulk::rows_from_batch(batch, convs, first_row).map_err(|e| bulk::describe_read_error(e, t_ref, names, convs))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn v2_copy_path_and_options() {
            let bind_err = |r: Result<CopyBind>| match r { Err(e) => e.to_string(), Ok(_) => panic!("expected an error") };
            let opts = |v: &[(&str, &str)]| v.iter().map(|(a, b)| (a.to_string(), b.to_string())).collect::<Vec<_>>();
            let cols = vec![("id".to_string(), "BIGINT".to_string())];
            let b = copy_bind("mssql://sa:p%40ss@h:1433/Sales/dbo.kpi?encrypt=false", &opts(&[("CREATE", "true"), ("batch_rows", "500"), ("SORT_BY", "id")]), cols.clone()).unwrap();
            assert_eq!((b.url.database.as_str(), b.schema.as_str(), b.table.as_str(), b.url.target.password.as_str(), b.url.target.encrypt), ("Sales", "dbo", "kpi", "p@ss", false));
            assert!(b.o.create && b.o.batch_rows == 500 && b.o.sort_by.as_deref() == Some("id") && b.o.table_lock);
            assert_eq!(b.table_ref(), "[dbo].[kpi]");
            let b = copy_bind("mssql://h/Sales", &opts(&[("table", "[s].[a b]"), ("keep_identity", "true"), ("string_type", "auto")]), cols.clone()).unwrap();
            assert_eq!((b.schema.as_str(), b.table.as_str(), b.o.keep_identity, b.o.string_type), ("s", "a b", true, StringType::Auto));
            assert!(bind_err(copy_bind("mssql://h/Sales", &[], cols.clone())).contains("name the table"));
            assert!(bind_err(copy_bind("mssql://h/Sales/t", &opts(&[("order_by", "id")]), cols.clone())).contains("SORT_BY"));
            assert!(bind_err(copy_bind("postgres://h/db/t", &[], cols.clone())).contains("mssql://"));
            assert!(bind_err(copy_bind("mssql://h/Db/t", &opts(&[("create", "maybe")]), cols.clone())).contains("CREATE"));
            let e = bind_err(copy_bind("mssql://h/Db/t", &[], vec![("tags".into(), "VARCHAR[]".into())]));
            assert!(e.contains("column tags") && e.contains("to_json"), "{e}");
        }

        #[test]
        fn v2_run_script_and_scan_args() {
            let a = RunArgs { from: "tsql".into(), nocase_mode: "rewrite".into(), strict: false, mssql_math: false, types: String::new(), schema_map: String::new(), reset: false };
            let s = run_sql("SELECT TOP 1 ISNULL(a, 0) AS x INTO dbo.t FROM dbo.src; UPDATE dbo.t SET x = 1", &a).unwrap();
            assert!(s.starts_with("SET SESSION integer_division = true;\n"), "{s}");
            assert!(s.contains("main.t") && s.contains("UPDATE main.t SET x = 1") && s.ends_with(';') && !s.contains("RESET"), "{s}");
            let s = run_sql("UPDATE t SET x = 1", &RunArgs { reset: true, from: "duckdb".into(), ..a.clone() }).unwrap();
            assert!(!s.contains("SET SESSION") && s.ends_with("RESET default_collation;"), "{s}");
            let s = run_sql("SELECT 1; INSERT INTO t VALUES (1); WITH c AS (SELECT 2) SELECT * FROM c", &RunArgs { reset: true, ..a.clone() }).unwrap();
            assert!(s.contains("-- skipped, only the last statement's rows are returned: SELECT 1;\nINSERT INTO t VALUES (1);\nCREATE OR REPLACE TEMP TABLE __bridge_run_result AS WITH"), "{s}");
            assert!(s.ends_with("RESET default_collation;\nSELECT * FROM __bridge_run_result;"), "{s}");
            assert!(!returns_rows("WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c") && returns_rows("FROM t") && !returns_rows("CREATE TABLE t AS SELECT 1"));
            assert!(run_sql("SELECT 1", &RunArgs { from: "oracle".into(), ..a.clone() }).is_err());
            assert!(run_sql("  ", &a).unwrap_err().to_string().contains("no statements"));
            let st = run_settings(&a).unwrap();
            assert_eq!(st[0], ("integer_division".to_string(), "true".to_string()));
            assert!(st.iter().any(|(k, v)| k == "TimeZone" && v == "UTC"), "{st:?}");
            assert!(run_settings(&RunArgs { nocase_mode: "global".into(), ..a.clone() }).unwrap().iter().any(|(k, v)| k == "default_collation" && v == "nocase"));
            assert!(run_settings(&RunArgs { from: "duckdb".into(), ..a.clone() }).unwrap().is_empty());
            let d = ScanArgs::default();
            assert_eq!(d.sql(), "");
            assert_eq!(ScanArgs { nolock: true, isolation: "snapshot".into(), lock_timeout_ms: -1, ..d.clone() }.sql(), ", nolock := true, lock_timeout := -1, isolation := 'snapshot'");
            assert_eq!(ScanArgs { nolock: true, ..d.clone() }.safety().unwrap().isolation, Isolation::ReadUncommitted);
            assert!(ScanArgs { isolation: "dirty".into(), ..d }.safety().is_err());
        }
    }
}
