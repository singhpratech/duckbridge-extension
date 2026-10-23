//! duckdb-neo glue (feature `neo`, DuckDB 2.0 v2 C extension API).
//!
//! The v2 extension API gives an extension no connection to the database that loaded it
//! (`duckdb_v2_extension_input` carries an extension handle, a borrowed context and an error slot;
//! see docs/handoff-neo.md), so everything here is a function DuckDB calls, and DuckDB does the
//! reading and writing of its own tables (docs/plan-2.0-extension.md):
//!
//! * `bridge_scan(dsn, source, nolock, batch_rows, lock_timeout, isolation, readonly)`: table
//!   function streaming one SQL Server table or query (`core::mssql::scan`). Bind connects and
//!   describes the source (result columns, cardinality), init starts the `RowStream` (one thread),
//!   exec writes one batch slice per call with `core::duck::write_column`.
//! * `bridge_tables(dsn, tables, exclude, schema_map, nolock)`: the catalog, one row per table/view
//!   with the `CREATE TABLE ... AS FROM bridge_scan(..)` for each table.
//! * `PRAGMA bridge_import(dsn, ...)` / `PRAGMA bridge_run(sql, ...)`: query pragmas returning a
//!   script (several statements) that DuckDB runs in place of the PRAGMA. Pragma arguments are
//!   written `name = value` (DuckDB's PRAGMA grammar has no `:=`).
//! * `COPY ... TO 'mssql://user:pw@host:port/Database/schema.table' (FORMAT duckbridge, ...)`:
//!   copy function; bind checks path, options and column types, init prepares the table and opens a
//!   `BulkSink`, batch exports the input with `ArrowExporter` and converts it (`bulk::rows_from_batch`),
//!   flush sends, finalize commits and checks the row count.
//! * `bridge_transpile` (both overloads) and `bridge_prelude`, unchanged.
//! * `bridge_import` / `bridge_export` / `bridge_run` as table functions (the 1.x call shapes) fail at
//!   bind with the 2.0 replacement ([`use_instead`]).
//!
//! Table-function bind receives the arguments without their names: positional ones first, then the
//! named ones in declaration order with their defaults filled in (`bind` checks the count).
//! The backend-neutral work (URLs, options, scripts, the COPY target) is in `lib.rs` (`v2`).

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

use duckbridge::core::duck::{self as duck, Kind};
use duckbridge::mssql::bulk::{BulkSink, TdsRows};
use duckbridge::mssql::convert::Converter;
use duckbridge::mssql::scan::{Batch, RowStream, ScanColumn};
use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::arrow::ArrowExporter;
use duckdb_neo::bind_arguments::{BindArgument, BindArguments};
use duckdb_neo::column_data_collection::ColumnDataCollection;
use duckdb_neo::connection::{Context, SettingScope};
use duckdb_neo::copy_function::{CopyFunctionBuilder, CopyToBindInfo, CopyToFunctionCallbacks};
use duckdb_neo::data_chunk::{DataChunkRef, VectorCollection};
use duckdb_neo::extension::Extension;
use duckdb_neo::logical_type::LogicalType;
use duckdb_neo::pragma_function::{PragmaFunctionBuilder, PragmaQueryCallbacks};
use duckdb_neo::scalar::{ScalarCallbacks, ScalarFunctionBuilder};
use duckdb_neo::signature::{FunctionParameter, FunctionParameterKind, SignatureBuilder};
use duckdb_neo::table_function::{BindFunctionHandle, ExecColumnInfo, InitColumnData, TableFunctionBuilder, TableFunctionCallbacks};
use duckdb_neo::types::Any;
use duckdb_neo::value::Value;
use duckdb_neo::vector::{Unknown, Vector};
use duckdb_neo::{DuckDBType, Error, Result};

use crate::v2::{self, CopyBind, ImportArgs, RunArgs, ScanArgs, ScanBind, TableRow, SCAN_NAMED};
use crate::{prelude_text, transpile_text, Ty, VECTOR_SIZE};

fn logical(ext: &Extension, t: Ty) -> Result<duckdb_neo::logical_type::LogicalType> {
    match t { Ty::Varchar => String::logical_type(ext), Ty::Bigint => i64::logical_type(ext), Ty::Boolean => bool::logical_type(ext) }
}

fn err(e: impl std::fmt::Display) -> Error { Error::invalid_input(e.to_string()) }
/// An anyhow error with its context chain, as one message.
fn aerr(e: anyhow::Error) -> Error { Error::invalid_input(format!("{e:#}")) }

fn named_param<T: DuckDBType + duckdb_neo::ToValue + 'static>(name: &str, default: T) -> FunctionParameter {
    FunctionParameter::with_default(name, default).kind(FunctionParameterKind::Named)
}

// ------------------------------------------------------------------ arguments

/// A bind argument as text / BIGINT / BOOLEAN (`None` for NULL); `what` names it in errors.
fn arg_text(a: &BindArgument, what: &str) -> Result<Option<String>> {
    let v = a.value.as_ref().ok_or_else(|| err(format!("{what} must be a constant")))?;
    v.get::<String>()
}
fn arg_int(a: &BindArgument, what: &str) -> Result<Option<i64>> {
    let v = a.value.as_ref().ok_or_else(|| err(format!("{what} must be a constant")))?;
    v.get::<i64>()
}
fn arg_bool(a: &BindArgument, what: &str) -> Result<Option<bool>> {
    let v = a.value.as_ref().ok_or_else(|| err(format!("{what} must be a constant")))?;
    v.get::<bool>()
}

/// Pragma arguments by name (DuckDB fills in the declared defaults; NULL reads as the default).
struct PArgs<'a>(BindArguments<'a>);

impl PArgs<'_> {
    fn value(&self, name: &str) -> Result<Option<Value>> {
        match self.0.index(name)? { Some(i) => self.0.value(i), None => Ok(None) }
    }
    fn text(&self, name: &str, default: &str) -> Result<String> {
        Ok(self.value(name)?.map(|v| v.get::<String>()).transpose()?.flatten().unwrap_or_else(|| default.to_string()))
    }
    fn int(&self, name: &str, default: i64) -> Result<i64> {
        Ok(self.value(name)?.map(|v| v.get::<i64>()).transpose()?.flatten().unwrap_or(default))
    }
    fn boolean(&self, name: &str, default: bool) -> Result<bool> {
        Ok(self.value(name)?.map(|v| v.get::<bool>()).transpose()?.flatten().unwrap_or(default))
    }
    fn positional(&self, i: usize, what: &str) -> Result<String> {
        self.0.value(i)?.map(|v| v.get::<String>()).transpose()?.flatten().ok_or_else(|| err(format!("{what} is required")))
    }
    fn scan(&self) -> Result<ScanArgs> {
        let d = ScanArgs::default();
        Ok(ScanArgs {
            nolock: self.boolean("nolock", d.nolock)?,
            batch_rows: self.int("batch_rows", d.batch_rows)?,
            lock_timeout_ms: self.int("lock_timeout", d.lock_timeout_ms)?,
            isolation: self.text("isolation", &d.isolation)?,
            readonly: self.boolean("readonly", d.readonly)?,
        })
    }
}

/// The scan options as named parameters (shared by bridge_scan, bridge_tables, PRAGMA bridge_import).
fn scan_params(sig: SignatureBuilder) -> SignatureBuilder {
    let d = ScanArgs::default();
    sig.param(named_param("nolock", d.nolock))
        .param(named_param("batch_rows", d.batch_rows))
        .param(named_param("lock_timeout", d.lock_timeout_ms))
        .param(named_param("isolation", d.isolation))
        .param(named_param("readonly", d.readonly))
}

/// [`SCAN_NAMED`] from bind arguments `a` (in declaration order).
fn scan_args(a: &[BindArgument], what: &str) -> Result<ScanArgs> {
    if a.len() != SCAN_NAMED.len() { return Err(err(format!("{what}: unexpected argument layout ({} named arguments, {} expected)", a.len(), SCAN_NAMED.len()))); }
    let d = ScanArgs::default();
    Ok(ScanArgs {
        nolock: arg_bool(&a[0], "nolock")?.unwrap_or(d.nolock),
        batch_rows: arg_int(&a[1], "batch_rows")?.unwrap_or(d.batch_rows),
        lock_timeout_ms: arg_int(&a[2], "lock_timeout")?.unwrap_or(d.lock_timeout_ms),
        isolation: arg_text(&a[3], "isolation")?.unwrap_or(d.isolation),
        readonly: arg_bool(&a[4], "readonly")?.unwrap_or(d.readonly),
    })
}

// ------------------------------------------------------------------ bridge_scan

/// `bridge_scan(dsn, source, ...)`.
struct ScanFn;

struct ScanBound {
    bind: ScanBind,
    kinds: Vec<Kind>,
}

/// The running read and the batch being handed out.
struct ScanState {
    stream: RowStream,
    pending: Batch,
    pos: usize,
    done: bool,
}

impl TableFunctionCallbacks for ScanFn {
    type BindData = ScanBound;
    type LocalState = ();
    type GlobalState = Mutex<ScanState>;

    fn bind(&self, ctx: &Context, a: Vec<BindArgument>, b: BindFunctionHandle<'_>) -> Result<ScanBound> {
        if a.len() < 2 { return Err(err("bridge_scan(dsn, source): two arguments expected")); }
        let dsn = arg_text(&a[0], "bridge_scan dsn")?.ok_or_else(|| err("bridge_scan: dsn is NULL"))?;
        let source = arg_text(&a[1], "bridge_scan source")?.ok_or_else(|| err("bridge_scan: source is NULL"))?;
        let args = scan_args(&a[2..], "bridge_scan")?;
        let bind = v2::scan_bind(&dsn, &source, &args).map_err(aerr)?;
        let mut kinds = Vec::with_capacity(bind.plan.columns.len());
        for c in &bind.plan.columns {
            let lt = LogicalType::from_text(ctx, &c.duck_type).or_else(|_| String::logical_type(ctx))?;
            let k = Kind::of_type(&lt).ok_or_else(|| err(format!("bridge_scan {source}: column {} ({}) has DuckDB type {}, which bridge_scan cannot produce", c.name, c.sql_type, c.duck_type)))?;
            b.add_result_column(&c.name, &lt)?;
            kinds.push(k);
        }
        if let Some(n) = bind.plan.rows { b.set_cardinality(n.max(0) as usize, false)?; }
        Ok(ScanBound { bind, kinds })
    }

    fn init_global_state(&self, bd: &ScanBound, _: &Context, _: InitColumnData<'_>) -> Result<(Option<Mutex<ScanState>>, Option<usize>)> {
        let b = &bd.bind;
        let (cols, stream) = RowStream::start(&b.url.target, &b.url.database, &b.safety, &b.plan.sql, b.batch_rows).map_err(|e| aerr(e.context(format!("bridge_scan {}", b.source))))?;
        check_same_columns(&b.plan.columns, &cols).map_err(|m| err(format!("bridge_scan {}: the source changed since the query was bound: {m}", b.source)))?;
        Ok((Some(Mutex::new(ScanState { stream, pending: vec![], pos: 0, done: false })), Some(1)))
    }

    fn exec(&self, bd: &ScanBound, g: Option<&Mutex<ScanState>>, _: Option<&mut ()>, _: &Context, mut out: DataChunkRef<'_>, _: ExecColumnInfo<'_>) -> Result<()> {
        let mut st = g.ok_or_else(|| err("bridge_scan: no scan state"))?.lock().map_err(|_| err("bridge_scan: state poisoned"))?;
        while !st.done && st.pos >= st.pending.len() {
            match st.stream.next_batch().map_err(|e| aerr(e.context(format!("bridge_scan {}", bd.bind.source))))? {
                Some(batch) => { st.pending = batch; st.pos = 0; }
                None => { st.done = true; st.pending = vec![]; st.pos = 0; }
            }
        }
        let n = (st.pending.len() - st.pos).min(VECTOR_SIZE);
        if n == 0 { return Ok(()); } // an empty chunk ends the scan
        let rows = &st.pending[st.pos..st.pos + n];
        for (c, kind) in bd.kinds.iter().enumerate() {
            duck::write_column(&mut out, c, *kind, rows)
                .map_err(|e| err(format!("bridge_scan {}: column {}: {e:#}", bd.bind.source, bd.bind.plan.columns[c].name)))?;
        }
        st.pos += n;
        if st.pos >= st.pending.len() { st.pending = vec![]; st.pos = 0; }
        Ok(())
    }
}

/// Same names and types, or a message naming the first difference.
fn check_same_columns(bound: &[ScanColumn], now: &[ScanColumn]) -> std::result::Result<(), String> {
    if bound.len() != now.len() { return Err(format!("{} columns at bind, {} now", bound.len(), now.len())); }
    for (a, b) in bound.iter().zip(now) {
        if a.name != b.name || a.duck_type != b.duck_type { return Err(format!("column {} ({}) is now {} ({})", a.name, a.duck_type, b.name, b.duck_type)); }
    }
    Ok(())
}

fn register_scan(ext: &Extension) -> Result<()> {
    let sig = scan_params(SignatureBuilder::new().param(FunctionParameter::new::<String>("dsn")).param(FunctionParameter::new::<String>("source")));
    TableFunctionBuilder::new("bridge_scan", sig, ScanFn).register(ext)
}

// ------------------------------------------------------------------ bridge_tables

/// `bridge_tables(dsn, tables, exclude, schema_map, <scan options>)`.
struct TablesFn;

struct TablesBound {
    dsn: String,
    tables: String,
    exclude: String,
    schema_map: String,
    scan: ScanArgs,
}

const TABLE_COLUMNS: &[(&str, Ty)] = &[("schema", Ty::Varchar), ("name", Ty::Varchar), ("kind", Ty::Varchar), ("rows", Ty::Bigint), ("columns", Ty::Bigint), ("pk", Ty::Varchar), ("import_sql", Ty::Varchar)];

impl TableFunctionCallbacks for TablesFn {
    type BindData = TablesBound;
    type LocalState = ();
    type GlobalState = Mutex<(Vec<TableRow>, usize)>;

    fn bind(&self, ctx: &Context, a: Vec<BindArgument>, b: BindFunctionHandle<'_>) -> Result<TablesBound> {
        if a.len() != 4 + SCAN_NAMED.len() { return Err(err(format!("bridge_tables: unexpected argument layout ({} arguments)", a.len()))); }
        for (name, t) in TABLE_COLUMNS {
            b.add_result_column(name, &match t { Ty::Varchar => String::logical_type(ctx)?, Ty::Bigint => i64::logical_type(ctx)?, Ty::Boolean => bool::logical_type(ctx)? })?;
        }
        Ok(TablesBound {
            dsn: arg_text(&a[0], "bridge_tables dsn")?.ok_or_else(|| err("bridge_tables: dsn is NULL"))?,
            tables: arg_text(&a[1], "tables")?.unwrap_or_default(),
            exclude: arg_text(&a[2], "exclude")?.unwrap_or_default(),
            schema_map: arg_text(&a[3], "schema_map")?.unwrap_or_default(),
            scan: scan_args(&a[4..], "bridge_tables")?,
        })
    }

    fn init_global_state(&self, b: &TablesBound, _: &Context, _: InitColumnData<'_>) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        let rows = v2::tables_rows(&b.dsn, &b.tables, &b.exclude, &b.schema_map, &b.scan).map_err(aerr)?;
        Ok((Some(Mutex::new((rows, 0))), Some(1)))
    }

    fn exec(&self, _: &TablesBound, g: Option<&Self::GlobalState>, _: Option<&mut ()>, _: &Context, mut out: DataChunkRef<'_>, _: ExecColumnInfo<'_>) -> Result<()> {
        let mut st = g.ok_or_else(|| err("bridge_tables: no state"))?.lock().map_err(|_| err("bridge_tables: state poisoned"))?;
        let start = st.1.min(st.0.len());
        let end = (start + VECTOR_SIZE).min(st.0.len());
        let page = &st.0[start..end];
        let n = page.len();
        macro_rules! column {
            ($c:expr, $t:ty, $r:ident => $e:expr) => {{
                let mut v = out.get_vector_at_mut::<$t>($c)?;
                v.set_size(n)?;
                for (i, $r) in page.iter().enumerate() { v.write(i, Some($e))?; }
            }};
        }
        column!(0, String, r => r.schema.as_str());
        column!(1, String, r => r.name.as_str());
        column!(2, String, r => r.kind);
        column!(3, i64, r => r.rows);
        column!(4, i64, r => r.columns);
        column!(5, String, r => r.pk.as_str());
        column!(6, String, r => r.import_sql.as_str());
        st.1 = end;
        Ok(())
    }
}

fn register_tables(ext: &Extension) -> Result<()> {
    let sig = SignatureBuilder::new()
        .param(FunctionParameter::new::<String>("dsn"))
        .param(named_param("tables", String::new()))
        .param(named_param("exclude", String::new()))
        .param(named_param("schema_map", String::new()));
    TableFunctionBuilder::new("bridge_tables", scan_params(sig), TablesFn).register(ext)
}

// ------------------------------------------------------------------ PRAGMA bridge_import / bridge_run

/// `PRAGMA bridge_import(dsn, tables = .., exclude = .., schema_map = .., keys = true,
/// keys_after_load = false, overwrite = false, views = true, schema_only = false, <scan options>)`.
struct ImportPragma;

impl PragmaQueryCallbacks for ImportPragma {
    fn query(&self, _: &Context, a: BindArguments<'_>) -> Result<String> {
        let p = PArgs(a);
        let dsn = p.positional(0, "bridge_import dsn")?;
        let args = ImportArgs {
            tables: p.text("tables", "")?, exclude: p.text("exclude", "")?, schema_map: p.text("schema_map", "")?,
            keys: p.boolean("keys", true)?, keys_after_load: p.boolean("keys_after_load", false)?, overwrite: p.boolean("overwrite", false)?,
            views: p.boolean("views", true)?, schema_only: p.boolean("schema_only", false)?, scan: p.scan()?,
        };
        v2::import_sql(&dsn, &args).map_err(aerr)
    }
}

/// `PRAGMA bridge_run(sql, dialect = 'tsql', nocase_mode = 'rewrite', strict = false, mssql_math = false,
/// types = '', schema_map = '', reset = false)`.
struct RunPragma;

impl PragmaQueryCallbacks for RunPragma {
    fn query(&self, ctx: &Context, a: BindArguments<'_>) -> Result<String> {
        let p = PArgs(a);
        let sql = p.positional(0, "bridge_run sql")?;
        let args = RunArgs {
            from: p.text("dialect", "tsql")?, nocase_mode: p.text("nocase_mode", "rewrite")?, strict: p.boolean("strict", false)?, mssql_math: p.boolean("mssql_math", false)?,
            types: p.text("types", "")?, schema_map: p.text("schema_map", "")?, reset: p.boolean("reset", false)?,
        };
        let script = v2::run_sql(&sql, &args).map_err(aerr)?;
        // before DuckDB parses the script: integer_division is a parse-time setting (see v2::run_settings)
        let config = ctx.config()?;
        for (k, v) in v2::run_settings(&args).map_err(aerr)? {
            config.set_text(&k, &v, SettingScope::Session).map_err(|e| err(format!("bridge_run: SET {k} = {v}: {e}")))?;
        }
        Ok(script)
    }
}

fn register_pragmas(ext: &Extension) -> Result<()> {
    let sig = SignatureBuilder::new()
        .param(FunctionParameter::new::<String>("dsn"))
        .param(named_param("tables", String::new()))
        .param(named_param("exclude", String::new()))
        .param(named_param("schema_map", String::new()))
        .param(named_param("keys", true))
        .param(named_param("keys_after_load", false))
        .param(named_param("overwrite", false))
        .param(named_param("views", true))
        .param(named_param("schema_only", false));
    PragmaFunctionBuilder::query("bridge_import", scan_params(sig), ImportPragma).register(ext)?;
    let sig = SignatureBuilder::new()
        .param(FunctionParameter::new::<String>("sql"))
        // `from` is a reserved word in PRAGMA arguments: the source dialect is `dialect`
        .param(named_param("dialect", "tsql".to_string()))
        .param(named_param("nocase_mode", "rewrite".to_string()))
        .param(named_param("strict", false))
        .param(named_param("mssql_math", false))
        .param(named_param("types", String::new()))
        .param(named_param("schema_map", String::new()))
        .param(named_param("reset", false));
    PragmaFunctionBuilder::query("bridge_run", sig, RunPragma).register(ext)
}

// ------------------------------------------------------------------ COPY ... TO 'mssql://...' (FORMAT duckbridge)

/// The `duckbridge` COPY format.
struct CopyFn;

/// One open COPY: converters, the bulk writer, rows converted so far.
struct CopyState {
    table_ref: String,
    label: String,
    names: Vec<String>,
    convs: Vec<Converter>,
    sink: Mutex<Option<BulkSink>>,
    rows: AtomicI64,
}

impl CopyToFunctionCallbacks for CopyFn {
    type InitData = CopyState;
    type BindData = CopyBind;
    type BatchData = TdsRows;

    fn bind(&self, _: &Context, bi: &CopyToBindInfo) -> Result<CopyBind> {
        let mut columns = Vec::with_capacity(bi.len()?);
        for i in 0..bi.len()? {
            let (name, t) = bi.get_column(i)?;
            columns.push((name.to_string(), t.to_text()?));
        }
        let mut opts = Vec::with_capacity(bi.option_count()?);
        for i in 0..bi.option_count()? {
            let (name, v) = bi.get_option(i)?;
            let text = match v.get::<String>() { Ok(Some(s)) => s, _ => v.dbg_string()? };
            opts.push((name.to_string(), text));
        }
        v2::copy_bind(&bi.file_path()?, &opts, columns).map_err(aerr)
    }

    fn batch_size(&self, _: &Context, b: &CopyBind) -> Result<Option<std::num::NonZeroUsize>> {
        // DuckDB batches of up to BATCH_ROWS (it rounds to its own row-group sizes); each one is
        // converted in `batch` (possibly in parallel) and sent in `flush`
        Ok(std::num::NonZeroUsize::new(b.o.batch_rows.clamp(VECTOR_SIZE, 122_880)))
    }

    fn init(&self, _: &Context, b: &CopyBind, _path: &str) -> Result<CopyState> {
        let t = v2::copy_open(b).map_err(aerr)?;
        Ok(CopyState { table_ref: t.table_ref, label: b.label(), names: t.names, convs: t.convs, sink: Mutex::new(Some(t.sink)), rows: AtomicI64::new(0) })
    }

    fn batch<'c>(&self, ctx: &'c Context, b: &CopyBind, st: &CopyState, input: ColumnDataCollection<'c>) -> Result<TdsRows> {
        let n = input.len()?;
        if n == 0 { return Ok(vec![]); }
        let types: Vec<LogicalType> = input.logical_types().to_vec();
        let columns: Vec<(&str, LogicalType)> = b.columns.iter().map(|(name, _)| name.as_str()).zip(types).collect();
        let mut exporter = ArrowExporter::new(ctx, &columns, None)?;
        let mut scan = input.to_scan()?;
        while let Some(chunk) = scan.next_chunk()? { exporter.append(&chunk, false)?; }
        exporter.flush()?;
        let mut arrays = vec![];
        while let Some(a) = exporter.next_array()? { arrays.push(a); }
        let batches = duck::import_arrow(exporter.schema()?, arrays).map_err(|e| aerr(e.context(format!("COPY TO {}", st.label))))?;
        let first = st.rows.fetch_add(n as i64, Ordering::SeqCst);
        let mut out: TdsRows = Vec::with_capacity(n);
        let mut done = 0i64;
        for rb in &batches {
            out.extend(v2::copy_rows(&st.table_ref, &st.names, &st.convs, rb, first + done).map_err(|e| aerr(e.context(format!("COPY TO {}", st.label))))?);
            done += rb.num_rows() as i64;
        }
        if out.len() != n { return Err(err(format!("COPY TO {}: {n} rows in the batch but {} converted", st.label, out.len()))); }
        Ok(out)
    }

    fn flush(&self, _: &Context, _: &CopyBind, st: &CopyState, rows: &mut TdsRows) -> Result<()> {
        let mut sink = st.sink.lock().map_err(|_| err("COPY TO: state poisoned"))?;
        let s = sink.as_mut().ok_or_else(|| err(format!("COPY TO {}: the bulk writer already stopped", st.label)))?;
        s.send(std::mem::take(rows)).map_err(|e| { let msg = format!("COPY TO {}: {e:#}", st.label); err(msg) })
    }

    fn finalize(&self, _: &Context, _: &CopyBind, st: &CopyState) -> Result<()> {
        let sink = st.sink.lock().map_err(|_| err("COPY TO: state poisoned"))?.take().ok_or_else(|| err(format!("COPY TO {}: finalized twice", st.label)))?;
        let sent = sink.finish().map_err(|e| aerr(e.context(format!("COPY TO {}", st.label))))?;
        let read = st.rows.load(Ordering::SeqCst);
        if sent != read && !duckbridge::core::runlog::planning() { return Err(err(format!("COPY TO {}: {read} rows read from DuckDB but {sent} sent", st.label))); }
        Ok(())
    }
}

// ------------------------------------------------------------------ bridge_import / bridge_export / bridge_run (1.x shapes)

/// What to call on DuckDB 2.0 instead of the 1.x table function `name`.
fn use_instead(name: &str) -> &'static str {
    match name {
        "bridge_import" => "on DuckDB 2.0 use PRAGMA bridge_import('mssql://user:password@host:port/Database', tables = 'dbo.*'), or CREATE TABLE t AS FROM bridge_scan('mssql://...', 'dbo.t') for one table",
        "bridge_export" => "on DuckDB 2.0 use COPY <table or (query)> TO 'mssql://user:password@host:port/Database/schema.table' (FORMAT duckbridge, CREATE true), one table per COPY; .bak, parquet, postgres and script targets need the duckbridge CLI",
        _ => "on DuckDB 2.0 use PRAGMA bridge_run('<T-SQL script>')",
    }
}

/// `bridge_import` / `bridge_export` / `bridge_run` as table functions: bind fails with [`use_instead`].
struct Unavailable(&'static str);

impl TableFunctionCallbacks for Unavailable {
    type BindData = ();
    type LocalState = ();
    type GlobalState = ();

    fn bind(&self, _context: &Context, _arguments: Vec<BindArgument>, _bind: BindFunctionHandle<'_>) -> Result<()> {
        Err(Error::invalid_input(format!("{}(...) is a DuckDB 1.x table function (the v2 extension API gives an extension no connection to this database): {}", self.0, use_instead(self.0))))
    }

    fn exec(&self, _: &(), _: Option<&()>, _: Option<&mut ()>, _: &Context, _output: DataChunkRef<'_>, _: ExecColumnInfo<'_>) -> Result<()> {
        Ok(()) // never reached: bind fails
    }
}

fn register_unavailable(ext: &Extension, name: &'static str) -> Result<()> {
    // one VARCHAR plus any named parameters: the v1 call shapes all reach bind
    let sig = SignatureBuilder::new()
        .param(FunctionParameter::new::<String>("target"))
        .param(FunctionParameter::new::<Any>("options").kind(FunctionParameterKind::NamedVariadic));
    TableFunctionBuilder::new(name, sig, Unavailable(name)).register(ext)
}

// ------------------------------------------------------------------ bridge_transpile

/// `bridge_transpile(from, to, sql [, target_version])`.
struct TranspileScalar;

fn strings(v: &Vector<'_, String>, n: usize) -> Result<Vec<Option<String>>> {
    (0..n).map(|i| v.get(i).map(|s| s.map(str::to_owned))).collect()
}

impl ScalarCallbacks for TranspileScalar {
    type BindData = ();
    type InitData = ();

    fn exec(&self, _: Option<&()>, _: Option<&mut ()>, _: &Context, input: &mut VectorCollection, output: Vector<'_, Unknown>) -> Result<()> {
        let n = input.row_count()?;
        let from = strings(input.get_vector_at::<String>(0)?, n)?;
        let to = strings(input.get_vector_at::<String>(1)?, n)?;
        let sql = strings(input.get_vector_at::<String>(2)?, n)?;
        let version: Vec<Option<i64>> = if input.col_count() > 3 {
            let v = input.get_vector_at::<i64>(3)?;
            (0..n).map(|i| v.get(i).map(|x| x.copied())).collect::<Result<_>>()?
        } else { vec![None; n] };
        let mut out = output.cast::<String>()?;
        out.set_size(n)?;
        for i in 0..n {
            let (Some(f), Some(t), Some(s)) = (&from[i], &to[i], &sql[i]) else { out.write(i, None)?; continue; };
            let text = transpile_text(f, t, s, version[i]).map_err(err)?;
            out.write(i, Some(text.as_str()))?;
        }
        Ok(())
    }
}

// ------------------------------------------------------------------ bridge_prelude

/// `bridge_prelude(nocase)`: the SET statements that make a DuckDB session behave like SQL Server.
struct PreludeScalar;

impl ScalarCallbacks for PreludeScalar {
    type BindData = ();
    type InitData = ();

    fn exec(&self, _: Option<&()>, _: Option<&mut ()>, _: &Context, input: &mut VectorCollection, output: Vector<'_, Unknown>) -> Result<()> {
        let n = input.row_count()?;
        let v = input.get_vector_at::<bool>(0)?;
        let mut out = output.cast::<String>()?;
        out.set_size(n)?;
        for i in 0..n {
            let nocase = v.get(i)?.copied().unwrap_or(true);
            out.write(i, Some(prelude_text(nocase).as_str()))?;
        }
        Ok(())
    }
}

// ------------------------------------------------------------------ entrypoint

#[duckdb_entrypoint_c_api_v2(extension_name = "duckbridge", api_version = "v2.0.0")]
pub fn entrypoint(ext: &Extension) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let varchar = || logical(ext, Ty::Varchar);
    ScalarFunctionBuilder::new(
        "bridge_transpile",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("from"))
            .param(FunctionParameter::new::<String>("to"))
            .param(FunctionParameter::new::<String>("sql"))
            .return_type(varchar()?),
        TranspileScalar,
    )
    .register(ext)?;
    ScalarFunctionBuilder::new(
        "bridge_transpile",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("from"))
            .param(FunctionParameter::new::<String>("to"))
            .param(FunctionParameter::new::<String>("sql"))
            .param(FunctionParameter::with_type("target_version", logical(ext, Ty::Bigint)?))
            .return_type(varchar()?),
        TranspileScalar,
    )
    .register(ext)?;
    ScalarFunctionBuilder::new(
        "bridge_prelude",
        SignatureBuilder::new().param(FunctionParameter::with_type("nocase", logical(ext, Ty::Boolean)?)).return_type(varchar()?),
        PreludeScalar,
    )
    .register(ext)?;
    for name in ["bridge_import", "bridge_export", "bridge_run"] { register_unavailable(ext, name)?; }
    register_scan(ext)?;
    register_tables(ext)?;
    register_pragmas(ext)?;
    CopyFunctionBuilder::new("duckbridge", CopyFn).register(ext)?;
    Ok(())
}
