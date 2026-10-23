//! duckdb-rs glue (backend `duckdb-v1`, DuckDB 1.x C API).
//!
//! Everything in this file is specific to the duckdb-rs bindings: table functions (`VTab`),
//! scalar functions (`VScalar`), reading/writing DuckDB vectors, the [`Params`] impl over
//! `BindInfo`, and the C-API entry point. The work itself is done by the backend-neutral
//! functions in `lib.rs`; a `duckdb-neo` backend replaces this file with its own glue.

use std::error::Error;

use duckbridge::api::Row;
use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::ffi;
use duckdb::types::DuckString;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::WritableVector;
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use duckdb::Connection;

use crate::{
    export_bind, export_run, import_bind, import_run, install_pool, prelude_text, run_bind, run_run, transpile_text, ExportBind,
    ImportBind, Paged, Params, RunBind, Ty, EXPORT_PARAMS, IMPORT_PARAMS, RESULT_COLUMNS, RUN_PARAMS,
};

/// DuckDB release this build is pinned to (unstable C API). DuckDB's extension-ci-tools pass the
/// target version in `DUCKDB_EXTENSION_MIN_DUCKDB_VERSION`; the local Makefile build uses the default.
const DUCKDB_VERSION: &str = match option_env!("DUCKDB_EXTENSION_MIN_DUCKDB_VERSION") { Some(v) => v, None => "v1.5.6" };

// ------------------------------------------------------------------ types, parameters, output

fn logical(t: Ty) -> LogicalTypeHandle {
    LogicalTypeHandle::from(match t { Ty::Varchar => LogicalTypeId::Varchar, Ty::Bigint => LogicalTypeId::Bigint, Ty::Boolean => LogicalTypeId::Boolean })
}
fn varchar() -> LogicalTypeHandle { logical(Ty::Varchar) }
fn bigint() -> LogicalTypeHandle { logical(Ty::Bigint) }
fn boolean() -> LogicalTypeHandle { logical(Ty::Boolean) }
fn named(items: &[(&str, Ty)]) -> Option<Vec<(String, LogicalTypeHandle)>> {
    Some(items.iter().map(|(n, t)| (n.to_string(), logical(*t))).collect())
}

fn add_result_columns(bind: &BindInfo) {
    for (name, t) in RESULT_COLUMNS { bind.add_result_column(name, logical(*t)); }
}

/// `Params` over duckdb-rs' bind info.
struct Bind<'a>(&'a BindInfo);

impl Params for Bind<'_> {
    fn positional(&self, i: usize) -> String { self.0.get_parameter(i as u64).to_string() }
    fn given(&self, name: &str) -> bool { self.0.get_named_parameter(name).is_some() }
    fn text(&self, name: &str) -> Option<String> {
        self.0.get_named_parameter(name).filter(|v| !v.is_null()).map(|v| v.to_string())
    }
    fn int(&self, name: &str) -> Option<i64> {
        self.0.get_named_parameter(name).filter(|v| !v.is_null()).map(|v| v.to_int64())
    }
    fn boolean(&self, name: &str) -> Option<bool> {
        self.0.get_named_parameter(name).filter(|v| !v.is_null()).map(|v| v.to_bool())
    }
}

/// Write one page of result rows (object, kind, rows, note) into `output`.
fn write_rows(output: &mut DataChunkHandle, rows: &[Row]) {
    let n = rows.len();
    if n > 0 {
        let object = output.flat_vector(0);
        let kind = output.flat_vector(1);
        let mut count = output.flat_vector(2);
        let note = output.flat_vector(3);
        let counts = unsafe { count.as_mut_slice::<i64>() };
        for (i, r) in rows.iter().enumerate() {
            object.insert(i, r.object.as_str());
            kind.insert(i, r.kind.as_str());
            counts[i] = r.rows;
            note.insert(i, r.note.as_str());
        }
    }
    output.set_len(n);
}

// ------------------------------------------------------------------ bridge_import

struct ImportVTab;

impl VTab for ImportVTab {
    type InitData = Paged;
    type BindData = ImportBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        add_result_columns(bind);
        Ok(import_bind(&Bind(bind)))
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> { Ok(Paged::new()) }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let b = func.get_bind_data();
        func.get_init_data().next_page(|| import_run(b), |rows| write_rows(output, rows))
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> { Some(vec![varchar()]) }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> { named(IMPORT_PARAMS) }
}

// ------------------------------------------------------------------ bridge_export

struct ExportVTab;

impl VTab for ExportVTab {
    type InitData = Paged;
    type BindData = ExportBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        add_result_columns(bind);
        Ok(export_bind(&Bind(bind)))
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> { Ok(Paged::new()) }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let b = func.get_bind_data();
        func.get_init_data().next_page(|| export_run(b), |rows| write_rows(output, rows))
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> { Some(vec![varchar()]) }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> { named(EXPORT_PARAMS) }
}

// ------------------------------------------------------------------ bridge_run

struct RunVTab;

impl VTab for RunVTab {
    type InitData = Paged;
    type BindData = RunBind;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        add_result_columns(bind);
        run_bind(&Bind(bind))
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> { Ok(Paged::new()) }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn Error>> {
        let b = func.get_bind_data();
        func.get_init_data().next_page(|| run_run(b), |rows| write_rows(output, rows))
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> { Some(vec![varchar()]) }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> { named(RUN_PARAMS) }
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
            let text = transpile_text(f, t, s, version[i])?;
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

// ------------------------------------------------------------------ bridge_prelude

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
            out.insert(i, prelude_text(nocase).as_str());
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
    install_pool(|| Ok(Connection::open_from_raw(db.cast())?))?;
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
