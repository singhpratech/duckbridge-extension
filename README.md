# duckbridge-extension

The [DuckBridge](https://github.com/singhpratech/duckbridge) engine packaged as a DuckDB community
extension: pull a SQL Server database into DuckDB, run the team's T-SQL there unchanged, and ship
the result back as a live load, a restorable `.bak`, PostgreSQL or Parquet.

```sql
INSTALL duckbridge FROM community;   -- once the community PR is merged
LOAD duckbridge;

SELECT * FROM bridge_import('mssql://user:p%40ss@sql01:1433/Sales', parallel := 8);
SELECT * FROM bridge_run('SELECT c.id, SUM(o.amount) AS total INTO dbo.kpi FROM dbo.customers c JOIN dbo.orders o ON o.customer_id = c.id GROUP BY c.id;');
SELECT * FROM bridge_export('mssql://user:p%40ss@sql01:1433/Sales_kpi', tables := 'kpi', bak := '/backups/Sales_kpi.bak', verify_restore := true);
SELECT bridge_transpile('tsql', 'duckdb', 'SELECT TOP 5 ISNULL(a, 0) FROM dbo.t');
```

Functions: `bridge_import`, `bridge_export`, `bridge_run`, `bridge_transpile`, `bridge_prelude`.
Parameters and behaviour are documented in the main repository's
[DuckDB extension section](https://github.com/singhpratech/duckbridge#duckdb-extension). Windows
authentication is used by default for SQL Server (`DOMAIN\user`, `user@domain`, or no user on Windows).

## Layout

This repository follows DuckDB's [Rust extension template](https://github.com/duckdb/extension-template-rs)
so the community-extensions CI can build it from source. `src/lib.rs` is a copy of
`extension/src/lib.rs` in the main repository at the revision pinned in `Cargo.toml`
(`scripts/sync-from-core.sh` refreshes it; CI fails if the two differ). The engine itself is the
`duckbridge` crate pulled from that same revision.

## Build and test

```bash
git clone --recurse-submodules https://github.com/singhpratech/duckbridge-extension
make configure           # Python venv with DuckDB and the sqllogictest runner
make release             # -> build/release/extension/duckbridge/duckbridge.duckdb_extension
make test_release        # test/sql/*.test
```

The extension uses duckdb-rs on DuckDB's stable v1 C API (`TARGET_DUCKDB_VERSION=v1.5.6` in the
Makefile is the API floor), so one binary loads on DuckDB 1.5.6 and on DuckDB 2.x. Prebuilt binaries for
Linux, macOS and Windows are attached to the main repository's
[releases](https://github.com/singhpratech/duckbridge/releases) as
`duckbridge-<platform>.duckdb_extension.zip`; load them with `duckdb -unsigned`.

MIT licensed.
