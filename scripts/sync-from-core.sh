#!/usr/bin/env bash
# Copy the extension source from the duckbridge core repo at the revision pinned in Cargo.toml,
# so this repo and singhpratech/duckbridge/extension/src/ never drift. CI runs the same check.
set -euo pipefail
cd "$(dirname "$0")/.."
REV=$(sed -n 's/.*github.com\/singhpratech\/duckbridge".*rev = "\([0-9a-f]*\)".*/\1/p' Cargo.toml)
[ -n "$REV" ] || { echo "no pinned rev in Cargo.toml"; exit 1; }
for f in lib.rs duckdb_v1.rs duckdb_neo.rs; do
  curl -sSfL "https://raw.githubusercontent.com/singhpratech/duckbridge/$REV/extension/src/$f" -o "src/$f"
done
echo "src/{lib,duckdb_v1,duckdb_neo}.rs synced from singhpratech/duckbridge@$REV"
