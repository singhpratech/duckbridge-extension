.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXTENSION_NAME=duckbridge

# Stable v1 C API (footer C_STRUCT, floor TARGET_DUCKDB_VERSION): one binary loads on DuckDB 1.5.6
# and on DuckDB 2.x, whose loader still serves the v1 API table that was stabilized in 1.5.6. The
# unstable footer (USE_UNSTABLE_C_API=1) pins a binary to exactly one DuckDB version, which is what
# kept 0.1.1 from loading on DuckDB 2.0 in the community registry.
USE_UNSTABLE_C_API=0

# The v1 C API table the extension asks for (DUCKDB_EXTENSION_MIN_DUCKDB_VERSION) and the footer floor.
# Keep in sync with the duckdb crate version in Cargo.toml: 1.5.6 <-> 1.10506.x.
TARGET_DUCKDB_VERSION=v1.5.6

all: configure debug

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/rust.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_rust
clean_all: clean_configure clean
