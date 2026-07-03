set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

check:
    ./scripts/check.sh

bench-smoke:
    @cargo run -p kevindb-bench --quiet

bench-core:
    @cargo run -p kevindb-bench --quiet -- core
