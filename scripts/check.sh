#!/usr/bin/env bash
set -euo pipefail

max_source_lines="${MAX_SOURCE_LINES:-1000}"
coverage_threshold="${COVERAGE_THRESHOLD:-80}"

echo "checking file sizes"
line_fail=0
while IFS= read -r -d '' file; do
  lines="$(wc -l < "$file" | tr -d ' ')"
  if [[ "$lines" -gt "$max_source_lines" ]]; then
    echo "file exceeds ${max_source_lines} lines: ${file} (${lines})"
    line_fail=1
  fi
done < <(
  find . \
    \( \
      -path './.git' -o \
      -path './target' -o \
      -path '*/.venv' -o \
      -path '*/.pytest_cache' -o \
      -path '*/__pycache__' \
    \) -prune -o \
    -type f \
    ! -name 'Cargo.lock' \
    ! -name '*.lock' \
    -print0
)

if [[ "$line_fail" -ne 0 ]]; then
  exit 1
fi

echo "checking formatting"
cargo fmt --all -- --check

echo "checking clippy"
cargo clippy --workspace --all-targets -- -D warnings

echo "running tests"
cargo test --workspace

echo "checking unit coverage"
cargo llvm-cov --workspace --lib --fail-under-lines "$coverage_threshold" \
  --summary-only --json --output-path target/coverage-summary.json
