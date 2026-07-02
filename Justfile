set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

check:
    ./scripts/check.sh
