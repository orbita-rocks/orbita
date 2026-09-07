#!/usr/bin/env bash
#
# Runs the live S3-compatible conditional-write suite without allowing an empty
# test binary to count as evidence. Both Moon's MinIO task and the weekly
# AWS/R2 workflow call this script so the three backends cannot drift onto
# different test commands.
set -euo pipefail

cargo test -p orbita-objectstore --all-features --test minio -- --ignored --list \
  | grep -F 'conditional_writes_enforce_the_manifest_swap_rules: test' >/dev/null

exec cargo test -p orbita-objectstore --all-features --test minio -- --ignored
