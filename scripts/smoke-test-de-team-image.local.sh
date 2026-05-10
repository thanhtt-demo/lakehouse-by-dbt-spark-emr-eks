#!/usr/bin/env bash
# ---------------------------------------------------------------------------------------------------------------------
# LOCAL SMOKE TEST — de-team Base Image
#
# Builds the de-team Base Image and verifies that the Spark runtime Python
# (python3.11, matching EMR on EKS 7.13's PYSPARK_PYTHON) can import the
# packages our entrypoint needs: dagster_pipes, dbt, boto3.
#
# Runs entirely offline (no ECR push, no EMR job). Fast feedback loop for
# Dockerfile.base changes.
#
# Usage:
#   ./scripts/smoke-test-de-team-image.local.sh [TAG]
#     TAG defaults to "local-smoke"
# ---------------------------------------------------------------------------------------------------------------------
set -euo pipefail

TAG="${1:-local-smoke}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DE_TEAM_DIR="$REPO_ROOT/dbt-dagster-project/de-team"
IMAGE="de-team-base:${TAG}"

echo "==> [1/2] Building Base Image: $IMAGE"
docker build -f "$DE_TEAM_DIR/Dockerfile.base" -t "$IMAGE" "$DE_TEAM_DIR"

echo ""
echo "==> [2/2] Running import check inside $IMAGE (python3.11)..."
TMP_PY="$(mktemp --suffix=.py)"
trap 'rm -f "$TMP_PY"' EXIT

cat >"$TMP_PY" <<'PY'
import sys
import dagster_pipes
import boto3
from dbt.version import __version__ as dbt_version

print("python:", sys.executable)
print("dagster_pipes:", dagster_pipes.__file__)
print("dbt:", dbt_version)
print("boto3:", boto3.__version__)
PY

docker run --rm \
    --entrypoint python3.11 \
    -v "$TMP_PY:/tmp/smoke.py:ro" \
    "$IMAGE" \
    /tmp/smoke.py

echo ""
echo "==> LOCAL SMOKE TEST PASSED"
