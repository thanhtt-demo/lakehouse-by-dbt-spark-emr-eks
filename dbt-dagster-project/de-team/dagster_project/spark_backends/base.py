# ---------------------------------------------------------------------------------------------------------------------
# SPARK BACKEND HELPERS
# Shared utilities for turning dbt run artifacts (run_results.json + compiled/run SQL) into
# Dagster events. Used by the eks_client backend, where dbt runs in-process and we report
# MaterializeResult / AssetCheckResult directly (no Dagster Pipes round-trip).
#
# These mirror the parsing logic in spark_entrypoint/entrypoint.py (the EMR path), kept here
# so the in-process client-mode backend has the same result semantics without the Pipes layer.
# ---------------------------------------------------------------------------------------------------------------------

from __future__ import annotations

import json
import os
import shutil
from typing import Any, Dict, List, Optional


def seed_partial_parse(project_dir: str, target_path: str) -> bool:
    """Copy dbt's partial_parse.msgpack from the baked project target into the runtime target dir.

    dbt only does fast *partial* parsing if it finds a previous partial_parse.msgpack in the
    --target-path it is given. Since each model run uses a fresh tmp target, we seed it with the
    cache baked into the image (produced by `dagster-dbt project prepare-and-package` in CI).
    Without this, every model run does a full re-parse of the whole project.

    If the cache is stale/incompatible (e.g. dbt version mismatch), dbt silently discards it and
    falls back to a full parse — correctness is never affected, only speed.

    Returns True if the cache file was found and copied.
    """
    src = os.path.join(project_dir, "target", "partial_parse.msgpack")
    if not os.path.isfile(src):
        return False
    try:
        os.makedirs(target_path, exist_ok=True)
        shutil.copy2(src, os.path.join(target_path, "partial_parse.msgpack"))
        return True
    except OSError:
        return False


def parse_run_results(path: str) -> Optional[dict]:
    """Parse dbt run_results.json if it exists, else None."""
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def read_model_sql(target_path: str, model_name: str, kind: str) -> Optional[str]:
    """Read compiled or executed SQL for a dbt model from the target dir.

    dbt writes artifacts under {target_path}/{kind}/{project}/models/.../{model_name}.sql
    where `kind` is "compiled" (Jinja-rendered SELECT) or "run" (full DDL/DML executed).
    The nested folder layout is not known up front, so walk the tree once.
    """
    root = os.path.join(target_path, kind)
    if not os.path.isdir(root):
        return None
    target_filename = f"{model_name}.sql"
    for dirpath, _dirnames, filenames in os.walk(root):
        if target_filename in filenames:
            try:
                with open(os.path.join(dirpath, target_filename)) as f:
                    return f.read()
            except OSError:
                return None
    return None


def collect_test_check_results(run_results: dict, target_path: str) -> List[Dict[str, Any]]:
    """Extract dbt test results into a list of check descriptors.

    Each descriptor: {check_name, passed, metadata}. The check_name uses parts[2] of the
    test unique_id (the bare test name registered by @dbt_assets), NOT the trailing hash.
    """
    checks: List[Dict[str, Any]] = []
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        if not unique_id.startswith("test."):
            continue
        parts = unique_id.split(".")
        if len(parts) < 3:
            continue
        test_name = parts[2]

        passed = result.get("status") == "pass"
        message = result.get("message", "") or ""
        test_sql = result.get("compiled_code") or read_model_sql(
            target_path, test_name, "compiled"
        )

        metadata: Dict[str, Any] = {
            "test_unique_id": unique_id,
            "test_message": message,
            "severity": result.get("severity", "ERROR"),
        }
        if test_sql:
            metadata["test_sql"] = test_sql

        checks.append({"check_name": test_name, "passed": passed, "metadata": metadata})
    return checks


def summarize_dbt_failure(
    run_results: Optional[dict],
    runner_exception: Optional[BaseException],
    model_name: str,
) -> str:
    """Build a single-line failure summary surfacing the actual dbt error(s)."""
    error_lines: List[str] = []
    if runner_exception is not None:
        error_lines.append(f"dbtRunner exception: {runner_exception!r}")
    if run_results:
        for r in run_results.get("results", []):
            status = r.get("status", "")
            if status in ("success", "pass"):
                continue
            uid = r.get("unique_id", "")
            msg = r.get("message", "") or ""
            error_lines.append(f"[{status}] {uid}: {msg}")
    detail = "; ".join(error_lines) if error_lines else "no detail from dbt"
    return f"dbt build failed for model {model_name}: {detail}"
