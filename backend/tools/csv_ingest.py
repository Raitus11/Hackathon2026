"""
csv_ingest.py
Deterministic data cleanup pipeline.
Runs BEFORE any agents touch the data.
Returns clean, validated dataframes as dicts.
"""
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "queue_managers": ["qm_id", "qm_name", "region"],
    "queues":         ["queue_id", "queue_name", "qm_id", "queue_type"],
    "applications":   ["app_id", "app_name", "qm_id", "direction"],
    "channels":       ["channel_id", "channel_name", "channel_type", "from_qm", "to_qm"],
}

ID_COLUMNS = ["qm_id", "queue_id", "app_id", "channel_id", "from_qm", "to_qm"]


def load_and_clean(csv_paths: dict) -> tuple[dict, dict]:
    """
    Load CSVs, run all 5 cleanup steps.
    Returns: (clean_data_dict, quality_report_dict)
    """
    report = {"steps": [], "warnings": [], "errors": [], "rows_removed": {}}
    data = {}

    # ── Step 1: Load files ────────────────────────────────────────────────
    for key, path in csv_paths.items():
        try:
            df = pd.read_csv(path)
            data[key] = df
            report["steps"].append(f"Loaded {key}: {len(df)} rows")
        except Exception as e:
            report["errors"].append(f"Failed to load {key}: {e}")
            return {}, report

    # ── Step 2: Schema validation ─────────────────────────────────────────
    for key, required in REQUIRED_COLUMNS.items():
        if key not in data:
            continue
        df = data[key]
        missing = [c for c in required if c not in df.columns]
        if missing:
            report["errors"].append(f"{key} missing required columns: {missing}")
        else:
            report["steps"].append(f"Schema OK for {key}")

    if report["errors"]:
        return {}, report

    # ── Step 3: Normalise IDs (strip whitespace, uppercase) ───────────────
    for key, df in data.items():
        for col in ID_COLUMNS:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.upper()
        data[key] = df
    report["steps"].append("Normalised all ID columns (strip + uppercase)")

    # ── Step 4: Deduplication ─────────────────────────────────────────────
    pk_map = {
        "queue_managers": "qm_id",
        "queues":         "queue_id",
        "applications":   None,          # no single PK — composite
        "channels":       "channel_id",
    }
    for key, pk in pk_map.items():
        if pk and key in data:
            before = len(data[key])
            data[key] = data[key].drop_duplicates(subset=[pk])
            removed = before - len(data[key])
            if removed:
                report["warnings"].append(f"Removed {removed} duplicate rows from {key}")
                report["rows_removed"][key] = removed
    report["steps"].append("Deduplication complete")

    # ── Step 5: Referential integrity ─────────────────────────────────────
    valid_qm_ids = set(data["queue_managers"]["qm_id"].tolist())

    # Queues must reference valid QMs
    before = len(data["queues"])
    data["queues"] = data["queues"][data["queues"]["qm_id"].isin(valid_qm_ids)]
    removed = before - len(data["queues"])
    if removed:
        report["warnings"].append(f"Removed {removed} queues with invalid qm_id references")

    # Applications must reference valid QMs
    before = len(data["applications"])
    data["applications"] = data["applications"][data["applications"]["qm_id"].isin(valid_qm_ids)]
    removed = before - len(data["applications"])
    if removed:
        report["warnings"].append(f"Removed {removed} applications with invalid qm_id references")

    # Channels must reference valid QMs on both ends
    before = len(data["channels"])
    data["channels"] = data["channels"][
        data["channels"]["from_qm"].isin(valid_qm_ids) &
        data["channels"]["to_qm"].isin(valid_qm_ids)
    ]
    removed = before - len(data["channels"])
    if removed:
        report["warnings"].append(f"Removed {removed} channels referencing unknown QMs")

    report["steps"].append("Referential integrity validated")

    # ── Step 6: Null checks on critical fields ────────────────────────────
    for key, required in REQUIRED_COLUMNS.items():
        if key not in data:
            continue
        null_mask = data[key][required].isnull().any(axis=1)
        null_count = null_mask.sum()
        if null_count:
            report["warnings"].append(f"Found {null_count} rows with null required fields in {key} — flagged")
            data[key] = data[key][~null_mask]

    report["steps"].append("Null value check complete")
    report["summary"] = {
        "queue_managers": len(data.get("queue_managers", [])),
        "queues":         len(data.get("queues", [])),
        "applications":   len(data.get("applications", [])),
        "channels":       len(data.get("channels", [])),
    }

    # Convert to JSON-serialisable dicts
    clean = {k: v.to_dict(orient="records") for k, v in data.items()}
    return clean, report
