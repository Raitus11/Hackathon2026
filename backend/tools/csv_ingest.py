"""
csv_ingest.py
Transforms a single MQ Raw Data file into the 4 logical tables
the pipeline expects: queue_managers, queues, applications, channels.

Input: csv_paths = {"raw_file": "/path/to/MQ_Raw_Data.csv"}
Output: (clean_data_dict, quality_report_dict)

Auto-detects CSV vs Excel — handles files with wrong extensions.
"""
import pandas as pd
import logging
import hashlib
from pathlib import Path

logger = logging.getLogger(__name__)


def _load_dataframe(file_path: str) -> pd.DataFrame:
    """
    Load a file as DataFrame. Tries CSV first, falls back to Excel.
    Handles the common case where an Excel file has a .csv extension.
    """
    # Try CSV first (fastest)
    try:
        df = pd.read_csv(file_path)
        if len(df.columns) > 1:  # sanity check — single-column = probably not CSV
            return df
    except Exception:
        pass

    # Fall back to Excel
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
        return df
    except Exception:
        pass

    # Last resort — CSV with different encodings
    for enc in ["latin-1", "cp1252", "iso-8859-1"]:
        try:
            df = pd.read_csv(file_path, encoding=enc)
            if len(df.columns) > 1:
                return df
        except Exception:
            pass

    raise ValueError(f"Cannot read file as CSV or Excel: {file_path}")


def load_and_clean(csv_paths: dict) -> tuple[dict, dict]:
    """
    Load single MQ Raw Data file, transform into 4 logical tables.
    Returns: (clean_data_dict, quality_report_dict)
    """
    report = {"steps": [], "warnings": [], "errors": [], "rows_removed": {}}

    file_path = csv_paths.get("raw_file", "")
    if not file_path:
        report["errors"].append("No raw_file path provided in csv_paths")
        return {}, report

    # ── Step 1: Load file (auto-detect format) ────────────────────────────
    try:
        df = _load_dataframe(file_path)
        report["steps"].append(f"Loaded file: {len(df)} rows, {len(df.columns)} columns")
    except Exception as e:
        report["errors"].append(f"Failed to load file: {e}")
        return {}, report

    # ── Step 2: Validate required columns ─────────────────────────────────
    required_cols = [
        "queue_manager_name", "app_id", "q_type",
        "Discrete Queue Name", "PrimaryAppRole",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        report["errors"].append(f"Missing required columns: {missing}. Found: {list(df.columns)[:10]}")
        return {}, report
    report["steps"].append("Schema validation passed")

    # ── Step 3: Normalise key fields ──────────────────────────────────────
    df["queue_manager_name"] = df["queue_manager_name"].astype(str).str.strip().str.upper()
    df["app_id"] = df["app_id"].astype(str).str.strip().str.upper()
    df["q_type"] = df["q_type"].astype(str).str.strip().str.upper()
    df["Discrete Queue Name"] = df["Discrete Queue Name"].astype(str).str.strip().str.upper()
    df["PrimaryAppRole"] = df["PrimaryAppRole"].astype(str).str.strip().str.upper()

    for col in ["remote_q_mgr_name", "remote_q_name", "xmit_q_name"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
            df.loc[df[col].isin(["NAN", "NONE", ""]), col] = pd.NA

    before = len(df)
    df = df.dropna(subset=["queue_manager_name", "app_id", "Discrete Queue Name"])
    removed = before - len(df)
    if removed:
        report["warnings"].append(f"Dropped {removed} rows with null critical fields")
        report["rows_removed"]["null_critical"] = removed

    report["steps"].append(f"Normalised key fields. Working with {len(df)} rows.")

    # ── Step 4: Extract QUEUE MANAGERS ────────────────────────────────────
    qm_rows = []
    for qm_name in sorted(df["queue_manager_name"].unique()):
        qm_slice = df[df["queue_manager_name"] == qm_name]

        region = "UNKNOWN"
        if "Neighborhood" in df.columns:
            vals = qm_slice["Neighborhood"].dropna()
            if len(vals) > 0:
                mode = vals.mode()
                region = str(mode.iloc[0]) if len(mode) > 0 else str(vals.iloc[0])

        lob = "UNKNOWN"
        if "line_of_business" in df.columns:
            vals = qm_slice["line_of_business"].dropna()
            if len(vals) > 0:
                mode = vals.mode()
                lob = str(mode.iloc[0]) if len(mode) > 0 else str(vals.iloc[0])

        qm_rows.append({
            "qm_id": qm_name,
            "qm_name": qm_name,
            "region": region.strip(),
            "line_of_business": lob.strip(),
        })

    # Add QMs only referenced in remote_q_mgr_name
    if "remote_q_mgr_name" in df.columns:
        existing_ids = {r["qm_id"] for r in qm_rows}
        for rqm in df["remote_q_mgr_name"].dropna().unique():
            if rqm not in existing_ids:
                qm_rows.append({
                    "qm_id": rqm, "qm_name": rqm,
                    "region": "REMOTE_REFERENCED", "line_of_business": "UNKNOWN",
                })
                report["warnings"].append(
                    f"QM '{rqm}' referenced in remote_q_mgr_name but not in queue_manager_name — added"
                )

    report["steps"].append(f"Extracted {len(qm_rows)} queue managers")

    # ── Step 5: Extract QUEUES ────────────────────────────────────────────
    queue_rows = []
    seen_queues = set()
    for _, row in df.iterrows():
        q_name = row["Discrete Queue Name"]
        qm_id = row["queue_manager_name"]
        key = (q_name, qm_id)
        if key in seen_queues:
            continue
        seen_queues.add(key)

        q_type_raw = row["q_type"]
        q_type_mapped = "LOCAL"
        if q_type_raw == "REMOTE":
            q_type_mapped = "REMOTE"
        elif q_type_raw == "ALIAS":
            q_type_mapped = "ALIAS"

        usage = "NORMAL"
        if "usage" in df.columns and pd.notna(row.get("usage")):
            raw_usage = str(row["usage"]).strip().upper()
            if raw_usage in ("NORMAL", "XMITQ"):
                usage = raw_usage

        q_id = f"Q_{hashlib.md5(f'{qm_id}:{q_name}'.encode()).hexdigest()[:8].upper()}"

        entry = {
            "queue_id": q_id,
            "queue_name": q_name,
            "qm_id": qm_id,
            "queue_type": q_type_mapped,
            "usage": usage,
        }

        if q_type_mapped == "REMOTE":
            if "remote_q_mgr_name" in df.columns and pd.notna(row.get("remote_q_mgr_name")):
                entry["remote_qm"] = str(row["remote_q_mgr_name"]).strip().upper()
            if "remote_q_name" in df.columns and pd.notna(row.get("remote_q_name")):
                entry["remote_queue"] = str(row["remote_q_name"]).strip().upper()
            if "xmit_q_name" in df.columns and pd.notna(row.get("xmit_q_name")):
                entry["xmit_queue"] = str(row["xmit_q_name"]).strip().upper()

        queue_rows.append(entry)

    report["steps"].append(f"Extracted {len(queue_rows)} unique queues")

    # ── Step 6: Extract APPLICATIONS ──────────────────────────────────────
    app_rows = []
    seen_app_rows = set()
    for _, row in df.iterrows():
        app_id = row["app_id"]
        qm_id = row["queue_manager_name"]
        q_name = row["Discrete Queue Name"]
        role = row["PrimaryAppRole"]

        direction = "UNKNOWN"
        if role == "PRODUCER":
            direction = "PUT"
        elif role == "CONSUMER":
            direction = "GET"

        app_name = app_id
        for name_col in ["Primary App_Full_Name", "PrimaryAppDisp", "ProducerName", "Consumer Name"]:
            if name_col in df.columns and pd.notna(row.get(name_col)):
                app_name = str(row[name_col]).strip()
                break

        q_id = f"Q_{hashlib.md5(f'{qm_id}:{q_name}'.encode()).hexdigest()[:8].upper()}"

        dedup_key = (app_id, qm_id, q_id, direction)
        if dedup_key in seen_app_rows:
            continue
        seen_app_rows.add(dedup_key)

        app_rows.append({
            "app_id": app_id,
            "app_name": app_name,
            "qm_id": qm_id,
            "queue_id": q_id,
            "queue_name": q_name,
            "direction": direction,
        })

    report["steps"].append(f"Extracted {len(app_rows)} application-queue relationships")

    # ── Step 7: Infer CHANNELS from REMOTE queue definitions ──────────────
    channel_rows = []
    seen_channels = set()
    valid_qm_ids = {r["qm_id"] for r in qm_rows}

    for q in queue_rows:
        if q["queue_type"] != "REMOTE":
            continue
        remote_qm = q.get("remote_qm")
        if not remote_qm:
            continue
        from_qm = q["qm_id"]
        to_qm = remote_qm
        if from_qm == to_qm:
            continue

        ch_key = (from_qm, to_qm)
        if ch_key in seen_channels:
            continue
        seen_channels.add(ch_key)

        channel_name = f"{from_qm}.{to_qm}"
        ch_id = f"CH_{hashlib.md5(channel_name.encode()).hexdigest()[:8].upper()}"

        channel_rows.append({
            "channel_id": ch_id + "_SDR",
            "channel_name": channel_name,
            "channel_type": "SENDER",
            "from_qm": from_qm, "to_qm": to_qm,
            "status": "RUNNING",
            "xmit_queue": q.get("xmit_queue", f"{to_qm}.XMITQ"),
        })
        channel_rows.append({
            "channel_id": ch_id + "_RCVR",
            "channel_name": channel_name,
            "channel_type": "RECEIVER",
            "from_qm": from_qm, "to_qm": to_qm,
            "status": "RUNNING",
        })

    # Also infer from xmit_q_name patterns
    if "xmit_q_name" in df.columns:
        for _, row in df.iterrows():
            xmit = row.get("xmit_q_name")
            if pd.isna(xmit):
                continue
            xmit = str(xmit).strip().upper()
            parts = xmit.split(".")
            from_qm = row["queue_manager_name"]
            to_qm_candidate = parts[-1] if len(parts) > 1 else xmit
            if to_qm_candidate in valid_qm_ids and from_qm != to_qm_candidate:
                ch_key = (from_qm, to_qm_candidate)
                if ch_key not in seen_channels:
                    seen_channels.add(ch_key)
                    channel_name = f"{from_qm}.{to_qm_candidate}"
                    ch_id = f"CH_{hashlib.md5(channel_name.encode()).hexdigest()[:8].upper()}"
                    channel_rows.append({
                        "channel_id": ch_id + "_SDR",
                        "channel_name": channel_name,
                        "channel_type": "SENDER",
                        "from_qm": from_qm, "to_qm": to_qm_candidate,
                        "status": "RUNNING", "xmit_queue": xmit,
                    })
                    channel_rows.append({
                        "channel_id": ch_id + "_RCVR",
                        "channel_name": channel_name,
                        "channel_type": "RECEIVER",
                        "from_qm": from_qm, "to_qm": to_qm_candidate,
                        "status": "RUNNING",
                    })

    report["steps"].append(f"Inferred {len(channel_rows)} channel definitions from remote queue metadata")

    # ── Summary ───────────────────────────────────────────────────────────
    report["summary"] = {
        "queue_managers": len(qm_rows),
        "queues": len(queue_rows),
        "applications": len(app_rows),
        "channels": len(channel_rows),
        "raw_rows": len(df),
    }

    clean_data = {
        "queue_managers": qm_rows,
        "queues": queue_rows,
        "applications": app_rows,
        "channels": channel_rows,
    }

    report["steps"].append(
        f"Transformation complete: {len(qm_rows)} QMs, {len(queue_rows)} queues, "
        f"{len(app_rows)} apps, {len(channel_rows)} channels from {len(df)} raw rows"
    )

    return clean_data, report
