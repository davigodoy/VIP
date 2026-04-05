from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

from .db import get_connection
from .retention import derive_report_culto_id_for_event_ts, load_config

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]


def _state_get(conn, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM sync_state WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def _state_set(conn, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO sync_state (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
          value = excluded.value,
          updated_at = CURRENT_TIMESTAMP
        """,
        (key, value),
    )


def sync_events_to_google_sheets(limit: int = 500) -> dict[str, Any]:
    cfg = load_config()
    if not cfg.sync_google_sheets_enabled:
        return {"status": "skipped", "rows_synced": 0, "message": "sync disabled"}
    if not cfg.sync_spreadsheet_id.strip():
        result = {"status": "error", "rows_synced": 0, "message": "missing spreadsheet id"}
        _record_sync_outcome(result)
        return result
    creds_json = _resolve_credentials_json(cfg)
    if not creds_json:
        result = {
            "status": "error",
            "rows_synced": 0,
            "message": "missing credentials (source/env/file/inline)",
        }
        _record_sync_outcome(result)
        return result

    try:
        creds_info = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(cfg.sync_spreadsheet_id).worksheet(
            cfg.sync_worksheet_name
        )
    except Exception as exc:
        result = {"status": "error", "rows_synced": 0, "message": f"auth/sheet error: {exc}"}
        _record_sync_outcome(result)
        return result

    with get_connection() as conn:
        last_id = int(_state_get(conn, "sync_cursor", "0") or "0")
        rows = conn.execute(
            """
            SELECT id, event_id, culto_id, temp_id, event_type, event_ts, age_band, gender
            FROM events
            WHERE id > ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (last_id, limit),
        ).fetchall()

        if not rows:
            now_iso = datetime.now(UTC).isoformat()
            _state_set(conn, "sync_last_run_ts", now_iso)
            _state_set(conn, "sync_last_status", "ok")
            _state_set(conn, "sync_last_error", "")
            conn.execute(
                "INSERT INTO sync_runs (status, rows_synced, message) VALUES (?, ?, ?)",
                ("ok", 0, "no new rows"),
            )
            conn.commit()
            return {"status": "ok", "rows_synced": 0, "message": "no new rows"}

        values = [
            [
                str(r["id"]),
                r["event_id"],
                derive_report_culto_id_for_event_ts(str(r["event_ts"])),
                r["temp_id"] or "",
                r["event_type"],
                r["event_ts"],
                r["age_band"] or "",
                r["gender"] or "",
            ]
            for r in rows
        ]

        try:
            # Create header lazily if worksheet is empty.
            if not sheet.row_values(1):
                sheet.append_row(
                    [
                        "local_id",
                        "event_id",
                        "culto_id",
                        "temp_id",
                        "event_type",
                        "event_ts",
                        "age_band",
                        "gender",
                    ],
                    value_input_option="USER_ENTERED",
                )
            sheet.append_rows(values, value_input_option="USER_ENTERED")
        except Exception as exc:
            msg = f"append error: {exc}"
            now_iso = datetime.now(UTC).isoformat()
            _state_set(conn, "sync_last_run_ts", now_iso)
            _state_set(conn, "sync_last_status", "error")
            _state_set(conn, "sync_last_error", msg)
            conn.execute(
                "INSERT INTO sync_runs (status, rows_synced, message) VALUES (?, ?, ?)",
                ("error", 0, msg),
            )
            conn.commit()
            return {"status": "error", "rows_synced": 0, "message": msg}

        last_row_id = int(rows[-1]["id"])
        now_iso = datetime.now(UTC).isoformat()
        _state_set(conn, "sync_cursor", str(last_row_id))
        _state_set(conn, "sync_last_run_ts", now_iso)
        _state_set(conn, "sync_last_status", "ok")
        _state_set(conn, "sync_last_error", "")
        conn.execute(
            "INSERT INTO sync_runs (status, rows_synced, message) VALUES (?, ?, ?)",
            ("ok", len(values), "synced"),
        )
        conn.commit()
        return {"status": "ok", "rows_synced": len(values), "message": "synced"}


def get_sync_status() -> dict[str, Any]:
    cfg = load_config()
    with get_connection() as conn:
        last_run_ts = _state_get(conn, "sync_last_run_ts", "")
        last_status = _state_get(conn, "sync_last_status", "never")
        last_error = _state_get(conn, "sync_last_error", "")
        cursor = int(_state_get(conn, "sync_cursor", "0") or "0")
        pending = conn.execute(
            "SELECT COUNT(*) AS c FROM events WHERE id > ?",
            (cursor,),
        ).fetchone()["c"]
        run = conn.execute(
            """
            SELECT run_ts, status, rows_synced, message
            FROM sync_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

    return {
        "enabled": cfg.sync_google_sheets_enabled,
        "interval_sec": cfg.sync_interval_sec,
        "spreadsheet_id_set": bool(cfg.sync_spreadsheet_id.strip()),
        "worksheet_name": cfg.sync_worksheet_name,
        "credentials_source": cfg.sync_credentials_source,
        "credentials_set": bool(_resolve_credentials_json(cfg)),
        "last_run_ts": last_run_ts,
        "last_status": last_status,
        "last_error": last_error,
        "pending_rows": int(pending),
        "last_run": dict(run) if run else None,
    }


def latest_sync_runs(limit: int = 10) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT run_ts, status, rows_synced, message
            FROM sync_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def _record_sync_outcome(result: dict[str, Any]) -> None:
    now_iso = datetime.now(UTC).isoformat()
    status = str(result.get("status", "error"))
    message = str(result.get("message", ""))
    rows_synced = int(result.get("rows_synced", 0))
    with get_connection() as conn:
        _state_set(conn, "sync_last_run_ts", now_iso)
        _state_set(conn, "sync_last_status", status)
        _state_set(conn, "sync_last_error", message if status == "error" else "")
        conn.execute(
            "INSERT INTO sync_runs (status, rows_synced, message) VALUES (?, ?, ?)",
            (status, rows_synced, message),
        )
        conn.commit()


def _resolve_credentials_json(cfg: Any) -> str:
    source = (cfg.sync_credentials_source or "env").strip().lower()
    if source == "env":
        env_name = (cfg.sync_credentials_env_var or "").strip()
        if not env_name:
            return ""
        return os.environ.get(env_name, "").strip()
    if source == "file":
        file_path = (cfg.sync_credentials_file_path or "").strip()
        if not file_path:
            return ""
        try:
            expanded = os.path.expanduser(file_path)
            with open(expanded, encoding="utf-8") as handle:
                return handle.read().strip()
        except OSError:
            return ""
    # Fallback compatible with previous behavior.
    return (cfg.sync_credentials_json or "").strip()

