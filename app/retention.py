from __future__ import annotations

import json
import os
import subprocess
import sys
import time as time_std
import uuid
import sqlite3
import threading
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from collections import defaultdict
from collections.abc import Callable, Mapping
from typing import Any

from .camera_devices import list_detected_cameras
from .db import get_connection
from .models import (
    AgeBand,
    EventIngestRequest,
    GenderBand,
    ReconciliationApplyRequest,
    RetentionConfig,
    ServiceScheduleCreate,
    ServiceScheduleOut,
    ServiceScheduleUpdate,
)

# Agregados ao vivo e fila de pessoas: um unico registro (culto na agenda e so para exibicao/consulta).
GLOBAL_STATS_ID = "__global__"

_INVOLVEMENT_WHERE_ENTRADA = r"""
    event_type = 'entrada'
    AND temp_id IS NOT NULL
    AND TRIM(COALESCE(temp_id, '')) != ''
    AND temp_id NOT LIKE 'hog\_%' ESCAPE '\\'
    AND LENGTH(COALESCE(event_ts, '')) >= 10
    AND substr(event_ts, 1, 10) >= date('now', ?)
"""

_involvement_summary_cache: dict[str, Any] | None = None
_involvement_summary_cache_time: float = 0.0
_INVOLVEMENT_SUMMARY_TTL_SEC = 45.0
_involvement_summary_lock = threading.Lock()


def invalidate_involvement_summary_cache() -> None:
    global _involvement_summary_cache
    with _involvement_summary_lock:
        _involvement_summary_cache = None


def _to_bool(value: str) -> bool:
    return str(value).lower() in {"1", "true", "yes", "on"}


def _legacy_envolvimento_tiers(raw: dict[str, Any]) -> tuple[int, int]:
    """
    Bases antigas so tinham envolvimento_visitas_min_membro.
    Mapeia para (max_visitante, max_frequentador) preservando faixas:
    visitante = 1 dia; frequentador = 2 .. min-1; membro >= min.
    """
    old_m = raw.get("envolvimento_visitas_min_membro")
    try:
        old_min = int(old_m) if old_m is not None and str(old_m).strip() != "" else 3
    except ValueError:
        old_min = 3
    old_min = max(2, min(31, old_min))
    return 1, max(1, old_min - 1)


def load_config() -> RetentionConfig:
    with get_connection() as conn:
        rows = conn.execute("SELECT key, value FROM config").fetchall()
    raw = {row["key"]: row["value"] for row in rows}
    v_key = str(raw.get("envolvimento_max_dias_visitante", "")).strip()
    f_key = str(raw.get("envolvimento_max_dias_frequentador", "")).strip()
    if not v_key or not f_key:
        lv, lf = _legacy_envolvimento_tiers(raw)
        raw = {
            **raw,
            "envolvimento_max_dias_visitante": v_key or str(lv),
            "envolvimento_max_dias_frequentador": f_key or str(lf),
        }
    default_cfg = RetentionConfig(
        retencao_temp_id_horas=6,
        retencao_profile_dias=90,
        retencao_eventos_dias=180,
        retencao_agregados_meses=24,
        retencao_imagens_horas=0,
        janela_reentrada_min=15,
        limiar_match=0.75,
        auto_cleanup_enabled=True,
        auto_cleanup_hour=3,
        camera_device="/dev/video0",
        camera_label="Entrada principal",
        camera_enabled=True,
        camera_inference_width=640,
        camera_inference_height=360,
        camera_fps=8,
        live_detection_enabled=False,
        culto_antecedencia_min=30,
        culto_duracao_min=150,
        estimar_faixa_etaria=True,
        estimar_genero=True,
        sync_google_sheets_enabled=False,
        sync_interval_sec=60,
        sync_spreadsheet_id="",
        sync_worksheet_name="Eventos",
        sync_credentials_source="env",
        sync_credentials_env_var="VIP_GSHEETS_CREDENTIALS_JSON",
        sync_credentials_file_path="",
        sync_credentials_json="",
        idade_limite_crianca=11,
        idade_limite_junior=14,
        idade_limite_adolescente=17,
        idade_limite_jovem=24,
        idade_limite_adulto=59,
        envolvimento_janela_dias=30,
        envolvimento_max_dias_visitante=2,
        envolvimento_max_dias_frequentador=5,
    )

    def _raw_or_default(key: str) -> str:
        value = raw.get(key)
        if value is not None:
            return value
        field_default = getattr(default_cfg, key, None)
        return "" if field_default is None else str(field_default)
    source = raw.get("sync_credentials_source", "").strip()
    if not source:
        source = "inline" if raw.get("sync_credentials_json", "").strip() else "env"
    if source not in {"env", "file", "inline"}:
        source = "env"
    env_var = raw.get("sync_credentials_env_var", "").strip() or "VIP_GSHEETS_CREDENTIALS_JSON"
    file_path = raw.get("sync_credentials_file_path", "").strip()
    # Keep app resilient with legacy/bad DB values.
    if source == "file" and not file_path:
        source = "env"
    return RetentionConfig(
        retencao_temp_id_horas=int(_raw_or_default("retencao_temp_id_horas")),
        retencao_profile_dias=int(_raw_or_default("retencao_profile_dias")),
        retencao_eventos_dias=int(_raw_or_default("retencao_eventos_dias")),
        retencao_agregados_meses=int(_raw_or_default("retencao_agregados_meses")),
        retencao_imagens_horas=int(_raw_or_default("retencao_imagens_horas")),
        janela_reentrada_min=int(_raw_or_default("janela_reentrada_min")),
        limiar_match=float(_raw_or_default("limiar_match")),
        auto_cleanup_enabled=_to_bool(_raw_or_default("auto_cleanup_enabled")),
        auto_cleanup_hour=int(_raw_or_default("auto_cleanup_hour")),
        camera_device=_raw_or_default("camera_device"),
        camera_label=_raw_or_default("camera_label"),
        camera_enabled=_to_bool(_raw_or_default("camera_enabled")),
        camera_inference_width=int(_raw_or_default("camera_inference_width")),
        camera_inference_height=int(_raw_or_default("camera_inference_height")),
        camera_fps=int(_raw_or_default("camera_fps")),
        live_detection_enabled=_to_bool(_raw_or_default("live_detection_enabled")),
        culto_antecedencia_min=int(_raw_or_default("culto_antecedencia_min")),
        culto_duracao_min=int(_raw_or_default("culto_duracao_min")),
        estimar_faixa_etaria=_to_bool(_raw_or_default("estimar_faixa_etaria")),
        estimar_genero=_to_bool(_raw_or_default("estimar_genero")),
        sync_google_sheets_enabled=_to_bool(
            _raw_or_default("sync_google_sheets_enabled")
        ),
        sync_interval_sec=int(_raw_or_default("sync_interval_sec")),
        sync_spreadsheet_id=_raw_or_default("sync_spreadsheet_id"),
        sync_worksheet_name=_raw_or_default("sync_worksheet_name"),
        sync_credentials_source=source,
        sync_credentials_env_var=env_var,
        sync_credentials_file_path=file_path,
        sync_credentials_json=raw.get("sync_credentials_json", "").strip(),
        idade_limite_crianca=int(_raw_or_default("idade_limite_crianca")),
        idade_limite_junior=int(_raw_or_default("idade_limite_junior")),
        idade_limite_adolescente=int(_raw_or_default("idade_limite_adolescente")),
        idade_limite_jovem=int(_raw_or_default("idade_limite_jovem")),
        idade_limite_adulto=int(_raw_or_default("idade_limite_adulto")),
        envolvimento_janela_dias=int(_raw_or_default("envolvimento_janela_dias")),
        envolvimento_max_dias_visitante=int(
            _raw_or_default("envolvimento_max_dias_visitante")
        ),
        envolvimento_max_dias_frequentador=int(
            _raw_or_default("envolvimento_max_dias_frequentador")
        ),
    )


def save_config(payload: RetentionConfig) -> None:
    update_data: dict[str, Any] = payload.model_dump()
    with get_connection() as conn:
        for key, value in update_data.items():
            conn.execute(
                """
                INSERT INTO config (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(key) DO UPDATE SET
                  value = excluded.value,
                  updated_at = CURRENT_TIMESTAMP
                """,
                (key, str(int(value) if isinstance(value, bool) else value)),
            )
        conn.commit()
    invalidate_involvement_summary_cache()


def update_involvement_rules(
    *,
    envolvimento_janela_dias: int,
    envolvimento_max_dias_visitante: int,
    envolvimento_max_dias_frequentador: int,
) -> RetentionConfig:
    """Atualiza so as tres chaves de envolvimento; revalida RetentionConfig completo."""
    cur = load_config()
    updated = cur.model_copy(
        update={
            "envolvimento_janela_dias": envolvimento_janela_dias,
            "envolvimento_max_dias_visitante": envolvimento_max_dias_visitante,
            "envolvimento_max_dias_frequentador": envolvimento_max_dias_frequentador,
        }
    )
    save_config(updated)
    return updated


def apply_camera_device(device: str) -> RetentionConfig:
    cfg = load_config()
    updated = cfg.model_copy(update={"camera_device": device.strip()})
    save_config(updated)
    return load_config()


def execute_cleanup(*, dry_run: bool) -> dict[str, Any]:
    config = load_config()
    now = datetime.now(UTC)
    event_cutoff_mod = f"-{config.retencao_eventos_dias} days"

    policies = {
        "temp_tracks": f"datetime('now', '-{config.retencao_temp_id_horas} hours')",
        "profiles": f"datetime('now', '-{config.retencao_profile_dias} days')",
        "events": f"datetime('now', '{event_cutoff_mod}')",
        "aggregated_metrics": f"datetime('now', '-{config.retencao_agregados_meses} months')",
        "snapshots": f"datetime('now', '-{config.retencao_imagens_horas} hours')",
        "anon_face_profiles": f"datetime('now', '-{config.retencao_profile_dias} days')",
    }

    with get_connection() as conn:
        counts = {
            "temp_tracks": conn.execute(
                f"SELECT COUNT(*) AS c FROM temp_tracks WHERE created_at < {policies['temp_tracks']}"
            ).fetchone()["c"],
            "profiles": conn.execute(
                f"SELECT COUNT(*) AS c FROM profiles WHERE last_seen < {policies['profiles']}"
            ).fetchone()["c"],
            "anon_face_profiles": conn.execute(
                f"SELECT COUNT(*) AS c FROM anon_face_profiles WHERE last_seen < {policies['anon_face_profiles']}"
            ).fetchone()["c"],
            "events": conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM events
                WHERE julianday(event_ts) IS NOT NULL
                  AND julianday(event_ts) < julianday('now', ?)
                """,
                (event_cutoff_mod,),
            ).fetchone()["c"],
            "aggregated_metrics": conn.execute(
                f"SELECT COUNT(*) AS c FROM aggregated_metrics WHERE service_started_at < {policies['aggregated_metrics']}"
            ).fetchone()["c"],
            "snapshots": conn.execute(
                f"SELECT COUNT(*) AS c FROM snapshots WHERE captured_at < {policies['snapshots']}"
            ).fetchone()["c"],
        }

        if not dry_run:
            conn.execute(
                f"DELETE FROM temp_tracks WHERE created_at < {policies['temp_tracks']}"
            )
            conn.execute(f"DELETE FROM profiles WHERE last_seen < {policies['profiles']}")
            conn.execute(
                f"DELETE FROM anon_face_profiles WHERE last_seen < {policies['anon_face_profiles']}"
            )
            conn.execute(
                """
                DELETE FROM events
                WHERE julianday(event_ts) IS NOT NULL
                  AND julianday(event_ts) < julianday('now', ?)
                """,
                (event_cutoff_mod,),
            )
            conn.execute(
                f"DELETE FROM aggregated_metrics WHERE service_started_at < {policies['aggregated_metrics']}"
            )
            conn.execute(f"DELETE FROM snapshots WHERE captured_at < {policies['snapshots']}")
            conn.commit()
            invalidate_involvement_summary_cache()

        result = {
            "run_ts": now.isoformat(),
            "dry_run": dry_run,
            "policies": payload_from_config(config),
            "deleted_or_would_delete": counts,
        }

        conn.execute(
            "INSERT INTO cleanup_runs (dry_run, result_json) VALUES (?, ?)",
            (1 if dry_run else 0, json.dumps(result)),
        )
        conn.commit()
        return result


def reset_identified_personas(
    *,
    reset_personas_day: str | None = None,
    wipe_all_personas: bool = False,
    delete_day_events: bool = False,
) -> dict[str, Any]:
    """
    Limpa identificadores de pessoa sem apagar eventos.

    - Dia especifico: zera temp_id dos eventos daquele dia (YYYY-MM-DD)
    - Dia especifico + delete_day_events=True: remove todos os eventos daquele dia
    - Global: zera temp_id de todos os eventos
    - Sempre limpa estado operacional (temp_tracks/profiles) e recomputa agregados
    """
    day = (reset_personas_day or "").strip()
    if day:
        try:
            datetime.strptime(day, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(
                "Data invalida em reset_personas_day (use YYYY-MM-DD)."
            ) from exc
    if not day and not wipe_all_personas:
        raise ValueError(
            "Informe reset_personas_day ou marque wipe_all_personas para executar o reset."
        )
    if delete_day_events and not day:
        raise ValueError(
            "delete_day_events exige reset_personas_day informado."
        )
    if delete_day_events and wipe_all_personas:
        raise ValueError(
            "Use delete_day_events para data especifica ou wipe_all_personas para reset global."
        )

    with get_connection() as conn:
        if wipe_all_personas:
            affected_rows = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM events
                    WHERE temp_id IS NOT NULL AND TRIM(COALESCE(temp_id, '')) != ''
                    """
                ).fetchone()["c"]
            )
            affected_person_ids = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM (
                      SELECT temp_id FROM events
                      WHERE temp_id IS NOT NULL AND TRIM(COALESCE(temp_id, '')) != ''
                      GROUP BY temp_id
                    )
                    """
                ).fetchone()["c"]
            )
            conn.execute(
                """
                UPDATE events
                SET temp_id = NULL
                WHERE temp_id IS NOT NULL AND TRIM(COALESCE(temp_id, '')) != ''
                """
            )
        else:
            day_total_rows = int(
                conn.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE substr(event_ts, 1, 10) = ?",
                    (day,),
                ).fetchone()["c"]
            )
            affected_rows = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM events
                    WHERE substr(event_ts, 1, 10) = ?
                      AND temp_id IS NOT NULL
                      AND TRIM(COALESCE(temp_id, '')) != ''
                    """,
                    (day,),
                ).fetchone()["c"]
            )
            affected_person_ids = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM (
                      SELECT temp_id FROM events
                      WHERE substr(event_ts, 1, 10) = ?
                        AND temp_id IS NOT NULL
                        AND TRIM(COALESCE(temp_id, '')) != ''
                      GROUP BY temp_id
                    )
                    """,
                    (day,),
                ).fetchone()["c"]
            )
            if delete_day_events:
                conn.execute(
                    "DELETE FROM events WHERE substr(event_ts, 1, 10) = ?",
                    (day,),
                )
                affected_rows = day_total_rows
            else:
                conn.execute(
                    """
                    UPDATE events
                    SET temp_id = NULL
                    WHERE substr(event_ts, 1, 10) = ?
                      AND temp_id IS NOT NULL
                      AND TRIM(COALESCE(temp_id, '')) != ''
                    """,
                    (day,),
                )

        wiped_temp_tracks = int(
            conn.execute("SELECT COUNT(*) AS c FROM temp_tracks").fetchone()["c"]
        )
        wiped_profiles = int(conn.execute("SELECT COUNT(*) AS c FROM profiles").fetchone()["c"])
        wiped_anon_face_profiles = int(
            conn.execute("SELECT COUNT(*) AS c FROM anon_face_profiles").fetchone()["c"]
        )
        conn.execute("DELETE FROM temp_tracks")
        conn.execute("DELETE FROM profiles")
        conn.execute("DELETE FROM anon_face_profiles")
        conn.commit()

    # Reconstroi agregados/pessoas a partir de events (agora com temp_id limpo no periodo pedido).
    cfg = load_config()
    with get_connection() as conn:
        event_rows = conn.execute(
            """
            SELECT temp_id, event_type, event_ts, age_band, gender
            FROM events
            ORDER BY event_ts ASC, id ASC
            """
        ).fetchall()
    rows_list = list(event_rows)
    by_culto: dict[str, list[Any]] = defaultdict(list)
    for r in rows_list:
        cid = derive_report_culto_id_for_event_ts(str(r["event_ts"]))
        if cid and str(cid).strip():
            by_culto[str(cid)].append(r)
    stats, people = recompute_reconciliation_metrics(rows_list, cfg.janela_reentrada_min)
    per_culto: list[tuple[str, dict[str, int], dict[str, dict[str, Any]]]] = []
    for cid in sorted(by_culto.keys()):
        st, pe = recompute_reconciliation_metrics(
            by_culto[cid], cfg.janela_reentrada_min
        )
        per_culto.append((cid, st, pe))
    write_full_reconciliation_all_partitions(stats, people, per_culto)

    invalidate_involvement_summary_cache()
    return {
        "ok": True,
        "reset_personas_day": day or None,
        "wipe_all_personas": bool(wipe_all_personas),
        "delete_day_events": bool(delete_day_events),
        "affected_event_rows": affected_rows,
        "affected_person_ids": affected_person_ids,
        "wiped_temp_tracks": wiped_temp_tracks,
        "wiped_profiles": wiped_profiles,
        "wiped_anon_face_profiles": wiped_anon_face_profiles,
        "reconciled_events": len(rows_list),
        "reconciled_partitions": 1 + len(per_culto),
    }


def payload_from_config(config: RetentionConfig) -> dict[str, Any]:
    return config.model_dump()


def get_reconciliation_status() -> dict[str, Any]:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT run_id, status, progress_pct, processed_events, total_events,
                   processed_services, total_services, message, started_at, updated_at
            FROM reconciliation_state
            WHERE state_key = 'main'
            """
        ).fetchone()
    if not row:
        return {
            "running": False,
            "run_id": "",
            "status": "idle",
            "progress_pct": 0,
            "processed_events": 0,
            "total_events": 0,
            "processed_services": 0,
            "total_services": 0,
            "message": "",
            "started_at": "",
            "updated_at": "",
            "recent_runs": [],
        }
    status = {
        "running": row["status"] in {"queued", "running"},
        "run_id": row["run_id"] or "",
        "status": row["status"] or "idle",
        "progress_pct": int(row["progress_pct"] or 0),
        "processed_events": int(row["processed_events"] or 0),
        "total_events": int(row["total_events"] or 0),
        "processed_services": int(row["processed_services"] or 0),
        "total_services": int(row["total_services"] or 0),
        "message": row["message"] or "",
        "started_at": row["started_at"] or "",
        "updated_at": row["updated_at"] or "",
    }
    status["recent_runs"] = get_reconciliation_runs(limit=10)
    return status


def get_reconciliation_runs(limit: int = 10) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT run_ts, status, progress_pct, processed_events, total_events,
                   processed_services, total_services, duration_sec, message
            FROM reconciliation_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def request_reconciliation_run() -> dict[str, Any]:
    status = get_reconciliation_status()
    if status["running"]:
        return {
            "accepted": False,
            "message": "Conciliação já está em execução.",
            "status": status,
        }

    run_id = str(uuid.uuid4())
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO reconciliation_runs (
                run_id, run_ts, status, progress_pct,
                processed_events, total_events,
                processed_services, total_services,
                duration_sec, message
            ) VALUES (?, CURRENT_TIMESTAMP, 'queued', 0, 0, 0, 0, 0, 0, 'Aguardando início...')
            """,
            (run_id,),
        )
        conn.commit()
    _set_reconciliation_state(
        run_id=run_id,
        status="queued",
        progress_pct=0,
        processed_events=0,
        total_events=0,
        processed_services=0,
        total_services=0,
        message="Aguardando início...",
    )
    return {
        "accepted": True,
        "run_id": run_id,
        "message": "Conciliação iniciada.",
        "status": get_reconciliation_status(),
    }


def _set_reconciliation_state(
    *,
    run_id: str,
    status: str,
    progress_pct: int,
    processed_events: int,
    total_events: int,
    processed_services: int,
    total_services: int,
    message: str,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO reconciliation_state (
                state_key, run_id, status, progress_pct, processed_events, total_events,
                processed_services, total_services, message, updated_at
            ) VALUES ('main', ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(state_key) DO UPDATE SET
                run_id = excluded.run_id,
                status = excluded.status,
                progress_pct = excluded.progress_pct,
                processed_events = excluded.processed_events,
                total_events = excluded.total_events,
                processed_services = excluded.processed_services,
                total_services = excluded.total_services,
                message = excluded.message,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                run_id,
                status,
                int(max(0, min(100, progress_pct))),
                int(max(0, processed_events)),
                int(max(0, total_events)),
                int(max(0, processed_services)),
                int(max(0, total_services)),
                message,
            ),
        )
        conn.commit()


def _close_reconciliation_run(
    *,
    run_id: str,
    status: str,
    progress_pct: int,
    processed_events: int,
    total_events: int,
    processed_services: int,
    total_services: int,
    duration_sec: int,
    message: str,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE reconciliation_runs
            SET status = ?,
                progress_pct = ?,
                processed_events = ?,
                total_events = ?,
                processed_services = ?,
                total_services = ?,
                duration_sec = ?,
                message = ?
            WHERE run_id = ?
            """,
            (
                status,
                int(max(0, min(100, progress_pct))),
                int(max(0, processed_events)),
                int(max(0, total_events)),
                int(max(0, processed_services)),
                int(max(0, total_services)),
                int(max(0, duration_sec)),
                message,
                run_id,
            ),
        )
        conn.commit()


def _reconciliation_row_val(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    return row[key]


def list_events_for_reconciliation_export() -> list[dict[str, Any]]:
    """Lista eventos para o browser recomputar (mesma ordem que o job no servidor)."""
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT temp_id, event_type, event_ts, age_band, gender
            FROM events
            ORDER BY event_ts ASC, id ASC
            """
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "temp_id": (r["temp_id"] or "") if r["temp_id"] is not None else "",
                "event_type": r["event_type"],
                "event_ts": r["event_ts"],
                "age_band": r["age_band"],
                "gender": r["gender"],
            }
        )
    return out


def recompute_reconciliation_metrics(
    event_rows: list[Any],
    janela_reentrada_min: int,
    *,
    on_progress: Callable[[int, int], None] | None = None,
) -> tuple[dict[str, int], dict[str, dict[str, Any]]]:
    """
    Recomputo puro (espelha a logica do antigo run_reconciliation_job).
    Retorna (stats para gravacao, people por person_id).

    Espelho no cliente: templates/index.html — funcao recomputeReconciliationInBrowser
    (manter algoritmo alinhado ao alterar esta funcao).
    """
    entry_seen: set[str] = set()
    people: dict[str, dict[str, Any]] = {}
    entries = 0
    exits = 0
    returns = 0
    unique = 0
    occupancy = 0
    peak = 0
    age_counts = {
        "crianca_count": 0,
        "junior_count": 0,
        "adolescente_count": 0,
        "jovem_count": 0,
        "adulto_count": 0,
        "idoso_count": 0,
    }
    gender_counts = {"homem_count": 0, "mulher_count": 0}
    processed = 0
    total_rows = len(event_rows)

    for row in event_rows:
        processed += 1
        person_id = str(_reconciliation_row_val(row, "temp_id") or "").strip()
        if not person_id:
            continue
        direction = _reconciliation_row_val(row, "event_type")
        event_ts_s = _reconciliation_row_val(row, "event_ts")
        try:
            event_dt = datetime.fromisoformat(str(event_ts_s))
        except (ValueError, TypeError):
            event_dt = None
        person = people.get(person_id)
        if person is None:
            ab = _reconciliation_row_val(row, "age_band")
            g = _reconciliation_row_val(row, "gender")
            person = {
                "first_seen_at": event_ts_s,
                "last_seen_at": event_ts_s,
                "entries_count": 0,
                "exits_count": 0,
                "returns_count": 0,
                "age_band": ab or None,
                "gender": g or None,
                "last_direction": direction,
                "last_exit_at": event_ts_s if direction == "saida" else None,
            }
            people[person_id] = person

        person["last_seen_at"] = event_ts_s
        row_ab = _reconciliation_row_val(row, "age_band")
        row_g = _reconciliation_row_val(row, "gender")
        if not person["age_band"] and row_ab:
            person["age_band"] = row_ab
        if not person["gender"] and row_g:
            person["gender"] = row_g

        if direction == "entrada":
            entries += 1
            occupancy += 1
            if person_id not in entry_seen:
                unique += 1
                entry_seen.add(person_id)
                if person["age_band"] in {
                    "crianca",
                    "junior",
                    "adolescente",
                    "jovem",
                    "adulto",
                    "idoso",
                }:
                    age_counts[f"{person['age_band']}_count"] += 1
                if person["gender"] in {"homem", "mulher"}:
                    gender_counts[f"{person['gender']}_count"] += 1

            if (
                person["last_direction"] == "saida"
                and person["last_exit_at"]
                and event_dt is not None
            ):
                try:
                    delta = event_dt - datetime.fromisoformat(
                        str(person["last_exit_at"])
                    )
                    if timedelta(minutes=0) <= delta <= timedelta(
                        minutes=janela_reentrada_min
                    ):
                        returns += 1
                        person["returns_count"] += 1
                except ValueError:
                    pass
            person["entries_count"] += 1
            person["last_direction"] = "entrada"
            peak = max(peak, occupancy)
        elif direction == "saida":
            exits += 1
            occupancy = max(0, occupancy - 1)
            person["exits_count"] += 1
            person["last_direction"] = "saida"
            person["last_exit_at"] = event_ts_s

        if on_progress is not None and total_rows > 0 and processed % 100 == 0:
            on_progress(processed, total_rows)

    stats: dict[str, int] = {
        "entries_count": entries,
        "exits_count": exits,
        "returns_count": returns,
        "unique_people_count": unique,
        "current_occupancy": max(0, occupancy),
        "peak_occupancy": max(0, peak),
        **age_counts,
        **gender_counts,
    }
    return stats, people


def _upsert_partition_reconciliation_conn(
    conn: sqlite3.Connection,
    culto_id: str,
    stats: dict[str, int],
    people: dict[str, dict[str, Any]],
) -> bool:
    """Grava uma particao (stats + pessoas). Retorna True se os totais de stats mudaram."""
    current_row = conn.execute(
        "SELECT * FROM service_event_stats WHERE culto_id = ?",
        (culto_id,),
    ).fetchone()
    next_row = {k: int(stats[k]) for k in stats}
    changed = current_row is None or any(
        int(current_row[key]) != int(value) for key, value in next_row.items()
    )

    conn.execute(
        """
            INSERT INTO service_event_stats (
                culto_id, entries_count, exits_count, returns_count, unique_people_count,
                current_occupancy, peak_occupancy, crianca_count, junior_count,
                adolescente_count, jovem_count, adulto_count, idoso_count,
                homem_count, mulher_count, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(culto_id) DO UPDATE SET
                entries_count = excluded.entries_count,
                exits_count = excluded.exits_count,
                returns_count = excluded.returns_count,
                unique_people_count = excluded.unique_people_count,
                current_occupancy = excluded.current_occupancy,
                peak_occupancy = excluded.peak_occupancy,
                crianca_count = excluded.crianca_count,
                junior_count = excluded.junior_count,
                adolescente_count = excluded.adolescente_count,
                jovem_count = excluded.jovem_count,
                adulto_count = excluded.adulto_count,
                idoso_count = excluded.idoso_count,
                homem_count = excluded.homem_count,
                mulher_count = excluded.mulher_count,
                updated_at = CURRENT_TIMESTAMP
            """,
        (
            culto_id,
            next_row["entries_count"],
            next_row["exits_count"],
            next_row["returns_count"],
            next_row["unique_people_count"],
            next_row["current_occupancy"],
            next_row["peak_occupancy"],
            next_row["crianca_count"],
            next_row["junior_count"],
            next_row["adolescente_count"],
            next_row["jovem_count"],
            next_row["adulto_count"],
            next_row["idoso_count"],
            next_row["homem_count"],
            next_row["mulher_count"],
        ),
    )

    conn.execute("DELETE FROM service_event_people WHERE culto_id = ?", (culto_id,))
    person_rows = [
        (
            culto_id,
            pid,
            data["first_seen_at"],
            data["last_seen_at"],
            int(data["entries_count"]),
            int(data["exits_count"]),
            int(data["returns_count"]),
            data["age_band"],
            data["gender"],
            data["last_direction"],
            data["last_exit_at"],
        )
        for pid, data in people.items()
    ]
    if person_rows:
        conn.executemany(
            """
                INSERT INTO service_event_people (
                    culto_id, person_id, first_seen_at, last_seen_at,
                    entries_count, exits_count, returns_count, age_band, gender,
                    last_direction, last_exit_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
            person_rows,
        )
    return changed


def write_full_reconciliation_all_partitions(
    global_stats: dict[str, int],
    global_people: dict[str, dict[str, Any]],
    per_culto: list[tuple[str, dict[str, int], dict[str, dict[str, Any]]]],
) -> int:
    """Substitui todas as particoes (job no servidor). Retorna 1 se alguma particao de stats mudou."""
    changed_rows = 0
    with get_connection() as conn:
        conn.execute("DELETE FROM service_event_stats")
        conn.execute("DELETE FROM service_event_people")
        if _upsert_partition_reconciliation_conn(
            conn, GLOBAL_STATS_ID, global_stats, global_people
        ):
            changed_rows = 1
        for cid, st, pe in per_culto:
            if _upsert_partition_reconciliation_conn(conn, cid, st, pe):
                changed_rows = 1
        conn.commit()
    return changed_rows


def write_reconciliation_results_to_db(
    stats: dict[str, int], people: dict[str, dict[str, Any]]
) -> int:
    """
    Conciliacao aplicada pelo browser: atualiza apenas __global__.
    Nao apaga nem altera outras particoes (culto_id != __global__).
    """
    changed_rows = 0
    with get_connection() as conn:
        if _upsert_partition_reconciliation_conn(
            conn, GLOBAL_STATS_ID, stats, people
        ):
            changed_rows = 1
        conn.commit()
    return changed_rows


def apply_reconciliation_from_browser(payload: ReconciliationApplyRequest) -> dict[str, Any]:
    """Aplica resultado calculado no PC/Mac; so escrita na BD do servidor."""
    if get_reconciliation_status()["running"]:
        return {
            "ok": False,
            "message": "Conciliacao no servidor em execucao; aguarde terminar.",
        }
    people_dict: dict[str, dict[str, Any]] = {}
    for p in payload.people:
        d = p.model_dump()
        pid = str(d.pop("person_id", "")).strip()
        if not pid:
            continue
        people_dict[pid] = d
    stats = payload.stats.model_dump()
    changed = write_reconciliation_results_to_db(stats, people_dict)
    return {
        "ok": True,
        "message": f"Conciliacao aplicada. Pessoas: {len(people_dict)}.",
        "changed_rows": changed,
        "people_rows": len(people_dict),
    }


def run_reconciliation_job(run_id: str) -> dict[str, Any]:
    started_at = datetime.now(UTC)
    cfg = load_config()
    _set_reconciliation_state(
        run_id=run_id,
        status="running",
        progress_pct=1,
        processed_events=0,
        total_events=0,
        processed_services=0,
        total_services=0,
        message="Preparando varredura...",
    )

    try:
        with get_connection() as conn:
            event_rows = conn.execute(
                """
                SELECT temp_id, event_type, event_ts, age_band, gender
                FROM events
                ORDER BY event_ts ASC, id ASC
                """
            ).fetchall()

        rows_list = list(event_rows)
        total_rows = len(rows_list)
        by_culto: dict[str, list[Any]] = defaultdict(list)
        for r in rows_list:
            cid = derive_report_culto_id_for_event_ts(str(r["event_ts"]))
            if cid and str(cid).strip():
                by_culto[str(cid)].append(r)

        total_partitions = 1 + len(by_culto)
        _set_reconciliation_state(
            run_id=run_id,
            status="running",
            progress_pct=3,
            processed_events=0,
            total_events=total_rows,
            processed_services=0,
            total_services=total_partitions,
            message="Iniciando recomputo de métricas (global e por culto)...",
        )

        def _progress(processed: int, tot: int) -> None:
            pct = min(95, int((processed / tot) * 100)) if tot > 0 else 0
            _set_reconciliation_state(
                run_id=run_id,
                status="running",
                progress_pct=pct,
                processed_events=processed,
                total_events=tot,
                processed_services=1,
                total_services=total_partitions,
                message=f"Reprocessando eventos {processed}/{tot}...",
            )

        stats, people = recompute_reconciliation_metrics(
            rows_list,
            cfg.janela_reentrada_min,
            on_progress=_progress,
        )

        per_culto: list[tuple[str, dict[str, int], dict[str, dict[str, Any]]]] = []
        for cid in sorted(by_culto.keys()):
            st, pe = recompute_reconciliation_metrics(
                by_culto[cid],
                cfg.janela_reentrada_min,
                on_progress=None,
            )
            per_culto.append((cid, st, pe))

        touched_cultos = total_partitions
        changed_rows = write_full_reconciliation_all_partitions(stats, people, per_culto)

        message = (
            f"Conciliação concluída. Eventos: {total_rows}, partições: {total_partitions}."
        )
        _set_reconciliation_state(
            run_id=run_id,
            status="done",
            progress_pct=100,
            processed_events=total_rows,
            total_events=total_rows,
            processed_services=touched_cultos,
            total_services=touched_cultos,
            message=message,
        )
        duration_sec = int((datetime.now(UTC) - started_at).total_seconds())
        _close_reconciliation_run(
            run_id=run_id,
            status="done",
            progress_pct=100,
            processed_events=total_rows,
            total_events=total_rows,
            processed_services=touched_cultos,
            total_services=touched_cultos,
            duration_sec=duration_sec,
            message=message,
        )
        return {
            "status": "done",
            "run_id": run_id,
            "scanned_events": total_rows,
            "touched_cultos": touched_cultos,
            "changed_rows": changed_rows,
            "message": message,
        }
    except Exception as exc:
        err = f"Falha na conciliação: {exc}"
        _set_reconciliation_state(
            run_id=run_id,
            status="error",
            progress_pct=100,
            processed_events=0,
            total_events=0,
            processed_services=0,
            total_services=0,
            message=err,
        )
        duration_sec = int((datetime.now(UTC) - started_at).total_seconds())
        _close_reconciliation_run(
            run_id=run_id,
            status="error",
            progress_pct=100,
            processed_events=0,
            total_events=0,
            processed_services=0,
            total_services=0,
            duration_sec=duration_sec,
            message=err,
        )
        raise


def involvement_band_for(visit_days: int, max_visitante: int, max_frequentador: int) -> str:
    """Classifica por dias distintos com entrada na janela (limites inclusivos)."""
    vd = int(visit_days)
    mv = int(max_visitante)
    mf = int(max_frequentador)
    if mf <= mv:
        mf = mv + 1
    if vd <= mv:
        return "visitante"
    if vd <= mf:
        return "frequentador"
    return "membro"


def _involvement_window_params() -> tuple[int, int, int, str]:
    cfg = load_config()
    janela = max(7, min(120, int(cfg.envolvimento_janela_dias)))
    max_v = max(1, min(janela, int(cfg.envolvimento_max_dias_visitante)))
    max_f = max(1, min(janela, int(cfg.envolvimento_max_dias_frequentador)))
    if max_f <= max_v:
        max_f = max_v + 1
    return janela, max_v, max_f, f"-{janela} days"


def _involvement_summary_and_total(
    conn: sqlite3.Connection, mod: str, max_visitante: int, max_frequentador: int
) -> tuple[dict[str, int], int]:
    total_row = conn.execute(
        f"""
        SELECT COUNT(*) AS c FROM (
          SELECT temp_id FROM events WHERE {_INVOLVEMENT_WHERE_ENTRADA} GROUP BY temp_id
        )
        """,
        (mod,),
    ).fetchone()
    total = int(total_row["c"]) if total_row else 0

    summary_rows = conn.execute(
        f"""
        SELECT env, COUNT(*) AS c FROM (
          SELECT CASE
            WHEN vd <= ? THEN 'visitante'
            WHEN vd <= ? THEN 'frequentador'
            ELSE 'membro'
          END AS env
          FROM (
            SELECT temp_id,
              COUNT(DISTINCT substr(event_ts, 1, 10)) AS vd
            FROM events
            WHERE {_INVOLVEMENT_WHERE_ENTRADA}
            GROUP BY temp_id
          )
        )
        GROUP BY env
        """,
        (max_visitante, max_frequentador, mod),
    ).fetchall()

    summary = {"visitante": 0, "frequentador": 0, "membro": 0}
    for sr in summary_rows:
        key = str(sr["env"])
        if key in summary:
            summary[key] = int(sr["c"])
    return summary, total


def fetch_involvement_summary_bundle() -> dict[str, Any]:
    """Resumo global (mesmas regras da lista de envolvimento). Sem cache."""
    janela, max_v, max_f, mod = _involvement_window_params()
    with get_connection() as conn:
        summary, total = _involvement_summary_and_total(conn, mod, max_v, max_f)
    return {
        "janela_dias": janela,
        "max_dias_visitante": max_v,
        "max_dias_frequentador": max_f,
        "summary": summary,
        "total_person_ids": total,
    }


def get_involvement_summary_for_live_metrics() -> dict[str, Any]:
    """Para o dashboard: cache com TTL + invalidacao em ingest/save_config."""
    global _involvement_summary_cache, _involvement_summary_cache_time
    now_m = time_std.monotonic()
    with _involvement_summary_lock:
        if _involvement_summary_cache is not None and (
            now_m - _involvement_summary_cache_time
        ) < _INVOLVEMENT_SUMMARY_TTL_SEC:
            return dict(_involvement_summary_cache)
    fresh = fetch_involvement_summary_bundle()
    with _involvement_summary_lock:
        _involvement_summary_cache = fresh
        _involvement_summary_cache_time = now_m
    return dict(fresh)


def get_people_involvement(*, limit: int, offset: int) -> dict[str, Any]:
    """
    Lista person_id (temp_id) com entradas na janela movel; envolvimento derivado
    de dias distintos com pelo menos uma entrada (nao numero de cultos isolados).
    """
    janela, max_v, max_f, mod = _involvement_window_params()
    with get_connection() as conn:
        summary, total = _involvement_summary_and_total(conn, mod, max_v, max_f)

        rows = conn.execute(
            f"""
            SELECT temp_id AS person_id,
              COUNT(DISTINCT substr(event_ts, 1, 10)) AS visit_days,
              MAX(event_ts) AS last_entrada
            FROM events
            WHERE {_INVOLVEMENT_WHERE_ENTRADA}
            GROUP BY temp_id
            ORDER BY last_entrada DESC
            LIMIT ? OFFSET ?
            """,
            (mod, limit, offset),
        ).fetchall()

    people: list[dict[str, Any]] = []
    for r in rows:
        vd = int(r["visit_days"])
        band = involvement_band_for(vd, max_v, max_f)
        people.append(
            {
                "person_id": r["person_id"],
                "visit_days": vd,
                "envolvimento": band,
                "last_entrada": r["last_entrada"],
            }
        )

    return {
        "janela_dias": janela,
        "max_dias_visitante": max_v,
        "max_dias_frequentador": max_f,
        "definicoes": {
            "visitante": (
                f"ate {max_v} dia(s) de calendario distinto(s) com entrada nos ultimos {janela} dias"
            ),
            "frequentador": (
                f"de {max_v + 1} a {max_f} dia(s) distintos com entrada nos ultimos {janela} dias"
            ),
            "membro": (
                f"{max_f + 1} ou mais dia(s) distintos com entrada nos ultimos {janela} dias"
            ),
        },
        "summary": summary,
        "total": total,
        "limit": limit,
        "offset": offset,
        "people": people,
        "nota_identidade": (
            "Fidedigno quando o edge envia o mesmo person_id em cada visita. "
            "IDs locais hog_* (detector HOG no servidor) sao ignorados no envolvimento "
            "para evitar falsos visitantes por troca de track."
        ),
    }


def latest_cleanup_runs(limit: int = 10) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT run_ts, dry_run, result_json FROM cleanup_runs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    response: list[dict[str, Any]] = []
    for row in rows:
        parsed = json.loads(row["result_json"])
        parsed["dry_run"] = bool(row["dry_run"])
        parsed["run_ts"] = row["run_ts"]
        response.append(parsed)
    return response


def list_schedules() -> list[ServiceScheduleOut]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, service_name, day_of_week, start_time, is_active
            FROM service_schedules
            ORDER BY day_of_week, start_time, id
            """
        ).fetchall()
    return [
        ServiceScheduleOut(
            id=row["id"],
            service_name=row["service_name"],
            day_of_week=int(row["day_of_week"]),
            start_time=row["start_time"],
            is_active=bool(row["is_active"]),
        )
        for row in rows
    ]


def create_schedule(payload: ServiceScheduleCreate) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO service_schedules (service_name, day_of_week, start_time, is_active, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                payload.service_name.strip(),
                payload.day_of_week,
                payload.start_time,
                1 if payload.is_active else 0,
            ),
        )
        conn.commit()


def update_schedule(schedule_id: int, payload: ServiceScheduleUpdate) -> bool:
    with get_connection() as conn:
        cur = conn.execute(
            """
            UPDATE service_schedules
            SET service_name = ?,
                day_of_week = ?,
                start_time = ?,
                is_active = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                payload.service_name.strip(),
                payload.day_of_week,
                payload.start_time,
                1 if payload.is_active else 0,
                schedule_id,
            ),
        )
        conn.commit()
        return cur.rowcount > 0


def delete_schedule(schedule_id: int) -> bool:
    with get_connection() as conn:
        cur = conn.execute("DELETE FROM service_schedules WHERE id = ?", (schedule_id,))
        conn.commit()
        return cur.rowcount > 0


def camera_status() -> dict[str, Any]:
    config = load_config()
    device = config.camera_device.strip()
    exists = Path(device).exists() if device.startswith("/") else True
    return {
        "camera_enabled": config.camera_enabled,
        "camera_device": device,
        "camera_label": config.camera_label,
        "camera_device_exists": exists,
        "inference_resolution": f"{config.camera_inference_width}x{config.camera_inference_height}",
        "camera_fps": config.camera_fps,
        "live_detection_enabled": config.live_detection_enabled,
    }


def list_camera_devices() -> list[str]:
    return [c["id"] for c in list_detected_cameras()]


def systemd_status(service_name: str = "vip-dashboard.service") -> dict[str, Any]:
    service_file = Path("/etc/systemd/system") / service_name
    exists = service_file.exists()
    enabled = "unknown"
    active = "unknown"

    if exists:
        enabled = _systemctl_value("is-enabled", service_name)
        active = _systemctl_value("is-active", service_name)

    return {
        "service_name": service_name,
        "service_file_exists": exists,
        "enabled": enabled,
        "active": active,
    }


def _systemctl_value(cmd: str, service_name: str) -> str:
    try:
        result = subprocess.run(
            ["systemctl", cmd, service_name],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
        return (result.stdout or result.stderr).strip() or "unknown"
    except Exception:
        return "unknown"


_UPDATE_LOCK = threading.Lock()
def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _git_cmd(repo: Path, *args: str) -> list[str]:
    """Prefixo git com safe.directory (evita 'dubious ownership' entre users, ex. admin vs pi)."""
    return ["git", "-c", f"safe.directory={repo.resolve()}", *args]


def _run_command(
    cmd: list[str], *, cwd: Path, timeout: int = 120
) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except Exception as exc:
        return False, str(exc)
    output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    text = output.strip() or f"returncode={proc.returncode}"
    return proc.returncode == 0, text


def _collect_git_update_info(*, refresh_remote: bool) -> dict[str, Any]:
    cwd = _repo_root()
    branch = ""
    local_commit = ""
    remote_commit = ""
    ahead = 0
    behind = 0
    fetch_error = ""

    ok_branch, out_branch = _run_command(
        _git_cmd(cwd, "rev-parse", "--abbrev-ref", "HEAD"), cwd=cwd, timeout=30
    )
    if ok_branch:
        branch = out_branch.splitlines()[-1].strip()

    ok_local, out_local = _run_command(_git_cmd(cwd, "rev-parse", "HEAD"), cwd=cwd, timeout=30)
    if ok_local:
        local_commit = out_local.splitlines()[-1].strip()

    if branch and branch != "HEAD":
        if refresh_remote:
            ok_fetch, out_fetch = _run_command(
                _git_cmd(cwd, "fetch", "origin", branch), cwd=cwd, timeout=120
            )
            if not ok_fetch:
                fetch_error = out_fetch

        ok_remote, out_remote = _run_command(
            _git_cmd(cwd, "rev-parse", f"origin/{branch}"), cwd=cwd, timeout=30
        )
        if ok_remote:
            remote_commit = out_remote.splitlines()[-1].strip()

        if local_commit and remote_commit:
            ok_counts, out_counts = _run_command(
                _git_cmd(
                    cwd,
                    "rev-list",
                    "--left-right",
                    "--count",
                    f"HEAD...origin/{branch}",
                ),
                cwd=cwd,
                timeout=30,
            )
            if ok_counts:
                parts = out_counts.split()
                if len(parts) >= 2:
                    ahead = int(parts[0] or 0)
                    behind = int(parts[1] or 0)

    return {
        "branch": branch,
        "local_commit": local_commit,
        "remote_commit": remote_commit,
        "ahead_count": ahead,
        "behind_count": behind,
        "fetch_error": fetch_error,
    }


def _set_update_state(
    *,
    run_id: str,
    status: str,
    progress_pct: int,
    current_step: str,
    message: str,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO update_state (
                state_key, run_id, status, progress_pct, current_step, message, updated_at
            ) VALUES ('main', ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(state_key) DO UPDATE SET
                run_id = excluded.run_id,
                status = excluded.status,
                progress_pct = excluded.progress_pct,
                current_step = excluded.current_step,
                message = excluded.message,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                run_id,
                status,
                int(max(0, min(100, progress_pct))),
                current_step,
                message,
            ),
        )
        conn.execute(
            """
            UPDATE update_runs
            SET status = ?, progress_pct = ?, current_step = ?, message = ?
            WHERE run_id = ?
            """,
            (
                status,
                int(max(0, min(100, progress_pct))),
                current_step,
                message,
                run_id,
            ),
        )
        conn.commit()


def _append_update_log(run_id: str, output: str) -> None:
    with get_connection() as conn:
        current_row = conn.execute(
            "SELECT output_log FROM update_runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        current = str(current_row["output_log"] or "") if current_row else ""
        sep = "\n\n" if current else ""
        combined = (current + sep + output).strip()
        if len(combined) > 120_000:
            combined = combined[-120_000:]
        conn.execute(
            "UPDATE update_runs SET output_log = ? WHERE run_id = ?",
            (combined, run_id),
        )
        conn.commit()


def _close_update_run(
    *,
    run_id: str,
    status: str,
    progress_pct: int,
    current_step: str,
    message: str,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            UPDATE update_runs
            SET finished_at = CURRENT_TIMESTAMP,
                status = ?,
                progress_pct = ?,
                current_step = ?,
                message = ?
            WHERE run_id = ?
            """,
            (
                status,
                int(max(0, min(100, progress_pct))),
                current_step,
                message,
                run_id,
            ),
        )
        conn.commit()


def get_update_history(limit: int = 10) -> list[dict[str, Any]]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT run_id, started_at, finished_at, status, progress_pct, current_step, message
            FROM update_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_update_status(*, refresh_remote: bool = False) -> dict[str, Any]:
    git_info = _collect_git_update_info(refresh_remote=refresh_remote)
    with get_connection() as conn:
        state = conn.execute(
            """
            SELECT run_id, status, progress_pct, current_step, message
            FROM update_state
            WHERE state_key = 'main'
            """
        ).fetchone()
        last = conn.execute(
            """
            SELECT run_id, started_at, finished_at, status, progress_pct, current_step, message
            FROM update_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

    state_status = str(state["status"]) if state else "idle"
    running = state_status in {"queued", "running"}
    last_run_ts = ""
    last_status = "never"
    last_message = ""
    if last:
        last_run_ts = str(last["finished_at"] or last["started_at"] or "")
        last_status = str(last["status"] or "unknown")
        last_message = str(last["message"] or "")

    message = str(state["message"]) if state and running else last_message
    if git_info["fetch_error"]:
        message = (
            f"{message} | Falha ao checar remoto: {git_info['fetch_error']}"
            if message
            else f"Falha ao checar remoto: {git_info['fetch_error']}"
        )

    return {
        "run_id": str(state["run_id"]) if state and state["run_id"] else "",
        "running": running,
        "status": state_status,
        "progress_pct": int(state["progress_pct"] or 0) if state else 0,
        "current_step": str(state["current_step"]) if state else "",
        "message": message,
        "branch": git_info["branch"] or "",
        "local_commit": git_info["local_commit"] or "",
        "local_commit_short": (git_info["local_commit"] or "")[:8],
        "remote_commit": git_info["remote_commit"] or "",
        "remote_commit_short": (git_info["remote_commit"] or "")[:8],
        "ahead_count": int(git_info["ahead_count"] or 0),
        "behind_count": int(git_info["behind_count"] or 0),
        "last_run_ts": last_run_ts,
        "last_status": last_status,
        "last_message": last_message,
    }


def request_system_update_run() -> dict[str, Any]:
    status = get_update_status(refresh_remote=False)
    if status["running"]:
        return {
            "accepted": False,
            "message": "Atualizacao ja esta em execucao.",
            "status": status,
        }
    run_id = str(uuid.uuid4())
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO update_runs (
                run_id, started_at, status, progress_pct, current_step, message, output_log
            ) VALUES (?, CURRENT_TIMESTAMP, 'queued', 0, 'queued', 'Aguardando inicio...', '')
            """,
            (run_id,),
        )
        conn.commit()
    _set_update_state(
        run_id=run_id,
        status="queued",
        progress_pct=0,
        current_step="queued",
        message="Aguardando inicio...",
    )
    return {
        "accepted": True,
        "run_id": run_id,
        "message": "Atualizacao iniciada.",
    }


def run_system_update_job(run_id: str) -> dict[str, Any]:
    if not _UPDATE_LOCK.acquire(blocking=False):
        _set_update_state(
            run_id=run_id,
            status="error",
            progress_pct=100,
            current_step="locked",
            message="Outra atualizacao ja esta em execucao.",
        )
        _close_update_run(
            run_id=run_id,
            status="error",
            progress_pct=100,
            current_step="locked",
            message="Outra atualizacao ja esta em execucao.",
        )
        return {"status": "error", "message": "Outra atualizacao ja esta em execucao."}

    repo = _repo_root()
    # Igual ao update_raspi.sh: .venv local se existir e for executavel; senao pip no
    # Python do processo com --break-system-packages (PEP 668 no Debian/Raspberry Pi OS).
    venv_python = repo / ".venv" / "bin" / "python"
    use_venv = venv_python.is_file() and os.access(venv_python, os.X_OK)
    if use_venv:
        python_exec = str(venv_python)
        pip_exec_cmd = [python_exec, "-m", "pip", "install", "-r", "requirements.txt"]
    else:
        python_exec = sys.executable
        pip_exec_cmd = [
            python_exec,
            "-m",
            "pip",
            "install",
            "--break-system-packages",
            "-r",
            "requirements.txt",
        ]

    git_info = _collect_git_update_info(refresh_remote=False)
    branch = str(git_info["branch"] or "").strip()
    if not branch or branch == "HEAD":
        branch = "main"
    fetch_cmd = _git_cmd(repo, "fetch", "origin", branch)
    # Espelha origin: remove arquivos/dirs nao rastreados que bloqueariam checkout, depois alinha o HEAD.
    # Ignorados (.venv, data/*.db com data/ no gitignore, etc.) nao sao removidos por clean -fd.
    clean_cmd = _git_cmd(repo, "clean", "-fd")
    reset_cmd = _git_cmd(repo, "reset", "--hard", f"origin/{branch}")

    steps: list[tuple[str, list[str], int, bool]] = [
        ("git_fetch", fetch_cmd, 10, False),
        ("git_clean", clean_cmd, 22, False),
        ("git_reset_hard", reset_cmd, 35, False),
    ]
    if (repo / "requirements.txt").exists():
        steps.append(("pip_install", pip_exec_cmd, 55, False))
    steps.extend(
        [
            ("compile", [python_exec, "-m", "compileall", "app"], 70, False),
            ("init_db", [python_exec, "-c", "from app.db import init_db; init_db(); print('DB OK')"], 82, False),
            ("systemd_restart", ["systemctl", "restart", "vip-dashboard.service"], 94, True),
            ("systemd_status", ["systemctl", "is-active", "vip-dashboard.service"], 100, True),
        ]
    )

    warnings: list[str] = []
    try:
        _set_update_state(
            run_id=run_id,
            status="running",
            progress_pct=1,
            current_step="init",
            message="Preparando atualizacao...",
        )
        for step_name, cmd, pct, optional in steps:
            _set_update_state(
                run_id=run_id,
                status="running",
                progress_pct=max(1, pct - 3),
                current_step=step_name,
                message=f"Executando {step_name}...",
            )
            ok, out = _run_command(
                cmd,
                cwd=repo,
                timeout=600 if step_name == "pip_install" else 180,
            )
            _append_update_log(run_id, f"$ {' '.join(cmd)}\n{out}")
            if not ok:
                if optional:
                    warnings.append(f"{step_name}: {out}")
                    continue
                error_msg = f"Falha no passo {step_name}: {out}"
                _set_update_state(
                    run_id=run_id,
                    status="error",
                    progress_pct=100,
                    current_step=step_name,
                    message=error_msg,
                )
                _close_update_run(
                    run_id=run_id,
                    status="error",
                    progress_pct=100,
                    current_step=step_name,
                    message=error_msg,
                )
                return {"status": "error", "run_id": run_id, "message": error_msg}
            _set_update_state(
                run_id=run_id,
                status="running",
                progress_pct=pct,
                current_step=step_name,
                message=out[-500:] if out else f"{step_name} concluido",
            )

        final_status = "warning" if warnings else "done"
        final_message = "Atualizacao concluida com sucesso."
        if warnings:
            final_message = (
                "Atualizacao concluida com avisos (reinicio/systemd pode exigir sudo)."
            )
            _append_update_log(run_id, "WARNINGS:\n" + "\n\n".join(warnings))
        _set_update_state(
            run_id=run_id,
            status=final_status,
            progress_pct=100,
            current_step="finished",
            message=final_message,
        )
        _close_update_run(
            run_id=run_id,
            status=final_status,
            progress_pct=100,
            current_step="finished",
            message=final_message,
        )
        return {"status": final_status, "run_id": run_id, "message": final_message}
    except Exception as exc:
        err = f"Erro inesperado na atualizacao: {exc}"
        _set_update_state(
            run_id=run_id,
            status="error",
            progress_pct=100,
            current_step="exception",
            message=err,
        )
        _close_update_run(
            run_id=run_id,
            status="error",
            progress_pct=100,
            current_step="exception",
            message=err,
        )
        return {"status": "error", "run_id": run_id, "message": err}
    finally:
        _UPDATE_LOCK.release()


def _build_culto_window(start_at: datetime, config: RetentionConfig) -> tuple[datetime, datetime]:
    window_start = start_at - timedelta(minutes=config.culto_antecedencia_min)
    window_end = start_at + timedelta(minutes=config.culto_duracao_min)
    return window_start, window_end


def _candidate_service_windows(
    event_ts: datetime, day_of_week: int, start_hhmm: str, config: RetentionConfig
) -> list[tuple[datetime, datetime, datetime]]:
    hh, mm = map(int, start_hhmm.split(":"))
    day_delta = day_of_week - event_ts.weekday()
    base_date = (event_ts + timedelta(days=day_delta)).date()
    candidates: list[tuple[datetime, datetime, datetime]] = []
    for week_shift in (-7, 0, 7):
        start_at = datetime.combine(
            base_date + timedelta(days=week_shift),
            time(hour=hh, minute=mm),
            tzinfo=event_ts.tzinfo,
        )
        window_start, window_end = _build_culto_window(start_at=start_at, config=config)
        candidates.append((window_start, start_at, window_end))
    return candidates


def resolve_active_service(event_ts: datetime) -> dict[str, Any] | None:
    config = load_config()
    event_ts = event_ts.astimezone()
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, service_name, day_of_week, start_time
            FROM service_schedules
            WHERE is_active = 1
            ORDER BY id
            """
        ).fetchall()

    candidates: list[dict[str, Any]] = []
    for row in rows:
        for window_start, service_start, window_end in _candidate_service_windows(
            event_ts=event_ts,
            day_of_week=int(row["day_of_week"]),
            start_hhmm=row["start_time"],
            config=config,
        ):
            if window_start <= event_ts <= window_end:
                culto_id = f"{service_start:%Y%m%d}_{row['id']}"
                candidates.append(
                    {
                        "culto_id": culto_id,
                        "service_id": int(row["id"]),
                        "service_name": row["service_name"],
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                        "service_start": service_start.isoformat(),
                        "start_time": row["start_time"],
                    }
                )
    if not candidates:
        return None
    # Prefer the closest service start to event_ts when overlapping windows happen.
    candidates.sort(
        key=lambda item: abs(datetime.fromisoformat(item["service_start"]) - event_ts)
    )
    selected = candidates[0]
    selected.pop("service_start", None)
    return selected


def agenda_display_context(event_ts: datetime) -> dict[str, Any]:
    """
    Contexto de UI a partir da agenda: nome do culto, se esta no horario programado
    e report_culto_id (chave sintetica). Eventos de deteccao gravam so horario; o culto
    e sempre derivado de event_ts + agenda para exibicao, graficos e particoes agregadas.
    """
    event_ts = event_ts.astimezone()
    service = resolve_active_service(event_ts)
    if service is not None:
        return {
            "service_name": service["service_name"],
            "scheduled": True,
            "report_culto_id": service["culto_id"],
        }
    return {
        "service_name": "Fora da agenda",
        "scheduled": False,
        "report_culto_id": None,
    }


def derive_report_culto_id_for_event_ts(event_ts_raw: str) -> str:
    """Chave sintetica AAAAMMDD_scheduleId para planilhas, a partir do horario do evento."""
    try:
        dt = datetime.fromisoformat(str(event_ts_raw).replace("Z", "+00:00"))
    except ValueError:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    svc = resolve_active_service(dt.astimezone())
    return str(svc["culto_id"]) if svc else ""


def _inc_age_band(stats_row: sqlite3.Row | None, age_band: AgeBand | None) -> dict[str, int]:
    current = {
        "crianca_count": int(stats_row["crianca_count"]) if stats_row else 0,
        "junior_count": int(stats_row["junior_count"]) if stats_row else 0,
        "adolescente_count": int(stats_row["adolescente_count"]) if stats_row else 0,
        "jovem_count": int(stats_row["jovem_count"]) if stats_row else 0,
        "adulto_count": int(stats_row["adulto_count"]) if stats_row else 0,
        "idoso_count": int(stats_row["idoso_count"]) if stats_row else 0,
    }
    if age_band == "crianca":
        current["crianca_count"] += 1
    elif age_band == "junior":
        current["junior_count"] += 1
    elif age_band == "adolescente":
        current["adolescente_count"] += 1
    elif age_band == "jovem":
        current["jovem_count"] += 1
    elif age_band == "adulto":
        current["adulto_count"] += 1
    elif age_band == "idoso":
        current["idoso_count"] += 1
    return current


def _inc_gender_band(
    stats_row: sqlite3.Row | None, gender: GenderBand | None
) -> dict[str, int]:
    current = {
        "homem_count": int(stats_row["homem_count"]) if stats_row else 0,
        "mulher_count": int(stats_row["mulher_count"]) if stats_row else 0,
    }
    if gender == "homem":
        current["homem_count"] += 1
    elif gender == "mulher":
        current["mulher_count"] += 1
    return current


def _resolve_age_band_from_estimate(
    age_estimate: int | None, config: RetentionConfig
) -> AgeBand | None:
    if age_estimate is None:
        return None
    if age_estimate <= config.idade_limite_crianca:
        return "crianca"
    if age_estimate <= config.idade_limite_junior:
        return "junior"
    if age_estimate <= config.idade_limite_adolescente:
        return "adolescente"
    if age_estimate <= config.idade_limite_jovem:
        return "jovem"
    if age_estimate <= config.idade_limite_adulto:
        return "adulto"
    return "idoso"


def ingest_event(payload: EventIngestRequest) -> dict[str, Any]:
    event_ts = payload.event_ts or datetime.now(UTC)
    event_ts = event_ts.astimezone()
    config = load_config()
    display = agenda_display_context(event_ts)
    event_ts_s = event_ts.isoformat()
    event_id = str(uuid.uuid4())
    gender_to_use = payload.gender if config.estimar_genero else None
    if config.estimar_faixa_etaria:
        age_band_to_use = payload.age_band or _resolve_age_band_from_estimate(
            payload.age_estimate, config
        )
    else:
        age_band_to_use = None
    event_gender = gender_to_use if config.estimar_genero else None

    raw_culto = derive_report_culto_id_for_event_ts(event_ts_s)
    persist_culto_id = raw_culto.strip() if raw_culto else None
    targets: list[str] = [GLOBAL_STATS_ID]
    if persist_culto_id:
        targets.append(persist_culto_id)

    response_partition = persist_culto_id or GLOBAL_STATS_ID
    response_is_return = False
    response_is_new_unique = False

    with get_connection() as conn:
        for partition in targets:
            person = conn.execute(
                """
                SELECT *
                FROM service_event_people
                WHERE culto_id = ? AND person_id = ?
                """,
                (partition, payload.person_id),
            ).fetchone()

            is_new_unique = person is None
            is_return = False
            if person is None:
                if payload.direction == "entrada":
                    entries_count = 1
                    exits_count = 0
                    returns_count = 0
                    last_exit_at = None
                else:
                    entries_count = 0
                    exits_count = 1
                    returns_count = 0
                    last_exit_at = event_ts_s
                conn.execute(
                    """
                    INSERT INTO service_event_people (
                        culto_id, person_id, first_seen_at, last_seen_at,
                        entries_count, exits_count, returns_count, age_band, gender, last_direction, last_exit_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        partition,
                        payload.person_id,
                        event_ts_s,
                        event_ts_s,
                        entries_count,
                        exits_count,
                        returns_count,
                        age_band_to_use,
                        gender_to_use,
                        payload.direction,
                        last_exit_at,
                    ),
                )
            else:
                entries_count = int(person["entries_count"])
                exits_count = int(person["exits_count"])
                returns_count = int(person["returns_count"])
                last_direction = person["last_direction"]
                last_exit_at = person["last_exit_at"]
                age_band = (
                    (person["age_band"] or age_band_to_use)
                    if config.estimar_faixa_etaria
                    else None
                )
                gender = (
                    (person["gender"] or gender_to_use) if config.estimar_genero else None
                )
                if payload.direction == "entrada":
                    entries_count += 1
                    if last_direction == "saida" and last_exit_at:
                        try:
                            delta = event_ts - datetime.fromisoformat(last_exit_at)
                            if timedelta(minutes=0) <= delta <= timedelta(
                                minutes=config.janela_reentrada_min
                            ):
                                returns_count += 1
                                is_return = True
                        except ValueError:
                            pass
                else:
                    exits_count += 1
                    last_exit_at = event_ts_s

                conn.execute(
                    """
                    UPDATE service_event_people
                    SET last_seen_at = ?,
                        entries_count = ?,
                        exits_count = ?,
                        returns_count = ?,
                        age_band = ?,
                        gender = ?,
                        last_direction = ?,
                        last_exit_at = ?
                    WHERE culto_id = ? AND person_id = ?
                    """,
                    (
                        event_ts_s,
                        entries_count,
                        exits_count,
                        returns_count,
                        age_band,
                        gender,
                        payload.direction,
                        last_exit_at,
                        partition,
                        payload.person_id,
                    ),
                )

            if partition == response_partition:
                response_is_return = is_return
                response_is_new_unique = is_new_unique

            stats = conn.execute(
                "SELECT * FROM service_event_stats WHERE culto_id = ?",
                (partition,),
            ).fetchone()
            entries = int(stats["entries_count"]) if stats else 0
            exits = int(stats["exits_count"]) if stats else 0
            returns = int(stats["returns_count"]) if stats else 0
            unique_count = int(stats["unique_people_count"]) if stats else 0
            current_occupancy = int(stats["current_occupancy"]) if stats else 0
            peak_occupancy = int(stats["peak_occupancy"]) if stats else 0

            if payload.direction == "entrada":
                entries += 1
                current_occupancy += 1
                if is_return:
                    returns += 1
            else:
                exits += 1
                current_occupancy = max(0, current_occupancy - 1)
            if is_new_unique and payload.direction == "entrada":
                unique_count += 1
            peak_occupancy = max(peak_occupancy, current_occupancy)

            age_counts = _inc_age_band(
                stats, age_band_to_use if is_new_unique else None
            )
            gender_counts = _inc_gender_band(
                stats, gender_to_use if is_new_unique else None
            )

            conn.execute(
                """
                INSERT INTO service_event_stats (
                    culto_id, entries_count, exits_count, returns_count, unique_people_count,
                    current_occupancy, peak_occupancy, crianca_count, junior_count,
                    adolescente_count, jovem_count, adulto_count, idoso_count,
                    homem_count, mulher_count, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(culto_id) DO UPDATE SET
                    entries_count = excluded.entries_count,
                    exits_count = excluded.exits_count,
                    returns_count = excluded.returns_count,
                    unique_people_count = excluded.unique_people_count,
                    current_occupancy = excluded.current_occupancy,
                    peak_occupancy = excluded.peak_occupancy,
                    crianca_count = excluded.crianca_count,
                    junior_count = excluded.junior_count,
                    adolescente_count = excluded.adolescente_count,
                    jovem_count = excluded.jovem_count,
                    adulto_count = excluded.adulto_count,
                    idoso_count = excluded.idoso_count,
                    homem_count = excluded.homem_count,
                    mulher_count = excluded.mulher_count,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    partition,
                    entries,
                    exits,
                    returns,
                    unique_count,
                    current_occupancy,
                    peak_occupancy,
                    age_counts["crianca_count"],
                    age_counts["junior_count"],
                    age_counts["adolescente_count"],
                    age_counts["jovem_count"],
                    age_counts["adulto_count"],
                    age_counts["idoso_count"],
                    gender_counts["homem_count"],
                    gender_counts["mulher_count"],
                ),
            )

        event_age_band = age_band_to_use if config.estimar_faixa_etaria else None
        conn.execute(
            """
            INSERT INTO events (event_id, culto_id, profile_id, temp_id, event_type, event_ts, age_band, gender)
            VALUES (?, NULL, NULL, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                payload.person_id,
                payload.direction,
                event_ts_s,
                event_age_band,
                event_gender,
            ),
        )
        conn.commit()

    invalidate_involvement_summary_cache()

    return {
        "event_id": event_id,
        "culto_id": None,
        "report_culto_id": display["report_culto_id"],
        "service_name": display["service_name"],
        "scheduled": display["scheduled"],
        "direction": payload.direction,
        "is_return": response_is_return,
        "is_new_unique": response_is_new_unique,
        "age_band_used": event_age_band,
        "gender_used": event_gender,
    }


def _camera_detection_status() -> dict[str, Any]:
    """Estado da captura/HOG para o dashboard (a deteccao nao depende da agenda)."""
    cfg = load_config()
    opencv = False
    try:
        import cv2  # noqa: F401

        opencv = True
    except ImportError:
        pass
    return {
        "camera_enabled": cfg.camera_enabled,
        "live_detection_enabled": cfg.live_detection_enabled,
        "opencv_installed": opencv,
    }


def _resolve_live_partition_id(
    culto_id_param: str | None, display: dict[str, Any]
) -> str:
    """Particao de stats/charts: query explícita, ou culto da agenda, ou agregado global."""
    req = (culto_id_param or "").strip()
    if req:
        return req
    if display.get("scheduled") and display.get("report_culto_id"):
        return str(display["report_culto_id"])
    return GLOBAL_STATS_ID


def get_live_metrics(
    culto_id: str | None = None, reference_time: datetime | None = None
) -> dict[str, Any]:
    now = (
        reference_time.astimezone()
        if reference_time is not None
        else datetime.now(UTC).astimezone()
    )
    display = agenda_display_context(now)
    partition = _resolve_live_partition_id(culto_id, display)
    cam_det = _camera_detection_status()
    stats_scope = "culto" if partition != GLOBAL_STATS_ID else "global"
    global_stats_fallback = False

    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM service_event_stats WHERE culto_id = ?",
            (partition,),
        ).fetchone()
        # Eventos fora da janela do culto na agenda so incrementam __global__; dentro
        # da janela o painel pedia a particao do culto e via linha inexistente -> zeros.
        if row is None and partition != GLOBAL_STATS_ID:
            row = conn.execute(
                "SELECT * FROM service_event_stats WHERE culto_id = ?",
                (GLOBAL_STATS_ID,),
            ).fetchone()
            if row is not None:
                global_stats_fallback = True
                stats_scope = "global"

    involvement = get_involvement_summary_for_live_metrics()

    culto_id_out: str | None = None
    if partition != GLOBAL_STATS_ID and not global_stats_fallback:
        culto_id_out = partition

    base = {
        "active": True,
        "culto_id": culto_id_out,
        "stats_scope": stats_scope,
        "global_stats_fallback": global_stats_fallback,
        "camera_detection": cam_det,
        "report_culto_id": display["report_culto_id"],
        "service_name": display["service_name"],
        "scheduled": display["scheduled"],
        "involvement": involvement,
    }

    if row is None:
        return {
            **base,
            "entries_count": 0,
            "exits_count": 0,
            "returns_count": 0,
            "unique_people_count": 0,
            "current_occupancy": 0,
            "peak_occupancy": 0,
            "age_bands": {
                "crianca": 0,
                "junior": 0,
                "adolescente": 0,
                "jovem": 0,
                "adulto": 0,
                "idoso": 0,
            },
            "genders": {
                "homem": 0,
                "mulher": 0,
            },
        }

    return {
        **base,
        "entries_count": int(row["entries_count"]),
        "exits_count": int(row["exits_count"]),
        "returns_count": int(row["returns_count"]),
        "unique_people_count": int(row["unique_people_count"]),
        "current_occupancy": int(row["current_occupancy"]),
        "peak_occupancy": int(row["peak_occupancy"]),
        "age_bands": {
            "crianca": int(row["crianca_count"]),
            "junior": int(row["junior_count"]),
            "adolescente": int(row["adolescente_count"]),
            "jovem": int(row["jovem_count"]),
            "adulto": int(row["adulto_count"]),
            "idoso": int(row["idoso_count"]),
        },
        "genders": {
            "homem": int(row["homem_count"]),
            "mulher": int(row["mulher_count"]),
        },
    }


def _parse_event_ts_iso(ts_raw: str) -> datetime | None:
    try:
        dt = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone()


def _filter_event_rows_by_chart_partition(
    rows: list[sqlite3.Row], chart_partition: str
) -> list[sqlite3.Row]:
    if chart_partition == GLOBAL_STATS_ID:
        return list(rows)
    out: list[sqlite3.Row] = []
    for r in rows:
        if (
            derive_report_culto_id_for_event_ts(str(r["event_ts"])).strip()
            == chart_partition
        ):
            out.append(r)
    return out


def _demographics_from_window_event_rows(
    window_rows: list[sqlite3.Row],
) -> tuple[dict[str, int], dict[str, int]]:
    """Unicos com pelo menos uma entrada na janela; faixa/sexo = primeiro valor visto nos eventos da janela."""
    people: dict[str, dict[str, Any]] = {}
    for r in window_rows:
        pid = str(r["temp_id"] or "").strip()
        if not pid:
            continue
        p = people.setdefault(
            pid, {"entrada": False, "age_band": None, "gender": None}
        )
        ab = r["age_band"]
        g = r["gender"]
        if ab and not p["age_band"]:
            p["age_band"] = ab
        if g and not p["gender"]:
            p["gender"] = g
        if str(r["event_type"]) == "entrada":
            p["entrada"] = True
    age_bands = {
        "crianca": 0,
        "junior": 0,
        "adolescente": 0,
        "jovem": 0,
        "adulto": 0,
        "idoso": 0,
    }
    genders = {"homem": 0, "mulher": 0}
    age_ok = {
        "crianca",
        "junior",
        "adolescente",
        "jovem",
        "adulto",
        "idoso",
    }
    for p in people.values():
        if not p["entrada"]:
            continue
        ab = p["age_band"]
        if ab in age_ok:
            age_bands[str(ab)] += 1
        ge = p["gender"]
        if ge == "homem":
            genders["homem"] += 1
        elif ge == "mulher":
            genders["mulher"] += 1
    return age_bands, genders


def _returns_in_window(
    window_rows: list[sqlite3.Row], janela_reentrada_min: int
) -> int:
    """Reentradas na janela: entrada apos saida dentro de N minutos (mesmo temp_id)."""
    by_pid: dict[str, list[tuple[datetime, str]]] = {}
    for r in window_rows:
        pid = str(r["temp_id"] or "").strip()
        if not pid:
            continue
        ts = _parse_event_ts_iso(str(r["event_ts"]))
        if ts is None:
            continue
        by_pid.setdefault(pid, []).append((ts, str(r["event_type"])))

    returns = 0
    for events in by_pid.values():
        events.sort(key=lambda x: x[0])
        last_exit: datetime | None = None
        for ts, et in events:
            if et == "saida":
                last_exit = ts
            elif et == "entrada":
                if last_exit is not None:
                    delta = ts - last_exit
                    if timedelta(minutes=0) <= delta <= timedelta(
                        minutes=janela_reentrada_min
                    ):
                        returns += 1
                    last_exit = None
                else:
                    last_exit = None
    return returns


def get_dashboard_charts(
    culto_id: str | None = None,
    window_minutes: int = 180,
    bucket_seconds: int = 300,
    center: datetime | None = None,
) -> dict[str, Any]:
    # Padrao 5 min para leitura de chegadas no grafico; minimo 5 min.
    safe_bucket = max(300, min(bucket_seconds, 3600))
    now_wall = datetime.now(UTC).astimezone()
    range_mode = center is not None
    if range_mode:
        safe_window = 180
        ref = center.astimezone()  # type: ignore[union-attr]
        half = timedelta(minutes=90)
        window_start_dt = ref - half
        window_end_dt = ref + half
    else:
        safe_window = max(30, min(window_minutes, 24 * 60))
        ref = now_wall
        window_start_dt = None
        window_end_dt = None

    display = agenda_display_context(ref)
    partition = _resolve_live_partition_id(culto_id, display)
    live = get_live_metrics(culto_id=culto_id, reference_time=ref)
    chart_partition = (
        GLOBAL_STATS_ID if live.get("global_stats_fallback") else partition
    )
    if not live.get("active"):
        return {
            "active": False,
            "charts": {
                "flow_per_minute": [],
                "occupancy_series": [],
            },
            "summary": {
                "entries_count": 0,
                "exits_count": 0,
                "returns_count": 0,
                "current_occupancy": 0,
                "peak_occupancy": 0,
            },
            "age_bands": {
                "crianca": 0,
                "junior": 0,
                "adolescente": 0,
                "jovem": 0,
                "adulto": 0,
                "idoso": 0,
            },
            "genders": {"homem": 0, "mulher": 0},
        }

    if range_mode:
        assert window_start_dt is not None and window_end_dt is not None
        cfg = load_config()
        janela_re = int(cfg.janela_reentrada_min)
        w_end_s = window_end_dt.isoformat()
        tz = window_start_dt.tzinfo or UTC

        with get_connection() as conn:
            raw_rows = conn.execute(
                """
                SELECT event_type, event_ts, temp_id, age_band, gender
                FROM events
                WHERE event_ts <= ?
                ORDER BY event_ts ASC, id ASC
                """,
                (w_end_s,),
            ).fetchall()

        filtered = _filter_event_rows_by_chart_partition(raw_rows, chart_partition)

        occ0 = 0
        window_rows: list[sqlite3.Row] = []
        entries_w = 0
        exits_w = 0
        unique_ids: set[str] = set()

        for r in filtered:
            ts = _parse_event_ts_iso(str(r["event_ts"]))
            if ts is None:
                continue
            et = str(r["event_type"])
            if ts < window_start_dt:
                if et == "entrada":
                    occ0 += 1
                elif et == "saida":
                    occ0 = max(0, occ0 - 1)
            elif ts <= window_end_dt:
                window_rows.append(r)
                if et == "entrada":
                    entries_w += 1
                    pid = str(r["temp_id"] or "").strip()
                    if pid:
                        unique_ids.add(pid)
                elif et == "saida":
                    exits_w += 1

        age_bands_w, genders_w = _demographics_from_window_event_rows(window_rows)
        returns_w = _returns_in_window(window_rows, janela_re)

        events: list[tuple[datetime, str]] = []
        for r in window_rows:
            ts = _parse_event_ts_iso(str(r["event_ts"]))
            if ts is None:
                continue
            events.append((ts, str(r["event_type"])))

        base_epoch = int(window_start_dt.timestamp())
        bucket_start_epoch = base_epoch - (base_epoch % safe_bucket)

        buckets: dict[int, dict[str, Any]] = {}
        occupancy = occ0
        peak = occ0
        for ts, direction in events:
            epoch = int(ts.timestamp())
            bucket_epoch = epoch - (epoch % safe_bucket)
            info = buckets.setdefault(
                bucket_epoch,
                {
                    "ts": datetime.fromtimestamp(bucket_epoch, tz).isoformat(),
                    "entries": 0,
                    "exits": 0,
                    "occupancy": 0,
                },
            )
            if direction == "entrada":
                info["entries"] += 1
                occupancy += 1
                peak = max(peak, occupancy)
            elif direction == "saida":
                info["exits"] += 1
                occupancy = max(0, occupancy - 1)
            info["occupancy"] = occupancy

        series: list[dict[str, Any]] = []
        cursor = bucket_start_epoch
        end_epoch = int(window_end_dt.timestamp())
        last_occ = occ0
        while cursor <= end_epoch:
            slot = buckets.get(cursor)
            if slot is None:
                slot = {
                    "ts": datetime.fromtimestamp(cursor, tz).isoformat(),
                    "entries": 0,
                    "exits": 0,
                    "occupancy": last_occ,
                }
            else:
                last_occ = int(slot["occupancy"])
            series.append(slot)
            cursor += safe_bucket

        current_occ_end = occupancy

        return {
            "active": True,
            "range_mode": True,
            "center": ref.isoformat(),
            "window_start": window_start_dt.isoformat(),
            "window_end": window_end_dt.isoformat(),
            "culto_id": live.get("culto_id"),
            "report_culto_id": live.get("report_culto_id"),
            "service_name": live.get("service_name"),
            "scheduled": live.get("scheduled", True),
            "window_minutes": safe_window,
            "bucket_seconds": safe_bucket,
            "charts": {
                "flow_per_minute": [
                    {
                        "ts": item["ts"],
                        "entries": int(item["entries"]),
                        "exits": int(item["exits"]),
                    }
                    for item in series
                ],
                "occupancy_series": [
                    {
                        "ts": item["ts"],
                        "occupancy": int(item["occupancy"]),
                    }
                    for item in series
                ],
            },
            "summary": {
                "entries_count": entries_w,
                "exits_count": exits_w,
                "returns_count": returns_w,
                "unique_people_count": len(unique_ids),
                "current_occupancy": current_occ_end,
                "peak_occupancy": peak,
            },
            "age_bands": age_bands_w,
            "genders": genders_w,
        }

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT event_type, event_ts
            FROM events
            WHERE julianday(event_ts) >= julianday('now', ?)
            ORDER BY julianday(event_ts) ASC, id ASC
            """,
            (f"-{safe_window} minutes",),
        ).fetchall()
    if chart_partition != GLOBAL_STATS_ID:
        rows = [
            r
            for r in rows
            if derive_report_culto_id_for_event_ts(str(r["event_ts"])).strip()
            == chart_partition
        ]

    if not rows:
        return {
            "active": True,
            "range_mode": False,
            "culto_id": live.get("culto_id"),
            "report_culto_id": live.get("report_culto_id"),
            "service_name": live.get("service_name"),
            "scheduled": live.get("scheduled", True),
            "window_minutes": safe_window,
            "bucket_seconds": safe_bucket,
            "charts": {
                "flow_per_minute": [],
                "occupancy_series": [],
            },
            "summary": {
                "entries_count": int(live["entries_count"]),
                "exits_count": int(live["exits_count"]),
                "returns_count": int(live["returns_count"]),
                "current_occupancy": int(live["current_occupancy"]),
                "peak_occupancy": int(live["peak_occupancy"]),
            },
            "age_bands": live.get("age_bands", {}),
            "genders": live.get("genders", {}),
        }

    events_live: list[tuple[datetime, str]] = []
    for row in rows:
        try:
            ts = datetime.fromisoformat(str(row["event_ts"])).astimezone()
        except ValueError:
            continue
        events_live.append((ts, str(row["event_type"])))
    if not events_live:
        return {
            "active": True,
            "range_mode": False,
            "culto_id": live.get("culto_id"),
            "report_culto_id": live.get("report_culto_id"),
            "service_name": live.get("service_name"),
            "scheduled": live.get("scheduled", True),
            "window_minutes": safe_window,
            "bucket_seconds": safe_bucket,
            "charts": {
                "flow_per_minute": [],
                "occupancy_series": [],
            },
            "summary": {
                "entries_count": int(live["entries_count"]),
                "exits_count": int(live["exits_count"]),
                "returns_count": int(live["returns_count"]),
                "current_occupancy": int(live["current_occupancy"]),
                "peak_occupancy": int(live["peak_occupancy"]),
            },
            "age_bands": live.get("age_bands", {}),
            "genders": live.get("genders", {}),
        }

    first_ts = events_live[0][0]
    now_ts = datetime.now(first_ts.tzinfo)
    window_start = now_ts - timedelta(minutes=safe_window)
    series_start = min(first_ts, window_start)
    base_epoch = int(series_start.timestamp())
    bucket_start_epoch = base_epoch - (base_epoch % safe_bucket)

    buckets: dict[int, dict[str, Any]] = {}
    occupancy = 0
    peak = 0
    for ts, direction in events_live:
        epoch = int(ts.timestamp())
        bucket_epoch = epoch - (epoch % safe_bucket)
        info = buckets.setdefault(
            bucket_epoch,
            {
                "ts": datetime.fromtimestamp(bucket_epoch, ts.tzinfo).isoformat(),
                "entries": 0,
                "exits": 0,
                "occupancy": 0,
            },
        )
        if direction == "entrada":
            info["entries"] += 1
            occupancy += 1
            peak = max(peak, occupancy)
        elif direction == "saida":
            info["exits"] += 1
            occupancy = max(0, occupancy - 1)
        info["occupancy"] = occupancy

    # Fill missing buckets for smoother chart line.
    series: list[dict[str, Any]] = []
    cursor = bucket_start_epoch
    end_epoch = int(now_ts.timestamp())
    last_occ = 0
    while cursor <= end_epoch:
        slot = buckets.get(cursor)
        if slot is None:
            slot = {
                "ts": datetime.fromtimestamp(cursor, now_ts.tzinfo).isoformat(),
                "entries": 0,
                "exits": 0,
                "occupancy": last_occ,
            }
        else:
            last_occ = int(slot["occupancy"])
        series.append(slot)
        cursor += safe_bucket

    return {
        "active": True,
        "range_mode": False,
        "culto_id": live.get("culto_id"),
        "report_culto_id": live.get("report_culto_id"),
        "service_name": live.get("service_name"),
        "scheduled": live.get("scheduled", True),
        "window_minutes": safe_window,
        "bucket_seconds": safe_bucket,
        "charts": {
            "flow_per_minute": [
                {
                    "ts": item["ts"],
                    "entries": int(item["entries"]),
                    "exits": int(item["exits"]),
                }
                for item in series
            ],
            "occupancy_series": [
                {
                    "ts": item["ts"],
                    "occupancy": int(item["occupancy"]),
                }
                for item in series
            ],
        },
        "summary": {
            "entries_count": int(live["entries_count"]),
            "exits_count": int(live["exits_count"]),
            "returns_count": int(live["returns_count"]),
            "current_occupancy": int(live["current_occupancy"]),
            "peak_occupancy": int(max(peak, int(live["peak_occupancy"]))),
        },
        "age_bands": live.get("age_bands", {}),
        "genders": live.get("genders", {}),
    }
