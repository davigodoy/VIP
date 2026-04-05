from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "app.db"


DEFAULT_CONFIG: dict[str, Any] = {
    "retencao_temp_id_horas": 6,
    "retencao_profile_dias": 90,
    "retencao_eventos_dias": 180,
    "retencao_agregados_meses": 24,
    "retencao_imagens_horas": 0,
    "janela_reentrada_min": 15,
    "limiar_match": 0.75,
    "auto_cleanup_enabled": 1,
    "auto_cleanup_hour": 3,
    "camera_device": "/dev/video0",
    "camera_label": "Entrada principal",
    "camera_enabled": 1,
    "camera_inference_width": 640,
    "camera_inference_height": 360,
    "camera_fps": 8,
    "culto_antecedencia_min": 30,
    "culto_duracao_min": 150,
    "estimar_faixa_etaria": 1,
    "estimar_genero": 1,
    "sync_google_sheets_enabled": 0,
    "sync_interval_sec": 60,
    "sync_spreadsheet_id": "",
    "sync_worksheet_name": "Eventos",
    "sync_credentials_source": "env",
    "sync_credentials_env_var": "VIP_GSHEETS_CREDENTIALS_JSON",
    "sync_credentials_file_path": "",
    "sync_credentials_json": "",
    "idade_limite_crianca": 11,
    "idade_limite_junior": 14,
    "idade_limite_adolescente": 17,
    "idade_limite_jovem": 24,
    "idade_limite_adulto": 59,
}


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS temp_tracks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                temp_id TEXT NOT NULL,
                culto_id TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_id TEXT NOT NULL UNIQUE,
                first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                seen_count INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                culto_id TEXT,
                profile_id TEXT,
                temp_id TEXT,
                event_type TEXT NOT NULL,
                event_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                age_band TEXT,
                gender TEXT
            );

            CREATE TABLE IF NOT EXISTS aggregated_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                culto_id TEXT NOT NULL,
                service_started_at TEXT NOT NULL,
                unique_count INTEGER NOT NULL,
                returns_count INTEGER NOT NULL,
                exits_count INTEGER NOT NULL,
                peak_occupancy INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL,
                captured_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS cleanup_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                dry_run INTEGER NOT NULL,
                result_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS service_schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_name TEXT NOT NULL,
                day_of_week INTEGER NOT NULL CHECK(day_of_week BETWEEN 0 AND 6),
                start_time TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS service_event_people (
                person_id TEXT NOT NULL PRIMARY KEY,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                entries_count INTEGER NOT NULL DEFAULT 0,
                exits_count INTEGER NOT NULL DEFAULT 0,
                returns_count INTEGER NOT NULL DEFAULT 0,
                age_band TEXT,
                gender TEXT,
                last_direction TEXT NOT NULL DEFAULT 'entrada',
                last_exit_at TEXT
            );

            CREATE TABLE IF NOT EXISTS service_event_stats (
                culto_id TEXT PRIMARY KEY,
                entries_count INTEGER NOT NULL DEFAULT 0,
                exits_count INTEGER NOT NULL DEFAULT 0,
                returns_count INTEGER NOT NULL DEFAULT 0,
                unique_people_count INTEGER NOT NULL DEFAULT 0,
                current_occupancy INTEGER NOT NULL DEFAULT 0,
                peak_occupancy INTEGER NOT NULL DEFAULT 0,
                crianca_count INTEGER NOT NULL DEFAULT 0,
                junior_count INTEGER NOT NULL DEFAULT 0,
                adolescente_count INTEGER NOT NULL DEFAULT 0,
                jovem_count INTEGER NOT NULL DEFAULT 0,
                adulto_count INTEGER NOT NULL DEFAULT 0,
                idoso_count INTEGER NOT NULL DEFAULT 0,
                homem_count INTEGER NOT NULL DEFAULT 0,
                mulher_count INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                status TEXT NOT NULL,
                rows_synced INTEGER NOT NULL DEFAULT 0,
                message TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS reconciliation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT UNIQUE,
                run_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                started_at TEXT,
                finished_at TEXT,
                status TEXT NOT NULL DEFAULT 'queued',
                progress_pct INTEGER NOT NULL DEFAULT 0,
                total_events INTEGER NOT NULL DEFAULT 0,
                processed_events INTEGER NOT NULL DEFAULT 0,
                total_services INTEGER NOT NULL DEFAULT 0,
                processed_services INTEGER NOT NULL DEFAULT 0,
                duration_sec INTEGER NOT NULL DEFAULT 0,
                scanned_events INTEGER NOT NULL DEFAULT 0,
                touched_cultos INTEGER NOT NULL DEFAULT 0,
                changed_rows INTEGER NOT NULL DEFAULT 0,
                message TEXT NOT NULL DEFAULT '',
                result_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS reconciliation_state (
                state_key TEXT PRIMARY KEY,
                run_id TEXT,
                status TEXT NOT NULL DEFAULT 'idle',
                progress_pct INTEGER NOT NULL DEFAULT 0,
                processed_events INTEGER NOT NULL DEFAULT 0,
                total_events INTEGER NOT NULL DEFAULT 0,
                processed_services INTEGER NOT NULL DEFAULT 0,
                total_services INTEGER NOT NULL DEFAULT 0,
                message TEXT NOT NULL DEFAULT '',
                started_at TEXT,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS update_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT UNIQUE,
                started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                finished_at TEXT,
                status TEXT NOT NULL DEFAULT 'queued',
                progress_pct INTEGER NOT NULL DEFAULT 0,
                current_step TEXT NOT NULL DEFAULT '',
                message TEXT NOT NULL DEFAULT '',
                output_log TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS update_state (
                state_key TEXT PRIMARY KEY,
                run_id TEXT,
                status TEXT NOT NULL DEFAULT 'idle',
                progress_pct INTEGER NOT NULL DEFAULT 0,
                current_step TEXT NOT NULL DEFAULT '',
                message TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        _run_migrations(conn)

        for key, value in DEFAULT_CONFIG.items():
            conn.execute(
                """
                INSERT INTO config (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO NOTHING
                """,
                (key, str(value)),
            )

        schedule_count = conn.execute(
            "SELECT COUNT(*) AS c FROM service_schedules"
        ).fetchone()["c"]
        if schedule_count == 0:
            conn.executemany(
                """
                INSERT INTO service_schedules (service_name, day_of_week, start_time, is_active)
                VALUES (?, ?, ?, ?)
                """,
                [
                    ("Quarta Noite", 2, "19:30", 1),
                    ("Sabado Noite", 5, "19:00", 1),
                    ("Domingo Manha", 6, "09:00", 1),
                    ("Domingo Noite", 6, "19:00", 1),
                ],
            )
        conn.commit()


def _migrate_global_operational_schema(conn: sqlite3.Connection) -> None:
    """
    Eventos deixam de persistir culto_id; estado ao vivo e por pessoa sao globais.
    Executa uma vez quando detecta schema legado (NOT NULL em events.culto_id ou
    service_event_people.culto_id).
    """
    ev_info = {
        row["name"]: row for row in conn.execute("PRAGMA table_info(events)").fetchall()
    }
    people_info = list(conn.execute("PRAGMA table_info(service_event_people)").fetchall())
    people_names = {row["name"] for row in people_info}

    migrate_events = "culto_id" in ev_info and int(ev_info["culto_id"]["notnull"]) == 1
    migrate_people = "culto_id" in people_names

    if not migrate_events and not migrate_people:
        return

    if migrate_events:
        conn.executescript(
            """
            CREATE TABLE events__mg (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                culto_id TEXT,
                profile_id TEXT,
                temp_id TEXT,
                event_type TEXT NOT NULL,
                event_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                age_band TEXT,
                gender TEXT
            );
            INSERT INTO events__mg (
                id, event_id, culto_id, profile_id, temp_id, event_type, event_ts, age_band, gender
            )
            SELECT id, event_id, NULL, profile_id, temp_id, event_type, event_ts, age_band, gender
            FROM events;
            DROP TABLE events;
            ALTER TABLE events__mg RENAME TO events;
            """
        )

    if migrate_people:
        conn.execute("DROP INDEX IF EXISTS idx_service_event_people_unique")
        conn.executescript(
            """
            CREATE TABLE service_event_people__mg (
                person_id TEXT NOT NULL PRIMARY KEY,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                entries_count INTEGER NOT NULL DEFAULT 0,
                exits_count INTEGER NOT NULL DEFAULT 0,
                returns_count INTEGER NOT NULL DEFAULT 0,
                age_band TEXT,
                gender TEXT,
                last_direction TEXT NOT NULL DEFAULT 'entrada',
                last_exit_at TEXT
            );
            DROP TABLE service_event_people;
            ALTER TABLE service_event_people__mg RENAME TO service_event_people;
            """
        )

    conn.execute("DELETE FROM service_event_stats")


def _run_migrations(conn: sqlite3.Connection) -> None:
    _migrate_global_operational_schema(conn)

    event_cols = {
        row["name"] for row in conn.execute("PRAGMA table_info(events)").fetchall()
    }
    if "age_band" not in event_cols:
        conn.execute("ALTER TABLE events ADD COLUMN age_band TEXT")
    if "gender" not in event_cols:
        conn.execute("ALTER TABLE events ADD COLUMN gender TEXT")

    people_cols = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(service_event_people)").fetchall()
    }
    if "last_direction" not in people_cols:
        conn.execute(
            "ALTER TABLE service_event_people ADD COLUMN last_direction TEXT NOT NULL DEFAULT 'entrada'"
        )
    if "last_exit_at" not in people_cols:
        conn.execute("ALTER TABLE service_event_people ADD COLUMN last_exit_at TEXT")
    if "gender" not in people_cols:
        conn.execute("ALTER TABLE service_event_people ADD COLUMN gender TEXT")

    stats_cols = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(service_event_stats)").fetchall()
    }
    if "adolescente_count" not in stats_cols:
        conn.execute(
            "ALTER TABLE service_event_stats ADD COLUMN adolescente_count INTEGER NOT NULL DEFAULT 0"
        )
    if "jovem_count" not in stats_cols:
        conn.execute(
            "ALTER TABLE service_event_stats ADD COLUMN jovem_count INTEGER NOT NULL DEFAULT 0"
        )
    if "adulto_count" not in stats_cols:
        conn.execute(
            "ALTER TABLE service_event_stats ADD COLUMN adulto_count INTEGER NOT NULL DEFAULT 0"
        )
    if "idoso_count" not in stats_cols:
        conn.execute(
            "ALTER TABLE service_event_stats ADD COLUMN idoso_count INTEGER NOT NULL DEFAULT 0"
        )
    if "homem_count" not in stats_cols:
        conn.execute(
            "ALTER TABLE service_event_stats ADD COLUMN homem_count INTEGER NOT NULL DEFAULT 0"
        )
    if "mulher_count" not in stats_cols:
        conn.execute(
            "ALTER TABLE service_event_stats ADD COLUMN mulher_count INTEGER NOT NULL DEFAULT 0"
        )
    if "adolescente_jovem_count" in stats_cols:
        conn.execute(
            """
            UPDATE service_event_stats
            SET jovem_count = jovem_count + adolescente_jovem_count
            WHERE adolescente_jovem_count > 0
            """
        )

    recon_cols = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(reconciliation_runs)").fetchall()
    }
    if "run_id" not in recon_cols:
        conn.execute("ALTER TABLE reconciliation_runs ADD COLUMN run_id TEXT")
    if "progress_pct" not in recon_cols:
        conn.execute(
            "ALTER TABLE reconciliation_runs ADD COLUMN progress_pct INTEGER NOT NULL DEFAULT 0"
        )
    if "scanned_events" not in recon_cols:
        conn.execute(
            "ALTER TABLE reconciliation_runs ADD COLUMN scanned_events INTEGER NOT NULL DEFAULT 0"
        )
    if "touched_cultos" not in recon_cols:
        conn.execute(
            "ALTER TABLE reconciliation_runs ADD COLUMN touched_cultos INTEGER NOT NULL DEFAULT 0"
        )
    if "changed_rows" not in recon_cols:
        conn.execute(
            "ALTER TABLE reconciliation_runs ADD COLUMN changed_rows INTEGER NOT NULL DEFAULT 0"
        )
    if "run_ts" not in recon_cols:
        conn.execute(
            "ALTER TABLE reconciliation_runs ADD COLUMN run_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
        )
    if "processed_services" not in recon_cols:
        conn.execute(
            "ALTER TABLE reconciliation_runs ADD COLUMN processed_services INTEGER NOT NULL DEFAULT 0"
        )
    if "total_services" not in recon_cols:
        conn.execute(
            "ALTER TABLE reconciliation_runs ADD COLUMN total_services INTEGER NOT NULL DEFAULT 0"
        )
    if "duration_sec" not in recon_cols:
        conn.execute(
            "ALTER TABLE reconciliation_runs ADD COLUMN duration_sec INTEGER NOT NULL DEFAULT 0"
        )
    if "run_ts" in recon_cols and "started_at" in recon_cols:
        conn.execute(
            """
            UPDATE reconciliation_runs
            SET run_ts = COALESCE(NULLIF(run_ts, ''), started_at, CURRENT_TIMESTAMP)
            """
        )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_reconciliation_runs_run_id ON reconciliation_runs (run_id)"
    )

    recon_state_cols = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(reconciliation_state)").fetchall()
    }
    if "processed_events" not in recon_state_cols:
        conn.execute(
            "ALTER TABLE reconciliation_state ADD COLUMN processed_events INTEGER NOT NULL DEFAULT 0"
        )
    if "total_events" not in recon_state_cols:
        conn.execute(
            "ALTER TABLE reconciliation_state ADD COLUMN total_events INTEGER NOT NULL DEFAULT 0"
        )
    if "processed_services" not in recon_state_cols:
        conn.execute(
            "ALTER TABLE reconciliation_state ADD COLUMN processed_services INTEGER NOT NULL DEFAULT 0"
        )
    if "total_services" not in recon_state_cols:
        conn.execute(
            "ALTER TABLE reconciliation_state ADD COLUMN total_services INTEGER NOT NULL DEFAULT 0"
        )
    if "processed_rows" in recon_state_cols:
        conn.execute(
            """
            UPDATE reconciliation_state
            SET processed_events = processed_rows
            WHERE processed_rows > processed_events
            """
        )
    if "total_rows" in recon_state_cols:
        conn.execute(
            """
            UPDATE reconciliation_state
            SET total_events = total_rows
            WHERE total_rows > total_events
            """
        )

    update_runs_cols = {
        row["name"] for row in conn.execute("PRAGMA table_info(update_runs)").fetchall()
    }
    if "run_id" not in update_runs_cols:
        conn.execute("ALTER TABLE update_runs ADD COLUMN run_id TEXT")
    if "started_at" not in update_runs_cols:
        conn.execute("ALTER TABLE update_runs ADD COLUMN started_at TEXT")
    if "finished_at" not in update_runs_cols:
        conn.execute("ALTER TABLE update_runs ADD COLUMN finished_at TEXT")
    if "status" not in update_runs_cols:
        conn.execute("ALTER TABLE update_runs ADD COLUMN status TEXT NOT NULL DEFAULT 'queued'")
    if "progress_pct" not in update_runs_cols:
        conn.execute(
            "ALTER TABLE update_runs ADD COLUMN progress_pct INTEGER NOT NULL DEFAULT 0"
        )
    if "current_step" not in update_runs_cols:
        conn.execute(
            "ALTER TABLE update_runs ADD COLUMN current_step TEXT NOT NULL DEFAULT ''"
        )
    if "message" not in update_runs_cols:
        conn.execute(
            "ALTER TABLE update_runs ADD COLUMN message TEXT NOT NULL DEFAULT ''"
        )
    if "output_log" not in update_runs_cols:
        conn.execute(
            "ALTER TABLE update_runs ADD COLUMN output_log TEXT NOT NULL DEFAULT ''"
        )
    if "run_ts" in update_runs_cols:
        conn.execute(
            """
            UPDATE update_runs
            SET started_at = COALESCE(NULLIF(started_at, ''), run_ts, CURRENT_TIMESTAMP)
            WHERE started_at IS NULL OR started_at = ''
            """
        )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_update_runs_run_id ON update_runs (run_id)"
    )

    update_state_cols = {
        row["name"] for row in conn.execute("PRAGMA table_info(update_state)").fetchall()
    }
    if "run_id" not in update_state_cols:
        conn.execute("ALTER TABLE update_state ADD COLUMN run_id TEXT")
    if "status" not in update_state_cols:
        conn.execute(
            "ALTER TABLE update_state ADD COLUMN status TEXT NOT NULL DEFAULT 'idle'"
        )
    if "progress_pct" not in update_state_cols:
        conn.execute(
            "ALTER TABLE update_state ADD COLUMN progress_pct INTEGER NOT NULL DEFAULT 0"
        )
    if "message" not in update_state_cols:
        conn.execute(
            "ALTER TABLE update_state ADD COLUMN message TEXT NOT NULL DEFAULT ''"
        )
    if "current_step" not in update_state_cols:
        conn.execute(
            "ALTER TABLE update_state ADD COLUMN current_step TEXT NOT NULL DEFAULT ''"
        )

