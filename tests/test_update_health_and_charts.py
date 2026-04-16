from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import numpy as np

from app import anonymous_face_reid, db, main, retention


class VipTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.data_dir = root / "data"
        self.db_path = self.data_dir / "app.db"
        self.repo_dir = root / "repo"
        self.repo_dir.mkdir(parents=True, exist_ok=True)
        (self.repo_dir / "app").mkdir(parents=True, exist_ok=True)
        (self.repo_dir / "requirements.txt").write_text("fastapi\n", encoding="utf-8")

        self._patchers = [
            patch.object(db, "DATA_DIR", self.data_dir),
            patch.object(db, "DB_PATH", self.db_path),
        ]
        for p in self._patchers:
            p.start()

        db.init_db()
        retention.invalidate_involvement_summary_cache()

    def tearDown(self) -> None:
        for p in reversed(self._patchers):
            p.stop()
        self._tmp.cleanup()

    def _insert_event(
        self,
        *,
        event_id: str,
        event_type: str,
        event_ts: str,
        temp_id: str = "face_test",
    ) -> None:
        with db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO events (event_id, culto_id, profile_id, temp_id, event_type, event_ts, age_band, gender)
                VALUES (?, NULL, NULL, ?, ?, ?, NULL, NULL)
                """,
                (event_id, temp_id, event_type, event_ts),
            )
            conn.commit()

    def test_update_flow_done_without_git_uses_pep668_fallback(self) -> None:
        calls: list[list[str]] = []

        def fake_run(cmd: list[str], *, cwd: Path, timeout: int = 120) -> tuple[bool, str]:
            calls.append(list(cmd))
            return True, "ok"

        gitless_info = {
            "branch": "",
            "local_commit": "",
            "remote_commit": "",
            "ahead_count": 0,
            "behind_count": 0,
            "fetch_error": "",
        }

        with (
            patch("app.retention._repo_root", return_value=self.repo_dir),
            patch("app.retention._collect_git_update_info", return_value=gitless_info),
            patch("app.retention._run_command", side_effect=fake_run),
        ):
            req = retention.request_system_update_run()
            self.assertTrue(req["accepted"])

            queued = retention.get_update_status(refresh_remote=False)
            self.assertEqual(queued["status"], "queued")
            self.assertTrue(queued["running"])

            result = retention.run_system_update_job(req["run_id"])
            self.assertEqual(result["status"], "done")

        fetch_main_seen = any(c[:1] == ["git"] and "fetch" in c and c[-1] == "main" for c in calls)
        self.assertTrue(fetch_main_seen, "Expected fallback fetch origin main in gitless mode")

        pip_calls = [c for c in calls if len(c) >= 3 and c[1:3] == ["-m", "pip"]]
        self.assertTrue(pip_calls, "Expected pip_install step to run")
        self.assertTrue(
            any("--break-system-packages" in c for c in pip_calls),
            "Expected PEP 668 fallback flag when .venv python is unavailable",
        )

    def test_update_flow_error_marks_last_status_error(self) -> None:
        def fake_run(cmd: list[str], *, cwd: Path, timeout: int = 120) -> tuple[bool, str]:
            if "clean" in cmd:
                return False, "boom clean failed"
            return True, "ok"

        with (
            patch("app.retention._repo_root", return_value=self.repo_dir),
            patch(
                "app.retention._collect_git_update_info",
                return_value={
                    "branch": "main",
                    "local_commit": "abc",
                    "remote_commit": "def",
                    "ahead_count": 0,
                    "behind_count": 1,
                    "fetch_error": "",
                },
            ),
            patch("app.retention._run_command", side_effect=fake_run),
        ):
            req = retention.request_system_update_run()
            self.assertTrue(req["accepted"])
            result = retention.run_system_update_job(req["run_id"])
            self.assertEqual(result["status"], "error")

            st = retention.get_update_status(refresh_remote=False)
            self.assertEqual(st["last_status"], "error")

    def test_dashboard_charts_handle_mixed_event_ts_formats_consistently(self) -> None:
        now = datetime.now(UTC)
        in_window_iso = (now - timedelta(minutes=10)).isoformat()
        in_window_sqlite = (now - timedelta(minutes=4)).strftime("%Y-%m-%d %H:%M:%S+00:00")
        out_window_iso = (now - timedelta(hours=3)).isoformat()

        self._insert_event(event_id="e-in-1", event_type="entrada", event_ts=in_window_iso)
        self._insert_event(event_id="e-in-2", event_type="saida", event_ts=in_window_sqlite)
        self._insert_event(event_id="e-out-1", event_type="entrada", event_ts=out_window_iso)

        data = retention.get_dashboard_charts(
            culto_id="__global__",
            window_minutes=60,
            bucket_seconds=300,
            center=None,
        )
        flow = data["charts"]["flow_per_minute"]
        total_entries = sum(int(item["entries"]) for item in flow)
        total_exits = sum(int(item["exits"]) for item in flow)

        self.assertEqual(total_entries, 1)
        self.assertEqual(total_exits, 1)

    def test_healthz_reports_ok_and_component_payloads(self) -> None:
        with (
            patch("app.main.camera_status", return_value={"active": True}),
            patch("app.main.get_sync_status", return_value={"enabled": False}),
            patch("app.main.get_update_status", return_value={"status": "idle", "running": False}),
        ):
            resp = asyncio.run(main.healthz())
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.body.decode("utf-8"))
        self.assertTrue(body["ok"])
        self.assertEqual(body["status"], "ok")
        self.assertTrue(body["checks"]["db"]["ok"])
        self.assertTrue(body["checks"]["camera"]["ok"])

    def test_people_involvement_ignores_local_face_ids(self) -> None:
        now = datetime.now(UTC)
        self._insert_event(
            event_id="hog-in-1",
            event_type="entrada",
            event_ts=(now - timedelta(days=1)).isoformat(),
            temp_id="face_123",
        )
        self._insert_event(
            event_id="edge-in-1",
            event_type="entrada",
            event_ts=(now - timedelta(days=2)).isoformat(),
            temp_id="edge_person_1",
        )

        data = retention.get_people_involvement(limit=50, offset=0)
        ids = {str(row["person_id"]) for row in data["people"]}

        self.assertIn("edge_person_1", ids)
        self.assertNotIn("face_123", ids)
        self.assertEqual(int(data["summary"]["visitante"]), 1)
        self.assertEqual(int(data["total"]), 1)

    def test_reset_identified_personas_with_day_scrub_keeps_events(self) -> None:
        day_a = "2026-04-01"
        day_b = "2026-04-02"
        with db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO service_event_people (
                    culto_id, person_id, first_seen_at, last_seen_at,
                    entries_count, exits_count, returns_count, age_band, gender,
                    last_direction, last_exit_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "__global__",
                    "p1",
                    f"{day_a}T10:00:00+00:00",
                    f"{day_a}T10:00:00+00:00",
                    1,
                    0,
                    0,
                    None,
                    None,
                    "entrada",
                    None,
                ),
            )
            conn.execute(
                """
                INSERT INTO service_event_stats (culto_id, entries_count, exits_count, returns_count, unique_people_count,
                    current_occupancy, peak_occupancy, crianca_count, junior_count, adolescente_count, jovem_count,
                    adulto_count, idoso_count, homem_count, mulher_count, updated_at)
                VALUES ('__global__', 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, CURRENT_TIMESTAMP)
                """
            )
            conn.execute(
                "INSERT INTO temp_tracks (temp_id, culto_id) VALUES ('t1', '__global__')"
            )
            conn.execute(
                "INSERT INTO profiles (profile_id) VALUES ('profile_1')"
            )
            conn.execute(
                """
                INSERT INTO events (event_id, culto_id, profile_id, temp_id, event_type, event_ts, age_band, gender)
                VALUES (?, NULL, NULL, ?, ?, ?, NULL, NULL)
                """,
                ("ev_a", "p1", "entrada", f"{day_a}T10:00:00+00:00"),
            )
            conn.execute(
                """
                INSERT INTO events (event_id, culto_id, profile_id, temp_id, event_type, event_ts, age_band, gender)
                VALUES (?, NULL, NULL, ?, ?, ?, NULL, NULL)
                """,
                ("ev_b", "p2", "entrada", f"{day_b}T10:00:00+00:00"),
            )
            conn.commit()

        result = retention.reset_identified_personas(
            reset_personas_day=day_a,
            wipe_all_personas=False,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(int(result["affected_event_rows"]), 1)
        self.assertEqual(int(result["affected_person_ids"]), 1)
        self.assertEqual(int(result["wiped_temp_tracks"]), 1)
        self.assertEqual(int(result["wiped_profiles"]), 1)

        with db.get_connection() as conn:
            left_events = int(conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"])
            day_a_with_id = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM events
                    WHERE substr(event_ts, 1, 10) = ?
                      AND temp_id IS NOT NULL
                      AND TRIM(COALESCE(temp_id, '')) != ''
                    """,
                    (day_a,),
                ).fetchone()["c"]
            )
            day_b_with_id = int(
                conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM events
                    WHERE substr(event_ts, 1, 10) = ?
                      AND temp_id IS NOT NULL
                      AND TRIM(COALESCE(temp_id, '')) != ''
                    """,
                    (day_b,),
                ).fetchone()["c"]
            )
        self.assertEqual(left_events, 2)
        self.assertEqual(day_a_with_id, 0)
        self.assertEqual(day_b_with_id, 1)

    def test_reset_identified_personas_with_day_event_delete(self) -> None:
        day_a = "2026-04-01"
        day_b = "2026-04-02"
        with db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO events (event_id, culto_id, profile_id, temp_id, event_type, event_ts, age_band, gender)
                VALUES (?, NULL, NULL, ?, ?, ?, NULL, NULL)
                """,
                ("ev_a1", "p1", "entrada", f"{day_a}T10:00:00+00:00"),
            )
            conn.execute(
                """
                INSERT INTO events (event_id, culto_id, profile_id, temp_id, event_type, event_ts, age_band, gender)
                VALUES (?, NULL, NULL, ?, ?, ?, NULL, NULL)
                """,
                ("ev_a2", "p2", "saida", f"{day_a}T11:00:00+00:00"),
            )
            conn.execute(
                """
                INSERT INTO events (event_id, culto_id, profile_id, temp_id, event_type, event_ts, age_band, gender)
                VALUES (?, NULL, NULL, ?, ?, ?, NULL, NULL)
                """,
                ("ev_b1", "p3", "entrada", f"{day_b}T10:00:00+00:00"),
            )
            conn.commit()

        result = retention.reset_identified_personas(
            reset_personas_day=day_a,
            wipe_all_personas=False,
            delete_day_events=True,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(bool(result["delete_day_events"]))
        self.assertEqual(int(result["affected_event_rows"]), 2)
        self.assertEqual(int(result["affected_person_ids"]), 2)

        with db.get_connection() as conn:
            left_day_a = int(
                conn.execute(
                    "SELECT COUNT(*) AS c FROM events WHERE substr(event_ts, 1, 10) = ?",
                    (day_a,),
                ).fetchone()["c"]
            )
            left_total = int(conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"])
        self.assertEqual(left_day_a, 0)
        self.assertEqual(left_total, 1)

    def test_anonymous_face_reid_reuses_person_id_for_similar_face(self) -> None:
        if not anonymous_face_reid.HAS_CV2:
            self.skipTest("OpenCV indisponivel no ambiente de teste")

        base = np.zeros((96, 96, 3), dtype=np.uint8)
        # Face sintetica simples para teste de recorrencia anonima.
        base[:, :] = (90, 90, 90)
        base[20:80, 20:80] = (180, 180, 180)
        base[38:46, 36:44] = (20, 20, 20)
        base[38:46, 52:60] = (20, 20, 20)
        base[58:64, 40:56] = (30, 30, 30)

        var = np.clip(base.astype(np.int16) + 5, 0, 255).astype(np.uint8)
        pid1 = anonymous_face_reid.resolve_anonymous_person_id(base)
        pid2 = anonymous_face_reid.resolve_anonymous_person_id(var)
        self.assertIsNotNone(pid1)
        self.assertEqual(pid1, pid2)


if __name__ == "__main__":
    unittest.main()
