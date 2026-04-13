from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import numpy as np

from .db import get_connection

try:
    import cv2

    HAS_CV2 = True
except ImportError:
    cv2 = None  # type: ignore[assignment]
    HAS_CV2 = False

# Conservador para reduzir trocas de identidade.
_SIMILARITY_THRESHOLD = 0.90
# Atualizacao lenta do embedding para absorver variacoes sem perder identidade.
_EMA_ALPHA = 0.15


def _normalize(v: np.ndarray) -> np.ndarray:
    n = float(np.linalg.norm(v))
    if n <= 1e-9:
        return v
    return v / n


def face_descriptor(face_bgr: np.ndarray) -> np.ndarray | None:
    """
    Assinatura anonima de rosto (sem nome/foto), robusta o suficiente para recorrencia.

    Usa canal em tons de cinza + equalizacao + DCT de baixa frequencia.
    """
    if not HAS_CV2 or cv2 is None:
        return None
    if face_bgr is None or face_bgr.size == 0:
        return None
    h, w = face_bgr.shape[:2]
    if h < 20 or w < 20:
        return None

    gray = cv2.cvtColor(face_bgr, cv2.COLOR_BGR2GRAY)
    gray = cv2.resize(gray, (64, 64), interpolation=cv2.INTER_AREA)
    gray = cv2.equalizeHist(gray)
    f = gray.astype(np.float32) / 255.0
    dct = cv2.dct(f)
    # Mantem apenas baixas frequencias (16x16), reduz sensibilidade a ruido fino.
    low = dct[:16, :16].flatten()
    desc = _normalize(low)
    return desc.astype(np.float32)


def _load_profiles(conn: Any) -> list[tuple[str, np.ndarray]]:
    rows = conn.execute(
        """
        SELECT person_id, embedding_json
        FROM anon_face_profiles
        """
    ).fetchall()
    out: list[tuple[str, np.ndarray]] = []
    for r in rows:
        pid = str(r["person_id"] or "").strip()
        raw = str(r["embedding_json"] or "").strip()
        if not pid or not raw:
            continue
        try:
            vec = np.asarray(json.loads(raw), dtype=np.float32)
        except Exception:
            continue
        if vec.size == 0:
            continue
        out.append((pid, _normalize(vec)))
    return out


def resolve_anonymous_person_id(face_bgr: np.ndarray) -> str | None:
    """
    Resolve recorrencia anonima por similaridade facial.
    Retorna person_id tecnico (anon_*) ou None quando nao ha dados suficientes.
    """
    desc = face_descriptor(face_bgr)
    if desc is None:
        return None

    now_sql = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        profiles = _load_profiles(conn)
        best_pid = ""
        best_sim = -1.0
        best_vec: np.ndarray | None = None
        for pid, vec in profiles:
            if vec.shape != desc.shape:
                continue
            sim = float(np.dot(vec, desc))
            if sim > best_sim:
                best_sim = sim
                best_pid = pid
                best_vec = vec

        if best_pid and best_vec is not None and best_sim >= _SIMILARITY_THRESHOLD:
            merged = _normalize((1.0 - _EMA_ALPHA) * best_vec + _EMA_ALPHA * desc)
            conn.execute(
                """
                UPDATE anon_face_profiles
                SET embedding_json = ?,
                    last_seen = ?,
                    seen_count = seen_count + 1
                WHERE person_id = ?
                """,
                (json.dumps(merged.tolist()), now_sql, best_pid),
            )
            conn.commit()
            return best_pid

        new_pid = f"anon_{uuid.uuid4().hex[:12]}"
        conn.execute(
            """
            INSERT INTO anon_face_profiles (
                person_id, embedding_json, first_seen, last_seen, seen_count
            ) VALUES (?, ?, ?, ?, 1)
            """,
            (new_pid, json.dumps(desc.tolist()), now_sql, now_sql),
        )
        conn.commit()
        return new_pid

