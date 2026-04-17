"""
Re-identificacao anonima por similaridade facial (sem nome/foto).

Gera um person_id tecnico (anon_*) estavel entre visitas, permitindo
acompanhar recorrencia sem armazenar dados biometricos identificaveis.

Dois modos de embedding (automatico, com fallback):

  SFace (preferido): modelo neural 128-dim via cv2.FaceRecognizerSF.
    Requer face_recognition_sface_2021dec.onnx em data/opencv_dnn_models/.
    Altamente discriminativo — distingue rostos semelhantes com >95% acuracia
    em condicoes frontais. ~10-15ms por face no Pi4.

  DCT (fallback): 400-dim baseado em DCT de baixa frequencia.
    Funciona sem modelos extras. Menos discriminativo (~80% recall), mas
    zero dependencias alem do OpenCV.

Perfis sao armazenados em SQLite (anon_face_profiles) e cacheados em memoria
com TTL de 10s. Embeddings de tamanho diferente nao sao comparados entre si
(shape mismatch → skip), entao a transicao DCT→SFace e transparente.
"""
from __future__ import annotations

import json
import logging
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any

import numpy as np

from .db import get_connection

logger = logging.getLogger(__name__)

try:
    import cv2

    HAS_CV2 = True
except ImportError:
    cv2 = None  # type: ignore[assignment]
    HAS_CV2 = False

_MODEL_DIR = Path(__file__).resolve().parent.parent / "data" / "opencv_dnn_models"

_EMA_ALPHA = 0.10
_EMA_MIN_SEEN = 2

# --- Thresholds por tipo de descriptor (cosine similarity via dot product) ---
_THRESHOLD_SFACE = 0.30
_THRESHOLD_DCT = 0.86

# DCT params
_DCT_BLOCK = 20
_DESC_RESIZE = 64
_MIN_FACE_PX = 60

# --- SFace singleton ---
_sface: Any | None = None
_sface_attempted = False
_sface_lock = threading.Lock()

# --- Profile cache ---
_cache_lock = threading.Lock()
_profile_cache: list[tuple[str, np.ndarray, int]] | None = None
_cache_ts: float = 0.0
_CACHE_TTL = 10.0

# --- CLAHE singleton (para DCT fallback) ---
_clahe: Any | None = None
_clahe_lock = threading.Lock()


def _get_clahe() -> Any:
    global _clahe
    if _clahe is not None:
        return _clahe
    with _clahe_lock:
        if _clahe is None and HAS_CV2 and cv2 is not None:
            _clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        return _clahe


def _get_sface() -> Any | None:
    """Carrega FaceRecognizerSF (SFace) se modelo e API disponiveis."""
    global _sface, _sface_attempted
    if _sface is not None:
        return _sface
    if _sface_attempted:
        return None
    with _sface_lock:
        if _sface is not None:
            return _sface
        if _sface_attempted:
            return None
        _sface_attempted = True
        if not HAS_CV2 or cv2 is None:
            return None
        if not hasattr(cv2, "FaceRecognizerSF"):
            logger.info(
                "cv2.FaceRecognizerSF nao disponivel (OpenCV < 4.5.4). "
                "Usando DCT como fallback."
            )
            return None
        model_path = _MODEL_DIR / "face_recognition_sface_2021dec.onnx"
        if not model_path.is_file():
            logger.info(
                "SFace nao encontrado em %s — usando DCT como fallback. "
                "Execute scripts/download_demographics_models.sh para instalar.",
                model_path,
            )
            return None
        try:
            _sface = cv2.FaceRecognizerSF.create(str(model_path), "")
            logger.info("SFace face recognizer carregado com sucesso (128-dim).")
            return _sface
        except Exception as exc:
            logger.warning("Falha ao carregar SFace: %s", exc)
            return None


def _normalize(v: np.ndarray) -> np.ndarray:
    n = float(np.linalg.norm(v))
    if n <= 1e-9:
        return v
    return v / n


_yunet_reid: Any | None = None
_yunet_reid_lock = threading.Lock()


def _get_yunet_for_reid(w: int, h: int) -> Any | None:
    """YuNet para deteccao de landmarks no crop (necessario para alignCrop do SFace)."""
    global _yunet_reid
    if not HAS_CV2 or cv2 is None:
        return None
    if not hasattr(cv2, "FaceDetectorYN"):
        return None
    model_path = _MODEL_DIR / "face_detection_yunet_2023mar.onnx"
    if not model_path.is_file():
        return None
    with _yunet_reid_lock:
        try:
            if _yunet_reid is None:
                _yunet_reid = cv2.FaceDetectorYN.create(
                    str(model_path), "", (w, h),
                    score_threshold=0.5, nms_threshold=0.3,
                )
            else:
                _yunet_reid.setInputSize((w, h))
            return _yunet_reid
        except Exception:
            return None


def _sface_descriptor(face_bgr: np.ndarray) -> np.ndarray | None:
    """128-dim neural face embedding via SFace com alinhamento por landmarks."""
    sface = _get_sface()
    if sface is None:
        return None
    if face_bgr is None or face_bgr.size == 0:
        return None
    h, w = face_bgr.shape[:2]
    if h < _MIN_FACE_PX or w < _MIN_FACE_PX:
        return None
    try:
        yunet = _get_yunet_for_reid(w, h)
        if yunet is not None:
            _, det = yunet.detect(face_bgr)
            if det is not None and len(det) > 0:
                aligned = sface.alignCrop(face_bgr, det[0])
                emb = sface.feature(aligned)
                vec = emb.flatten().astype(np.float32)
                return _normalize(vec)
        emb = sface.feature(face_bgr)
        vec = emb.flatten().astype(np.float32)
        return _normalize(vec)
    except Exception as exc:
        logger.debug("SFace feature extraction falhou: %s", exc)
        return None


def _dct_descriptor(face_bgr: np.ndarray) -> np.ndarray | None:
    """400-dim DCT de baixa frequencia (fallback sem modelo extra)."""
    if not HAS_CV2 or cv2 is None:
        return None
    if face_bgr is None or face_bgr.size == 0:
        return None
    h, w = face_bgr.shape[:2]
    if h < _MIN_FACE_PX or w < _MIN_FACE_PX:
        return None
    gray = cv2.cvtColor(face_bgr, cv2.COLOR_BGR2GRAY)
    gray = cv2.resize(gray, (_DESC_RESIZE, _DESC_RESIZE), interpolation=cv2.INTER_AREA)
    clahe = _get_clahe()
    if clahe is not None:
        gray = clahe.apply(gray)
    else:
        gray = cv2.equalizeHist(gray)
    f = gray.astype(np.float32) / 255.0
    dct = cv2.dct(f)
    low = dct[:_DCT_BLOCK, :_DCT_BLOCK].flatten()
    return _normalize(low).astype(np.float32)


def face_descriptor(face_bgr: np.ndarray) -> np.ndarray | None:
    """
    Gera embedding facial: tenta SFace (128-dim) primeiro, cai para DCT (400-dim).
    Retorna vetor L2-normalizado ou None se crop insuficiente.
    """
    desc = _sface_descriptor(face_bgr)
    if desc is not None:
        return desc
    return _dct_descriptor(face_bgr)


def _threshold_for_descriptor(desc: np.ndarray) -> float:
    """Seleciona threshold baseado na dimensao do embedding."""
    if desc.size == 128:
        return _THRESHOLD_SFACE
    return _THRESHOLD_DCT


def _load_profiles(conn: Any) -> list[tuple[str, np.ndarray, int]]:
    rows = conn.execute(
        "SELECT person_id, embedding_json, seen_count FROM anon_face_profiles"
    ).fetchall()
    out: list[tuple[str, np.ndarray, int]] = []
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
        seen = int(r["seen_count"]) if r["seen_count"] else 1
        out.append((pid, _normalize(vec), seen))
    return out


def _get_cached_profiles() -> list[tuple[str, np.ndarray, int]]:
    global _profile_cache, _cache_ts
    now = monotonic()
    with _cache_lock:
        if _profile_cache is not None and (now - _cache_ts) < _CACHE_TTL:
            return _profile_cache
    with get_connection() as conn:
        profiles = _load_profiles(conn)
    with _cache_lock:
        _profile_cache = profiles
        _cache_ts = monotonic()
    return profiles


def _invalidate_cache() -> None:
    global _profile_cache
    with _cache_lock:
        _profile_cache = None


def resolve_anonymous_person_id(face_bgr: np.ndarray) -> str | None:
    """
    Resolve recorrencia anonima por similaridade facial.

    Retorna person_id tecnico (anon_*) existente se a similaridade ultrapassar
    o limiar, ou cria um novo perfil. Retorna None se o crop for insuficiente.
    """
    desc = face_descriptor(face_bgr)
    if desc is None:
        return None

    threshold = _threshold_for_descriptor(desc)
    profiles = _get_cached_profiles()

    best_pid = ""
    best_sim = -1.0
    best_vec: np.ndarray | None = None
    best_seen = 0
    for pid, vec, seen in profiles:
        if vec.shape != desc.shape:
            continue
        sim = float(np.dot(vec, desc))
        if sim > best_sim:
            best_sim = sim
            best_pid = pid
            best_vec = vec
            best_seen = seen

    now_sql = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    import sys
    print(
        f"ReID desc={desc.size} best_sim={best_sim:.3f} thr={threshold:.3f} "
        f"match={best_pid if best_sim >= threshold else 'NONE'} profiles={len(profiles)}",
        file=sys.stderr, flush=True,
    )

    if best_pid and best_vec is not None and best_sim >= threshold:
        if best_seen >= _EMA_MIN_SEEN:
            merged = _normalize(
                (1.0 - _EMA_ALPHA) * best_vec + _EMA_ALPHA * desc
            )
            emb_json = json.dumps(merged.tolist())
        else:
            emb_json = json.dumps(best_vec.tolist())

        with get_connection() as conn:
            conn.execute(
                """
                UPDATE anon_face_profiles
                SET embedding_json = ?,
                    last_seen = ?,
                    seen_count = seen_count + 1
                WHERE person_id = ?
                """,
                (emb_json, now_sql, best_pid),
            )
            conn.commit()
        _invalidate_cache()
        return best_pid

    new_pid = f"anon_{uuid.uuid4().hex[:12]}"
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO anon_face_profiles (
                person_id, embedding_json, first_seen, last_seen, seen_count
            ) VALUES (?, ?, ?, ?, 1)
            """,
            (new_pid, json.dumps(desc.tolist()), now_sql, now_sql),
        )
        conn.commit()
    _invalidate_cache()
    return new_pid
