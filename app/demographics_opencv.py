"""
Estimativa opcional de idade (inteiro) e sexo (homem/mulher) a partir de crop
BGR de rosto, via redes Caffe no OpenCV DNN.

Dois modos de entrada:
  - estimate_demographics_optional: recebe crop generico, detecta rosto via
    Haar internamente (para uso com crops de corpo inteiro)
  - estimate_demographics_from_face: recebe crop ja centrado no rosto (com
    padding), pula Haar — evita deteccao redundante quando o caller ja fez

Requer ficheiros em data/opencv_dnn_models/ (ver README e
scripts/download_demographics_models.sh).
"""
from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Any, Literal

import numpy as np

logger = logging.getLogger(__name__)

try:
    import cv2

    HAS_CV2 = True
except ImportError:
    cv2 = None  # type: ignore[assignment]
    HAS_CV2 = False

_MODEL_DIR = Path(__file__).resolve().parent.parent / "data" / "opencv_dnn_models"

_AGE_BUCKETS: list[tuple[int, int]] = [
    (0, 2),
    (4, 6),
    (8, 12),
    (15, 20),
    (25, 32),
    (38, 43),
    (48, 53),
    (60, 100),
]

_MODEL_MEAN = (78.4263377603, 87.7689143744, 114.895847746)
_BLOB_SIZE = (227, 227)

_lock = threading.Lock()
_cascade: Any | None = None
_age_net: Any | None = None
_gender_net: Any | None = None
_age_attempted = False
_gender_attempted = False
_load_ok_age = False
_load_ok_gender = False
_missing_logged_age = False
_missing_logged_gender = False


def _skip_due_to_env() -> bool:
    return os.environ.get("VIP_SKIP_DEMOGRAPHICS", "").strip() in ("1", "true", "yes")


def _get_cascade() -> Any | None:
    global _cascade
    if not HAS_CV2 or cv2 is None:
        return None
    with _lock:
        if _cascade is None:
            path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
            _cascade = cv2.CascadeClassifier(path)
            if _cascade.empty():
                logger.warning("Haar cascade de rosto vazia ou invalida: %s", path)
                _cascade = None
        return _cascade


def _try_load_dnn_nets(*, want_age: bool, want_gender: bool) -> None:
    global _age_net, _gender_net
    global _age_attempted, _gender_attempted, _load_ok_age, _load_ok_gender
    global _missing_logged_age, _missing_logged_gender
    if not HAS_CV2 or cv2 is None:
        return
    with _lock:
        if want_age and not _age_attempted:
            _age_attempted = True
            age_p = _MODEL_DIR / "age_deploy.prototxt"
            age_w = _MODEL_DIR / "age_net.caffemodel"
            if age_p.is_file() and age_w.is_file():
                try:
                    _age_net = cv2.dnn.readNetFromCaffe(str(age_p), str(age_w))
                    _load_ok_age = True
                except Exception as exc:
                    logger.warning("Falha ao carregar age_net: %s", exc)
            elif not _missing_logged_age:
                _missing_logged_age = True
                logger.info(
                    "Pesos de idade ausentes em %s — corra scripts/download_demographics_models.sh.",
                    _MODEL_DIR,
                )
        if want_gender and not _gender_attempted:
            _gender_attempted = True
            g_p = _MODEL_DIR / "gender_deploy.prototxt"
            g_w = _MODEL_DIR / "gender_net.caffemodel"
            if g_p.is_file() and g_w.is_file():
                try:
                    _gender_net = cv2.dnn.readNetFromCaffe(str(g_p), str(g_w))
                    _load_ok_gender = True
                except Exception as exc:
                    logger.warning("Falha ao carregar gender_net: %s", exc)
            elif not _missing_logged_gender:
                _missing_logged_gender = True
                logger.info(
                    "Pesos de sexo ausentes em %s — corra scripts/download_demographics_models.sh.",
                    _MODEL_DIR,
                )


def _largest_face(
    gray: np.ndarray,
) -> tuple[int, int, int, int] | None:
    casc = _get_cascade()
    if casc is None:
        return None
    h, w = gray.shape[:2]
    if h < 8 or w < 8:
        return None
    min_px = max(18, min(w, h) // 14)
    faces = casc.detectMultiScale(
        gray,
        scaleFactor=1.08,
        minNeighbors=4,
        minSize=(min_px, min_px),
    )
    if faces is None or len(faces) == 0:
        return None
    best = max(faces, key=lambda r: int(r[2]) * int(r[3]))
    x, y, rw, rh = int(best[0]), int(best[1]), int(best[2]), int(best[3])
    return x, y, rw, rh


def extract_largest_face_crop(bgr_crop: np.ndarray) -> np.ndarray | None:
    """Extrai o maior rosto de um recorte BGR (ou None se nao encontrar)."""
    if not HAS_CV2 or cv2 is None:
        return None
    if bgr_crop is None or bgr_crop.size == 0:
        return None
    h, w = bgr_crop.shape[:2]
    if h < 16 or w < 16:
        return None
    gray = cv2.cvtColor(bgr_crop, cv2.COLOR_BGR2GRAY)
    face = _largest_face(gray)
    if face is None:
        return None
    fx, fy, frw, frh = face
    pad = max(2, int(min(frw, frh) * 0.08))
    x0 = max(0, fx - pad)
    y0 = max(0, fy - pad)
    x1 = min(w, fx + frw + pad)
    y1 = min(h, fy + frh + pad)
    if x1 <= x0 or y1 <= y0:
        return None
    face_bgr = bgr_crop[y0:y1, x0:x1]
    return face_bgr if face_bgr.size > 0 else None


_MIN_AGE_CONFIDENCE = 0.45
_MIN_GENDER_CONFIDENCE = 0.55


def _run_dnn_inference(
    face_bgr: np.ndarray,
    *,
    want_age: bool,
    want_gender: bool,
) -> tuple[int | None, Literal["homem", "mulher"] | None]:
    """Executa forward pass das redes de idade/genero sobre um crop de rosto."""
    blob = cv2.dnn.blobFromImage(
        face_bgr, 1.0, _BLOB_SIZE, _MODEL_MEAN, swapRB=False, crop=False,
    )

    age_out: int | None = None
    gender_out: Literal["homem", "mulher"] | None = None

    if want_age and _age_net is not None and _load_ok_age:
        try:
            _age_net.setInput(blob)
            preds = _age_net.forward()
            flat = np.array(preds).flatten()
            if flat.size > 0:
                idx = int(np.argmax(flat))
                conf = float(flat[idx])
                if conf >= _MIN_AGE_CONFIDENCE and 0 <= idx < len(_AGE_BUCKETS):
                    lo, hi = _AGE_BUCKETS[idx]
                    age_out = max(0, min(120, (lo + hi) // 2))
                else:
                    logger.debug("age conf=%.3f idx=%d — descartado", conf, idx)
        except Exception as exc:
            logger.debug("age_net.forward falhou: %s", exc)

    if want_gender and _gender_net is not None and _load_ok_gender:
        try:
            _gender_net.setInput(blob)
            preds = _gender_net.forward()
            flat = np.array(preds).flatten()
            if flat.size >= 2:
                idx = int(np.argmax(flat))
                conf = float(flat[idx])
                if conf >= _MIN_GENDER_CONFIDENCE:
                    gender_out = "homem" if idx == 0 else "mulher"
                else:
                    logger.debug("gender conf=%.3f — descartado", conf)
        except Exception as exc:
            logger.debug("gender_net.forward falhou: %s", exc)

    return age_out, gender_out


def estimate_demographics_optional(
    bgr_crop: np.ndarray,
    *,
    want_age: bool,
    want_gender: bool,
) -> tuple[int | None, Literal["homem", "mulher"] | None]:
    """
    Recebe crop generico (pode ser corpo inteiro), detecta rosto internamente
    via Haar e estima idade/genero. Para crops ja centrados no rosto, use
    estimate_demographics_from_face (mais eficiente).
    """
    if not want_age and not want_gender:
        return None, None
    if _skip_due_to_env():
        return None, None
    if not HAS_CV2 or cv2 is None:
        return None, None
    if bgr_crop is None or bgr_crop.size == 0:
        return None, None

    _try_load_dnn_nets(want_age=want_age, want_gender=want_gender)

    h, w = bgr_crop.shape[:2]
    if h < 16 or w < 16:
        return None, None

    face_bgr = extract_largest_face_crop(bgr_crop)
    if face_bgr is None:
        return None, None

    return _run_dnn_inference(face_bgr, want_age=want_age, want_gender=want_gender)


def estimate_demographics_from_face(
    face_bgr: np.ndarray,
    *,
    want_age: bool,
    want_gender: bool,
) -> tuple[int | None, Literal["homem", "mulher"] | None]:
    """
    Recebe crop ja centrado no rosto (com padding do detector). Pula a deteccao
    Haar interna — ideal quando o caller ja fez Haar ou sabe que o crop e um rosto.
    Economiza ~1 deteccao Haar por entrada no Pi4.
    """
    if not want_age and not want_gender:
        return None, None
    if _skip_due_to_env():
        return None, None
    if not HAS_CV2 or cv2 is None:
        return None, None
    if face_bgr is None or face_bgr.size == 0:
        return None, None

    _try_load_dnn_nets(want_age=want_age, want_gender=want_gender)

    h, w = face_bgr.shape[:2]
    if h < 36 or w < 36:
        return None, None

    return _run_dnn_inference(face_bgr, want_age=want_age, want_gender=want_gender)
