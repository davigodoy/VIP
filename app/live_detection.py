"""
Deteccao continua por rosto no mesmo fluxo da camera.
Gera entradas/saidas via ingest_event — independente do browser aberto.

Pipeline por frame:
  1. Redimensiona frame → CLAHE (normaliza iluminacao variavel)
  2. Detecta rostos: YuNet DNN (se modelo presente) ou Haar Cascade (fallback)
  3. Tracker IoU + distancia mantém identidade entre frames
  4. Acumula o melhor crop (maior area) durante os frames de estabilizacao
  5. Entrada confirmada apos N frames → melhor crop alimenta re-id + demographics
  6. Saida gravada apos N frames sem deteccao
"""
from __future__ import annotations

import logging
import math
import threading
from pathlib import Path
from time import monotonic
from typing import Any

import numpy as np

from .anonymous_face_reid import resolve_anonymous_person_id
from .demographics_opencv import estimate_demographics_from_face
from .models import EventIngestRequest, GenderBand
from .retention import ingest_event, load_config

logger = logging.getLogger(__name__)

try:
    import cv2

    HAS_CV2 = True
except ImportError:
    cv2 = None  # type: ignore[assignment]
    HAS_CV2 = False

_MODEL_DIR = Path(__file__).resolve().parent.parent / "data" / "opencv_dnn_models"

# --- Detector state ---
_face_cascade: Any | None = None
_face_lock = threading.Lock()
_yunet: Any | None = None
_yunet_attempted = False
_yunet_lock = threading.Lock()
_detector_type: str = ""

_clahe: Any | None = None
_clahe_lock = threading.Lock()

# --- Tracking state ---
_state_lock = threading.Lock()
_tracks: dict[int, dict[str, Any]] = {}
_next_tid = 1
_last_cfg_off = True

_exit_ring: list[
    tuple[float, float, float, float, int, tuple[float, float, float, float], str | None]
] = []
_EXIT_RING_CAP = 64
_REUSE_MAX_SEC = 45.0
_REUSE_DIST = 70.0
_REUSE_SZ_DIFF = 28.0

# --- Parametros de tracking (calibrados para rostos) ---
_MATCH_DIST = 85.0
_MIN_IOU_MATCH = 0.12
_MAX_MISSES = 20
_ENTRADA_MIN_FRAMES = 3
_DETECT_MAX_SIDE = 480

# Haar fallback params
_HAAR_SCALE_FACTOR = 1.12
_HAAR_MIN_NEIGHBORS = 4
_HAAR_MIN_W = 28
_HAAR_MIN_H = 28
_HAAR_MAX_W_RATIO = 0.70
_HAAR_MAX_H_RATIO = 0.80

# YuNet params
_YUNET_SCORE_THRESHOLD = 0.65
_YUNET_NMS_THRESHOLD = 0.3
_YUNET_MAX_W_RATIO = 0.70
_YUNET_MAX_H_RATIO = 0.80

_FACE_PAD_RATIO = 0.30
_MIN_CROP_PX = 40


def _iou_xywh(
    a: tuple[float, float, float, float], b: tuple[float, float, float, float]
) -> float:
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    x0 = max(ax, bx)
    y0 = max(ay, by)
    x1 = min(ax + aw, bx + bw)
    y1 = min(ay + ah, by + bh)
    if x1 <= x0 or y1 <= y0:
        return 0.0
    inter = (x1 - x0) * (y1 - y0)
    union = aw * ah + bw * bh - inter
    return inter / union if union > 0.0 else 0.0


def _prune_exit_ring(now_m: float) -> None:
    global _exit_ring
    keep = _REUSE_MAX_SEC + 10.0
    _exit_ring = [r for r in _exit_ring if now_m - r[0] <= keep]
    while len(_exit_ring) > _EXIT_RING_CAP:
        _exit_ring.pop(0)


def _get_clahe() -> Any | None:
    global _clahe
    if not HAS_CV2 or cv2 is None:
        return None
    with _clahe_lock:
        if _clahe is None:
            _clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        return _clahe


def _get_face_cascade() -> Any | None:
    global _face_cascade
    if not HAS_CV2 or cv2 is None:
        return None
    with _face_lock:
        if _face_cascade is None:
            path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
            casc = cv2.CascadeClassifier(path)
            if casc.empty():
                logger.warning("Haar cascade de rosto invalida: %s", path)
                return None
            _face_cascade = casc
        return _face_cascade


def _get_yunet(input_w: int, input_h: int) -> Any | None:
    """Tenta carregar YuNet DNN face detector (muito mais preciso que Haar)."""
    global _yunet, _yunet_attempted
    if not HAS_CV2 or cv2 is None:
        return None
    if not hasattr(cv2, "FaceDetectorYN"):
        return None
    with _yunet_lock:
        if _yunet is not None:
            _yunet.setInputSize((input_w, input_h))
            return _yunet
        if _yunet_attempted:
            return None
        _yunet_attempted = True
        model_path = _MODEL_DIR / "face_detection_yunet_2023mar.onnx"
        if not model_path.is_file():
            logger.info(
                "YuNet nao encontrado em %s — usando Haar como fallback. "
                "Execute scripts/download_demographics_models.sh para instalar.",
                model_path,
            )
            return None
        try:
            _yunet = cv2.FaceDetectorYN.create(
                str(model_path),
                "",
                (input_w, input_h),
                score_threshold=_YUNET_SCORE_THRESHOLD,
                nms_threshold=_YUNET_NMS_THRESHOLD,
            )
            logger.info("YuNet DNN face detector carregado com sucesso.")
            return _yunet
        except Exception as exc:
            logger.warning("Falha ao carregar YuNet: %s", exc)
            return None


def _detect_faces_yunet(
    detector: Any, frame_bgr: np.ndarray, img_w: int, img_h: int
) -> list[list[int]]:
    """Deteccao via YuNet DNN — retorna lista de [x, y, w, h]."""
    try:
        _, faces = detector.detect(frame_bgr)
    except Exception as exc:
        logger.warning("YuNet detect falhou: %s", exc)
        return []
    if faces is None:
        return []
    max_w = int(max(1, img_w * _YUNET_MAX_W_RATIO))
    max_h = int(max(1, img_h * _YUNET_MAX_H_RATIO))
    result: list[list[int]] = []
    for face in faces:
        x, y, w, h = int(face[0]), int(face[1]), int(face[2]), int(face[3])
        if w < _HAAR_MIN_W or h < _HAAR_MIN_H:
            continue
        if w > max_w or h > max_h:
            continue
        result.append([x, y, w, h])
    return result


def _detect_faces_haar(cascade: Any, gray: np.ndarray) -> list[list[int]]:
    """Deteccao via Haar Cascade (fallback)."""
    try:
        out = cascade.detectMultiScale(
            gray,
            scaleFactor=_HAAR_SCALE_FACTOR,
            minNeighbors=_HAAR_MIN_NEIGHBORS,
            minSize=(_HAAR_MIN_W, _HAAR_MIN_H),
        )
    except Exception as exc:
        logger.warning("Haar detectMultiScale falhou: %s", exc)
        return []
    rect_list: list[list[int]] = []
    if out is not None and len(out) > 0:
        h, w = gray.shape[:2]
        max_w = int(max(1, w * _HAAR_MAX_W_RATIO))
        max_h = int(max(1, h * _HAAR_MAX_H_RATIO))
        for x, y, rw, rh in out:
            xi, yi, wi, hi = int(x), int(y), int(rw), int(rh)
            if wi < _HAAR_MIN_W or hi < _HAAR_MIN_H:
                continue
            if wi > max_w or hi > max_h:
                continue
            rect_list.append([xi, yi, wi, hi])
    return rect_list


def reset_tracks() -> None:
    global _tracks, _next_tid, _exit_ring
    with _state_lock:
        _tracks.clear()
        _next_tid = 1
        _exit_ring.clear()


def _padded_face_crop(
    frame_bgr: np.ndarray,
    rect_small: tuple[float, float, float, float],
    small_w: int,
    small_h: int,
    full_w: int,
    full_h: int,
) -> np.ndarray | None:
    """Extrai crop do rosto com padding a partir do bbox no frame redimensionado."""
    sx, sy, srw, srh = rect_small
    if small_w <= 0 or small_h <= 0 or full_w <= 0 or full_h <= 0:
        return None
    fx = full_w / float(small_w)
    fy = full_h / float(small_h)
    face_x = sx * fx
    face_y = sy * fy
    face_w = srw * fx
    face_h = srh * fy
    pad_w = face_w * _FACE_PAD_RATIO
    pad_h = face_h * _FACE_PAD_RATIO
    x0 = int(max(0, face_x - pad_w))
    y0 = int(max(0, face_y - pad_h))
    x1 = int(min(full_w, face_x + face_w + pad_w))
    y1 = int(min(full_h, face_y + face_h + pad_h))
    if (x1 - x0) < _MIN_CROP_PX or (y1 - y0) < _MIN_CROP_PX:
        return None
    crop = frame_bgr[y0:y1, x0:x1]
    return crop if crop.size > 0 else None


def on_frame_bgr(frame: np.ndarray) -> None:
    """Chamado a partir do thread de captura apos ler um frame BGR."""
    global _next_tid, _last_cfg_off, _detector_type

    if not HAS_CV2 or frame is None or frame.size == 0:
        return

    cfg = load_config()
    if not cfg.camera_enabled or not cfg.live_detection_enabled:
        if not _last_cfg_off:
            reset_tracks()
        _last_cfg_off = True
        return
    _last_cfg_off = False

    h, w = frame.shape[:2]
    side = max(h, w)
    scale = min(1.0, _DETECT_MAX_SIDE / float(side)) if side > 0 else 1.0
    small_w = max(1, int(w * scale))
    small_h = max(1, int(h * scale))
    small = cv2.resize(frame, (small_w, small_h), interpolation=cv2.INTER_AREA)

    # --- Deteccao: YuNet (DNN) com fallback para Haar ---
    yunet = _get_yunet(small_w, small_h)
    if yunet is not None:
        rect_list = _detect_faces_yunet(yunet, small, small_w, small_h)
        _detector_type = "yunet"
    else:
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        clahe = _get_clahe()
        if clahe is not None:
            gray = clahe.apply(gray)
        cascade = _get_face_cascade()
        if cascade is None:
            return
        rect_list = _detect_faces_haar(cascade, gray)
        _detector_type = "haar"

    detections: list[tuple[float, float, float, float, float, float, float]] = []
    for xi, yi, wi, hi in rect_list:
        cx = float(xi + wi / 2.0)
        cy = float(yi + hi / 2.0)
        detections.append(
            (cx, cy, float(max(wi, hi)), float(xi), float(yi), float(wi), float(hi))
        )

    to_exit: list[tuple[int, str]] = []
    to_enter: list[int] = []
    enter_best_crop: dict[int, np.ndarray] = {}

    with _state_lock:
        _prune_exit_ring(monotonic())
        track_ids = list(_tracks.keys())
        used_j: set[int] = set()
        match_tid_to_j: dict[int, int] = {}

        scored: list[tuple[float, float, int, int]] = []
        for tid in track_ids:
            tr = _tracks[tid]
            prev = tr.get("rect_small")
            if prev is None:
                continue
            for j, det in enumerate(detections):
                drect = (det[3], det[4], det[5], det[6])
                iou = _iou_xywh(prev, drect)
                dcent = math.hypot(det[0] - tr["cx"], det[1] - tr["cy"])
                scored.append((iou, dcent, tid, j))
        scored.sort(key=lambda t: (-t[0], t[1]))

        matched_tid: set[int] = set()
        for iou, _dcent, tid, j in scored:
            if iou < _MIN_IOU_MATCH:
                break
            if tid in matched_tid or j in used_j:
                continue
            matched_tid.add(tid)
            used_j.add(j)
            match_tid_to_j[tid] = j

        for tid in track_ids:
            if tid in match_tid_to_j:
                continue
            tr = _tracks.get(tid)
            if tr is None:
                continue
            best_i: int | None = None
            best_d = _MATCH_DIST + 1.0
            for j, det in enumerate(detections):
                if j in used_j:
                    continue
                d = math.hypot(det[0] - tr["cx"], det[1] - tr["cy"])
                if d < best_d:
                    best_d = d
                    best_i = j
            if best_i is not None and best_d <= _MATCH_DIST:
                used_j.add(best_i)
                match_tid_to_j[tid] = best_i

        for tid in track_ids:
            tr = _tracks.get(tid)
            if tr is None:
                continue
            if tid in match_tid_to_j:
                j = match_tid_to_j[tid]
                cx, cy, sz, xs, ys, rws, rhs = detections[j]
                tr["cx"], tr["cy"], tr["sz"] = cx, cy, sz
                tr["rect_small"] = (xs, ys, rws, rhs)
                tr["misses"] = 0
                tr["stable_frames"] = int(tr.get("stable_frames", 0)) + 1

                # Acumula melhor crop enquanto estabiliza
                if not tr.get("entrada_commit"):
                    crop = _padded_face_crop(
                        frame, (xs, ys, rws, rhs),
                        small_w, small_h, w, h,
                    )
                    if crop is not None:
                        area = crop.shape[0] * crop.shape[1]
                        if area > tr.get("best_crop_area", 0):
                            tr["best_crop"] = crop.copy()
                            tr["best_crop_area"] = area

                if (
                    not tr.get("entrada_commit")
                    and tr["stable_frames"] >= _ENTRADA_MIN_FRAMES
                ):
                    tr["entrada_commit"] = True
                    to_enter.append(tid)
                    best = tr.get("best_crop")
                    if best is not None:
                        enter_best_crop[tid] = best
                    # Libera referencia ao crop (nao precisa mais)
                    tr.pop("best_crop", None)
                    tr.pop("best_crop_area", None)
            else:
                tr["misses"] = int(tr["misses"]) + 1
                tr["stable_frames"] = 0
                if tr["misses"] >= _MAX_MISSES:
                    if tr.get("entrada_commit"):
                        rs = tr.get("rect_small")
                        if rs is not None:
                            _exit_ring.append((
                                monotonic(),
                                float(tr["cx"]),
                                float(tr["cy"]),
                                float(tr["sz"]),
                                tid,
                                (float(rs[0]), float(rs[1]), float(rs[2]), float(rs[3])),
                                str(tr.get("person_id") or ""),
                            ))
                        while len(_exit_ring) > _EXIT_RING_CAP:
                            _exit_ring.pop(0)
                        exit_pid = str(tr.get("person_id") or f"face_{tid}")
                        to_exit.append((tid, exit_pid))
                    del _tracks[tid]

        nowm = monotonic()
        for i, (cx, cy, sz, xs, ys, rws, rhs) in enumerate(detections):
            if i in used_j:
                continue
            rect = (xs, ys, rws, rhs)
            reuse_tid: int | None = None
            eperson: str | None = None
            for ri in range(len(_exit_ring) - 1, -1, -1):
                ts, ecx, ecy, esz, etid, _erect, ep = _exit_ring[ri]
                if nowm - ts > _REUSE_MAX_SEC:
                    continue
                if abs(sz - esz) > _REUSE_SZ_DIFF:
                    continue
                if math.hypot(cx - ecx, cy - ecy) > _REUSE_DIST:
                    continue
                reuse_tid = etid
                eperson = ep
                _exit_ring.pop(ri)
                break

            if reuse_tid is not None:
                tid = reuse_tid
            else:
                tid = _next_tid
                _next_tid += 1

            # Primeiro crop para o novo track
            initial_crop = _padded_face_crop(
                frame, rect, small_w, small_h, w, h
            )
            _tracks[tid] = {
                "cx": cx,
                "cy": cy,
                "sz": sz,
                "misses": 0,
                "rect_small": rect,
                "stable_frames": 0,
                "entrada_commit": False,
                "person_id": eperson if reuse_tid is not None else None,
                "best_crop": initial_crop.copy() if initial_crop is not None else None,
                "best_crop_area": (
                    initial_crop.shape[0] * initial_crop.shape[1]
                    if initial_crop is not None
                    else 0
                ),
            }

    for _tid, pid in to_exit:
        _emit_saida(pid)
    for tid in to_enter:
        resolved_pid = _emit_entrada(tid, enter_best_crop.get(tid))
        with _state_lock:
            tr = _tracks.get(tid)
            if tr is not None:
                tr["person_id"] = resolved_pid


def _emit_entrada(track_id: int, best_crop: np.ndarray | None) -> str:
    pid = f"face_{track_id}"
    cfg = load_config()
    want_age = bool(cfg.estimar_faixa_etaria)
    want_gender = bool(cfg.estimar_genero)
    age_est: int | None = None
    gender_band: GenderBand | None = None

    if best_crop is not None and HAS_CV2 and cv2 is not None:
        anon_id = resolve_anonymous_person_id(best_crop)
        if anon_id:
            pid = anon_id

        if want_age or want_gender:
            age_est, g = estimate_demographics_from_face(
                best_crop, want_age=want_age, want_gender=want_gender
            )
            if g in ("homem", "mulher"):
                gender_band = g

    try:
        ingest_event(EventIngestRequest(
            person_id=pid,
            direction="entrada",
            age_estimate=age_est,
            gender=gender_band,
        ))
    except Exception as exc:
        logger.warning("ingest entrada falhou (%s): %s", pid, exc)
    return pid


def _emit_saida(pid: str) -> None:
    try:
        ingest_event(EventIngestRequest(person_id=pid, direction="saida"))
    except Exception as exc:
        logger.warning("ingest saida falhou (%s): %s", pid, exc)


def get_detection_models_status() -> dict[str, Any]:
    """Status de cada modelo DNN: arquivo presente, carregado, e qual detector ativo."""
    from . import anonymous_face_reid as _reid_mod
    from . import demographics_opencv as _demo_mod

    def _file_info(name: str) -> dict[str, Any]:
        p = _MODEL_DIR / name
        present = p.is_file()
        size_mb = round(p.stat().st_size / (1024 * 1024), 1) if present else None
        return {"present": present, "size_mb": size_mb}

    yunet_file = _file_info("face_detection_yunet_2023mar.onnx")
    sface_file = _file_info("face_recognition_sface_2021dec.onnx")
    age_file = _file_info("age_net.caffemodel")
    gender_file = _file_info("gender_net.caffemodel")

    sface_obj = getattr(_reid_mod, "_sface", None)
    sface_tried = getattr(_reid_mod, "_sface_attempted", False)
    age_obj = getattr(_demo_mod, "_age_net", None)
    age_ok = getattr(_demo_mod, "_load_ok_age", False)
    age_tried = getattr(_demo_mod, "_age_attempted", False)
    gender_obj = getattr(_demo_mod, "_gender_net", None)
    gender_ok = getattr(_demo_mod, "_load_ok_gender", False)
    gender_tried = getattr(_demo_mod, "_gender_attempted", False)

    return {
        "detector": {
            "active": _detector_type or "nenhum",
            "yunet": {
                **yunet_file,
                "loaded": _yunet is not None,
                "attempted": _yunet_attempted,
            },
            "haar_fallback": _detector_type == "haar",
        },
        "reid": {
            "active": "sface" if sface_obj is not None else ("dct" if sface_tried else "nenhum"),
            "sface": {
                **sface_file,
                "loaded": sface_obj is not None,
                "attempted": sface_tried,
            },
        },
        "demographics": {
            "age": {
                **age_file,
                "loaded": age_obj is not None and age_ok,
                "attempted": age_tried,
            },
            "gender": {
                **gender_file,
                "loaded": gender_obj is not None and gender_ok,
                "attempted": gender_tried,
            },
        },
    }
