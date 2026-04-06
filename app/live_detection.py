"""
Deteccao continua de pessoas (OpenCV HOG) no mesmo fluxo da camera que o preview.
Gera entradas/saidas via ingest_event — independente do browser aberto.

Nota: cada perda de track gera novo `hog_*` e conta como pessoa unica nova.
Emparelhamento por IoU + distancia reduz troca de id, mas **unicos HOG != pessoas
reais** (use edge com track estavel para identidade seria).
"""
from __future__ import annotations

import logging
import math
import threading
from typing import Any

import numpy as np

from .demographics_opencv import estimate_demographics_optional
from .models import EventIngestRequest, GenderBand
from .retention import ingest_event, load_config

logger = logging.getLogger(__name__)

try:
    import cv2

    HAS_CV2 = True
except ImportError:
    cv2 = None  # type: ignore[assignment]
    HAS_CV2 = False

_hog: Any | None = None
_hog_lock = threading.Lock()

_state_lock = threading.Lock()
_tracks: dict[int, dict[str, Any]] = {}
_next_tid = 1
_last_cfg_off = True

# Distancia maxima (pixels no frame redimensionado) para manter o mesmo track
_MATCH_DIST = 130.0
# Sobreposicao minima (IoU) para considerar a mesma pessoa quando o centro salta
_MIN_IOU_MATCH = 0.08
# Frames sem deteccao antes de contar saida (com ~8 FPS ~4s — reduz saidas por flicker do HOG)
_MAX_MISSES = 32
# Largura maxima do lado maior ao correr HOG (velocidade no Pi)
_DETECT_MAX_SIDE = 520
# > 0 reduz falsos positivos (0.0 aceita quase tudo). Subir se ainda houver fantasmas.
_HOG_HIT_THRESHOLD = 0.28
# Stride maior = menos sensibilidade a ruido, um pouco mais rapido no Pi
_HOG_WIN_STRIDE = 16
# Caixas ridiculamente pequenas (sombras, artefactos) ignoradas no frame small
_HOG_MIN_W = 28
_HOG_MIN_H = 56
# Silhueta vertical tipica de pessoa em pe; fora disto o HOG costuma errar
_HOG_MIN_AR = 1.15
_HOG_MAX_AR = 4.2
# Fundir deteccoes sobrepostas (mesma pessoa, varias caixas)
_HOG_GROUP_EPS = 0.35


def _iou_xywh(
    a: tuple[float, float, float, float], b: tuple[float, float, float, float]
) -> float:
    """IoU entre retangulos (x, y, w, h) no mesmo espaco de coordenadas."""
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


def _get_hog() -> Any | None:
    global _hog
    if not HAS_CV2 or cv2 is None:
        return None
    with _hog_lock:
        if _hog is None:
            hog = cv2.HOGDescriptor()
            hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())
            _hog = hog
        return _hog


def reset_tracks() -> None:
    global _tracks, _next_tid
    with _state_lock:
        _tracks.clear()
        _next_tid = 1


def on_frame_bgr(frame: np.ndarray) -> None:
    """Chamado a partir do thread de captura apos ler um frame BGR."""
    global _next_tid, _last_cfg_off

    if not HAS_CV2 or frame is None or frame.size == 0:
        return

    cfg = load_config()
    if not cfg.camera_enabled or not cfg.live_detection_enabled:
        if not _last_cfg_off:
            reset_tracks()
        _last_cfg_off = True
        return
    _last_cfg_off = False

    hog = _get_hog()
    if hog is None:
        return

    h, w = frame.shape[:2]
    side = max(h, w)
    scale = min(1.0, _DETECT_MAX_SIDE / float(side)) if side > 0 else 1.0
    small_w = max(1, int(w * scale))
    small_h = max(1, int(h * scale))
    small = cv2.resize(frame, (small_w, small_h), interpolation=cv2.INTER_AREA)
    gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)

    try:
        out = hog.detectMultiScale(
            gray,
            winStride=(_HOG_WIN_STRIDE, _HOG_WIN_STRIDE),
            padding=(16, 16),
            scale=1.06,
            hitThreshold=_HOG_HIT_THRESHOLD,
        )
    except Exception as exc:
        logger.warning("HOG detectMultiScale falhou: %s", exc)
        return

    if isinstance(out, tuple) and len(out) >= 2:
        rects = out[0]
    else:
        rects = out

    rect_list: list[list[int]] = []
    if rects is not None and len(rects) > 0:
        for (_i, (x, y, rw, rh)) in enumerate(rects):
            xi, yi, wi, hi = int(x), int(y), int(rw), int(rh)
            if wi < _HOG_MIN_W or hi < _HOG_MIN_H:
                continue
            ar = hi / float(max(1, wi))
            if ar < _HOG_MIN_AR or ar > _HOG_MAX_AR:
                continue
            rect_list.append([xi, yi, wi, hi])

    if rect_list:
        cv2.groupRectangles(rect_list, groupThreshold=1, eps=_HOG_GROUP_EPS)

    # (cx, cy, sz, x, y, rw, rh) em coordenadas do frame redimensionado (small)
    detections: list[tuple[float, float, float, float, float, float, float]] = []
    for (xi, yi, wi, hi) in rect_list:
        cx = float(xi + wi / 2.0)
        cy = float(yi + hi / 2.0)
        detections.append(
            (cx, cy, float(max(wi, hi)), float(xi), float(yi), float(wi), float(hi))
        )

    to_exit: list[int] = []
    to_enter: list[int] = []
    enter_rect_small: dict[int, tuple[float, float, float, float]] = {}
    with _state_lock:
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
            else:
                tr["misses"] = int(tr["misses"]) + 1
                if tr["misses"] >= _MAX_MISSES:
                    del _tracks[tid]
                    to_exit.append(tid)

        for i, (cx, cy, sz, xs, ys, rws, rhs) in enumerate(detections):
            if i in used_j:
                continue
            tid = _next_tid
            _next_tid += 1
            rect = (xs, ys, rws, rhs)
            _tracks[tid] = {
                "cx": cx,
                "cy": cy,
                "sz": sz,
                "misses": 0,
                "rect_small": rect,
            }
            to_enter.append(tid)
            enter_rect_small[tid] = rect

    for tid in to_enter:
        _emit_entrada(tid, frame, small_w, small_h, w, h, enter_rect_small.get(tid))
    for tid in to_exit:
        _emit_saida(tid)


def _emit_entrada(
    track_id: int,
    frame_bgr: np.ndarray,
    small_w: int,
    small_h: int,
    full_w: int,
    full_h: int,
    rect_small: tuple[float, float, float, float] | None,
) -> None:
    pid = f"hog_{track_id}"
    cfg = load_config()
    want_age = bool(cfg.estimar_faixa_etaria)
    want_gender = bool(cfg.estimar_genero)
    age_est: int | None = None
    gender_band: GenderBand | None = None

    if (want_age or want_gender) and rect_small is not None and HAS_CV2 and cv2 is not None:
        sx, sy, srw, srh = rect_small
        if small_w > 0 and small_h > 0 and full_w > 0 and full_h > 0:
            fx = full_w / float(small_w)
            fy = full_h / float(small_h)
            x0 = int(sx * fx)
            y0 = int(sy * fy)
            x1 = int((sx + srw) * fx)
            y1 = int((sy + srh) * fy)
            x0 = max(0, min(x0, full_w - 1))
            y0 = max(0, min(y0, full_h - 1))
            x1 = max(x0 + 1, min(x1, full_w))
            y1 = max(y0 + 1, min(y1, full_h))
            crop = frame_bgr[y0:y1, x0:x1]
            if crop.size > 0:
                age_est, g = estimate_demographics_optional(
                    crop, want_age=want_age, want_gender=want_gender
                )
                if g in ("homem", "mulher"):
                    gender_band = g

    try:
        req = EventIngestRequest(
            person_id=pid,
            direction="entrada",
            age_estimate=age_est,
            gender=gender_band,
        )
        ingest_event(req)
    except Exception as exc:
        logger.warning("ingest entrada falhou (%s): %s", pid, exc)


def _emit_saida(track_id: int) -> None:
    pid = f"hog_{track_id}"
    try:
        ingest_event(
            EventIngestRequest(person_id=pid, direction="saida")
        )
    except Exception as exc:
        logger.warning("ingest saida falhou (%s): %s", pid, exc)
