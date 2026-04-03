from __future__ import annotations

import importlib
import math
import threading
import time
from pathlib import Path
from typing import Any

_LOCK = threading.Lock()
_LATEST_JPEG: bytes | None = None
_LATEST_META: dict[str, Any] = {
    "running": False,
    "available": True,
    "last_error": "",
    "last_update_ts": 0.0,
    "width": 0,
    "height": 0,
    "faces": 0,
    "overlay_faces": False,
    "fps": 0,
    "camera_device": "",
}
_STOP_EVENT = threading.Event()
_THREAD: threading.Thread | None = None
_CURRENT_PARAMS: dict[str, Any] = {}


def _get_cv2():
    try:
        return importlib.import_module("cv2")
    except Exception:
        return None


def _resolve_device(device: str) -> int | str:
    raw = (device or "").strip()
    if raw.startswith("/dev/video"):
        suffix = raw.removeprefix("/dev/video")
        if suffix.isdigit():
            return int(suffix)
    if raw.isdigit():
        return int(raw)
    return raw


def _center(box: tuple[int, int, int, int]) -> tuple[float, float]:
    x, y, w, h = box
    return (x + (w / 2.0), y + (h / 2.0))


def _assign_face_ids(
    face_boxes: list[tuple[int, int, int, int]],
    prev_tracks: list[dict[str, float]],
    next_id: int,
    max_dist: float,
) -> tuple[list[dict[str, float]], int]:
    """Assign stable short IDs to faces by nearest centroid matching."""
    used_prev: set[int] = set()
    assigned: list[dict[str, float]] = []
    for box in face_boxes:
        cx, cy = _center(box)
        best_idx = -1
        best_dist = max_dist
        for idx, tr in enumerate(prev_tracks):
            if idx in used_prev:
                continue
            dist = math.hypot(cx - float(tr["cx"]), cy - float(tr["cy"]))
            if dist < best_dist:
                best_dist = dist
                best_idx = idx
        if best_idx >= 0:
            used_prev.add(best_idx)
            face_id = int(prev_tracks[best_idx]["id"])
        else:
            face_id = next_id
            next_id += 1
        x, y, w, h = box
        assigned.append({"id": face_id, "cx": cx, "cy": cy, "x": x, "y": y, "w": w, "h": h})
    return assigned, next_id


def _capture_loop(
    device: str,
    width: int,
    height: int,
    fps: int,
    overlay_faces: bool,
) -> None:
    global _LATEST_JPEG
    cv2 = _get_cv2()
    if cv2 is None:
        with _LOCK:
            _LATEST_META.update(
                {
                    "running": False,
                    "available": False,
                    "last_error": "OpenCV nao instalado. Instale opencv-python-headless.",
                    "camera_device": device,
                }
            )
        return

    face_cascade = None
    if overlay_faces:
        model_path = Path(cv2.data.haarcascades) / "haarcascade_frontalface_default.xml"
        face_cascade = cv2.CascadeClassifier(str(model_path))

    cap = cv2.VideoCapture(_resolve_device(device))
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, int(width))
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, int(height))
    cap.set(cv2.CAP_PROP_FPS, int(fps))
    if not cap.isOpened():
        with _LOCK:
            _LATEST_META.update(
                {
                    "running": False,
                    "available": True,
                    "last_error": f"Nao foi possivel abrir camera: {device}",
                    "camera_device": device,
                }
            )
        return

    with _LOCK:
        _LATEST_META.update(
            {
                "running": True,
                "available": True,
                "last_error": "",
                "overlay_faces": bool(overlay_faces),
                "camera_device": device,
                "fps": int(fps),
            }
        )

    min_interval = 1.0 / max(1, min(30, int(fps)))
    next_face_id = 1
    tracks: list[dict[str, float]] = []
    try:
        while not _STOP_EVENT.is_set():
            ok, frame = cap.read()
            if not ok or frame is None:
                with _LOCK:
                    _LATEST_META["last_error"] = "Falha ao capturar frame da camera."
                time.sleep(0.15)
                continue

            faces_count = 0
            if overlay_faces and face_cascade is not None:
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                faces = face_cascade.detectMultiScale(
                    gray,
                    scaleFactor=1.2,
                    minNeighbors=5,
                    minSize=(40, 40),
                )
                faces_count = len(faces)
                frame_w = int(frame.shape[1])
                max_dist = max(40.0, frame_w * 0.08)
                boxes = [
                    (int(x), int(y), int(w), int(h))
                    for (x, y, w, h) in faces
                ]
                tracks, next_face_id = _assign_face_ids(
                    boxes, tracks, next_face_id, max_dist
                )
                for tr in tracks:
                    x = int(tr["x"])
                    y = int(tr["y"])
                    w = int(tr["w"])
                    h = int(tr["h"])
                    face_id = int(tr["id"])
                    cv2.rectangle(frame, (x, y), (x + w, y + h), (30, 190, 255), 2)
                    cv2.putText(
                        frame,
                        f"ID-{face_id}",
                        (x, max(0, y - 8)),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.5,
                        (30, 190, 255),
                        1,
                        cv2.LINE_AA,
                    )
            else:
                tracks = []

            ok_jpg, encoded = cv2.imencode(
                ".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 82]
            )
            if ok_jpg:
                jpg_bytes = encoded.tobytes()
                now = time.time()
                with _LOCK:
                    _LATEST_JPEG = jpg_bytes
                    _LATEST_META.update(
                        {
                            "running": True,
                            "available": True,
                            "last_error": "",
                            "last_update_ts": now,
                            "width": int(frame.shape[1]),
                            "height": int(frame.shape[0]),
                            "faces": int(faces_count),
                            "overlay_faces": bool(overlay_faces),
                            "camera_device": device,
                            "fps": int(fps),
                        }
                    )
            time.sleep(min_interval)
    except Exception as exc:  # pragma: no cover
        with _LOCK:
            _LATEST_META.update({"running": False, "last_error": f"Erro no preview: {exc}"})
    finally:
        cap.release()
        with _LOCK:
            _LATEST_META["running"] = False


def start_preview(
    *,
    camera_device: str,
    width: int,
    height: int,
    fps: int,
    overlay_faces: bool,
) -> dict[str, Any]:
    global _THREAD, _CURRENT_PARAMS
    params = {
        "camera_device": camera_device,
        "width": int(width),
        "height": int(height),
        "fps": int(fps),
        "overlay_faces": bool(overlay_faces),
    }
    with _LOCK:
        same_params = params == _CURRENT_PARAMS
        thread_running = _THREAD is not None and _THREAD.is_alive()
    if same_params and thread_running:
        return {"started": False, "running": True}

    stop_preview()
    _STOP_EVENT.clear()
    _CURRENT_PARAMS = dict(params)
    _THREAD = threading.Thread(
        target=_capture_loop,
        kwargs={
            "device": camera_device,
            "width": int(width),
            "height": int(height),
            "fps": int(fps),
            "overlay_faces": bool(overlay_faces),
        },
        daemon=True,
    )
    _THREAD.start()
    return {"started": True, "running": True}


def stop_preview() -> dict[str, Any]:
    global _THREAD
    _STOP_EVENT.set()
    if _THREAD and _THREAD.is_alive():
        _THREAD.join(timeout=1.2)
    _THREAD = None
    with _LOCK:
        _LATEST_META["running"] = False
    return {"stopped": True}


def preview_jpeg() -> bytes | None:
    with _LOCK:
        return _LATEST_JPEG


def get_preview_status() -> dict[str, Any]:
    with _LOCK:
        return dict(_LATEST_META)

