from __future__ import annotations

import asyncio
import platform
import threading
import time
from typing import Any

from .retention import load_config

try:
    import cv2  # type: ignore
    import numpy as np

    _CV2 = cv2
    _NP = np
    HAS_CV2 = True
except ImportError:
    _CV2 = None
    _NP = None
    HAS_CV2 = False

_LOCK = threading.Lock()
_SUBSCRIBERS = 0
_THREAD: threading.Thread | None = None
_STOP = threading.Event()
_LAST_JPEG: bytes = b""
_LAST_META: dict[str, Any] = {
    "error": "",
    "fps": 0.0,
}


def preview_capability() -> dict[str, Any]:
    return {
        "opencv_installed": HAS_CV2,
    }


def _placeholder_jpeg(message: str) -> bytes:
    if not HAS_CV2 or _CV2 is None or _NP is None:
        return b""
    img = _NP.zeros((240, 640, 3), dtype=_NP.uint8)
    y0 = 100
    for i, line in enumerate(message.split("\n")[:4]):
        _CV2.putText(
            img,
            line[:60],
            (20, y0 + i * 28),
            _CV2.FONT_HERSHEY_SIMPLEX,
            0.55,
            (200, 200, 200),
            1,
            _CV2.LINE_AA,
        )
    ok, buf = _CV2.imencode(".jpg", img, [int(_CV2.IMWRITE_JPEG_QUALITY), 75])
    return buf.tobytes() if ok else b""


def _open_capture(device: str) -> Any | None:
    assert HAS_CV2 and _CV2 is not None
    d = device.strip()
    is_mac = platform.system() == "Darwin"
    is_linux = platform.system() == "Linux"

    def try_open(spec: tuple[Any, ...]) -> Any | None:
        try:
            if len(spec) == 2:
                cap = _CV2.VideoCapture(spec[0], spec[1])
            else:
                cap = _CV2.VideoCapture(spec[0])
        except Exception:
            return None
        if cap.isOpened():
            return cap
        cap.release()
        return None

    if is_linux:
        v4l2 = getattr(_CV2, "CAP_V4L2", None)
        if v4l2 is not None:
            if d.startswith("/dev/video"):
                cap = try_open((d, v4l2))
                if cap is not None:
                    return cap
            if d.isdigit():
                cap = try_open((int(d), v4l2))
                if cap is not None:
                    return cap
            cap = try_open((0, v4l2))
            if cap is not None:
                return cap
        if d.isdigit():
            cap = try_open((int(d),))
            if cap is not None:
                return cap
        cap = try_open((d,))
        if cap is not None:
            return cap
        return try_open((0,))

    if is_mac:
        av = getattr(_CV2, "CAP_AVFOUNDATION", None)
        if av is not None:
            idx = int(d) if d.isdigit() else 0
            cap = try_open((idx, av))
            if cap is not None:
                return cap
            if not d.isdigit() and d.startswith("/dev"):
                cap = try_open((0, av))
                if cap is not None:
                    return cap
        if d.isdigit():
            cap = try_open((int(d),))
        else:
            cap = try_open((d,))
        if cap is not None:
            return cap
        if av is not None and (d.startswith("/dev/video") or d == "/dev/video0"):
            return try_open((0, av))
        if d.startswith("/dev/video") or d == "/dev/video0":
            return try_open((0,))
        return None

    if d.isdigit():
        cap = try_open((int(d),))
    else:
        cap = try_open((d,))
    if cap is not None:
        return cap
    return try_open((0,))


def _capture_loop() -> None:
    global _LAST_JPEG, _LAST_META
    assert HAS_CV2 and _CV2 is not None
    cap: Any | None = None
    last_cfg: tuple[Any, ...] = ()
    t0 = time.perf_counter()
    n_frames = 0

    while not _STOP.is_set():
        with _LOCK:
            subs = _SUBSCRIBERS
        if subs <= 0:
            time.sleep(0.05)
            if cap is not None:
                cap.release()
                cap = None
            continue

        cfg = load_config()
        if not cfg.camera_enabled:
            jpeg = _placeholder_jpeg("Camera desabilitada\nna configuracao")
            with _LOCK:
                _LAST_JPEG = jpeg
                _LAST_META = {"error": "disabled", "fps": 0.0}
            time.sleep(0.5)
            continue

        key = (
            cfg.camera_device,
            cfg.camera_inference_width,
            cfg.camera_inference_height,
            cfg.camera_fps,
        )
        if cap is None or key != last_cfg:
            if cap is not None:
                cap.release()
                cap = None
            last_cfg = key
            cap = _open_capture(cfg.camera_device)
            if cap is None:
                hint = ""
                if platform.system() == "Linux":
                    hint = "\nPi/Linux: grupo video?\nsudo usermod -aG video $USER"
                elif platform.system() == "Darwin":
                    hint = "\nMac: Privacidade > Camera\n(permita Terminal/Cursor)"
                jpeg = _placeholder_jpeg(
                    f"Nao abriu a camera:\n{cfg.camera_device}\n(Pi: /dev/video0){hint}"
                )
                with _LOCK:
                    _LAST_JPEG = jpeg
                    _LAST_META = {"error": "open_failed", "fps": 0.0}
                time.sleep(1.0)
                continue
            cap.set(_CV2.CAP_PROP_FRAME_WIDTH, cfg.camera_inference_width)
            cap.set(_CV2.CAP_PROP_FRAME_HEIGHT, cfg.camera_inference_height)

        interval = max(1.0 / max(1, min(30, cfg.camera_fps)), 0.02)
        ok, frame = cap.read()
        if not ok or frame is None:
            jpeg = _placeholder_jpeg("Falha ao ler frame")
            with _LOCK:
                _LAST_JPEG = jpeg
                _LAST_META = {"error": "read_failed", "fps": 0.0}
            time.sleep(0.3)
            continue

        enc_ok, buf = _CV2.imencode(
            ".jpg", frame, [int(_CV2.IMWRITE_JPEG_QUALITY), 78]
        )
        jpeg = buf.tobytes() if enc_ok else b""

        n_frames += 1
        elapsed = time.perf_counter() - t0
        fps = n_frames / elapsed if elapsed > 0.5 else 0.0
        if elapsed > 2.0:
            t0 = time.perf_counter()
            n_frames = 0

        with _LOCK:
            _LAST_JPEG = jpeg
            _LAST_META = {"error": "", "fps": round(fps, 1)}

        time.sleep(interval)

    if cap is not None:
        cap.release()


def _ensure_thread() -> None:
    global _THREAD
    with _LOCK:
        if _THREAD is None or not _THREAD.is_alive():
            _STOP.clear()
            _THREAD = threading.Thread(target=_capture_loop, daemon=True)
            _THREAD.start()


def subscribe() -> None:
    global _SUBSCRIBERS
    with _LOCK:
        _SUBSCRIBERS += 1
    if HAS_CV2:
        _ensure_thread()


def unsubscribe() -> None:
    global _SUBSCRIBERS
    with _LOCK:
        _SUBSCRIBERS = max(0, _SUBSCRIBERS - 1)


def get_preview_status() -> dict[str, Any]:
    cap = preview_capability()
    with _LOCK:
        meta = dict(_LAST_META)
        has_frame = len(_LAST_JPEG) > 0
    hint = ""
    err = meta.get("error")
    if platform.system() == "Linux" and err == "open_failed":
        hint = (
            "No Raspberry Pi: confirme /dev/video0, usuario no grupo 'video' "
            "(sudo usermod -aG video pi) e reinicie a sessao; camera USB bem encaixada."
        )
    elif platform.system() == "Darwin" and err in {"open_failed", "read_failed"}:
        hint = (
            "No Mac: Ajustes > Privacidade e seguranca > Camera — permita Terminal, Cursor ou o app que corre o uvicorn."
        )
    out = {
        **cap,
        **meta,
        "has_frame": has_frame,
        "subscribers": _SUBSCRIBERS,
        "hint": hint,
    }
    return out


def get_last_jpeg() -> bytes:
    with _LOCK:
        jpeg = bytes(_LAST_JPEG)
    if jpeg:
        return jpeg
    return _placeholder_jpeg("Aguardando camera...\nClique em Iniciar preview") if HAS_CV2 else b""


def preview_engage() -> dict[str, Any]:
    subscribe()
    with _LOCK:
        n = _SUBSCRIBERS
    return {"ok": True, "subscribers": n}


def preview_disengage() -> dict[str, Any]:
    unsubscribe()
    with _LOCK:
        n = _SUBSCRIBERS
    return {"ok": True, "subscribers": n}


def get_mjpeg_part() -> bytes:
    with _LOCK:
        jpeg = _LAST_JPEG
    if not jpeg:
        jpeg = _placeholder_jpeg("Aguardando camera...") if HAS_CV2 else b""
    return b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + jpeg + b"\r\n"


async def iter_mjpeg(request: Any) -> Any:
    """Yields multipart MJPEG chunks; subscribe/release around the stream lifetime."""
    subscribe()
    try:
        while True:
            if await request.is_disconnected():
                break
            part = await asyncio.to_thread(get_mjpeg_part)
            yield part
            cfg_inner = load_config()
            delay = max(0.02, 1.0 / max(1, min(30, cfg_inner.camera_fps)))
            await asyncio.sleep(delay)
    finally:
        unsubscribe()
