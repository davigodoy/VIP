from __future__ import annotations

import platform
import re
from pathlib import Path
from typing import Any


def _video_sort_key(device_id: str) -> tuple[int, str]:
    if device_id.isdigit():
        return (int(device_id), device_id)
    name = Path(device_id).name
    m = re.fullmatch(r"video(\d+)", name)
    if m:
        return (int(m.group(1)), device_id)
    return (100_000, device_id)


def _v4l_index_from_video_name(name: str) -> int:
    """Ordem estavel: video0, video1, ... (symlinks by-id ordenam errado por string)."""
    m = re.fullmatch(r"video(\d+)", name)
    return int(m.group(1)) if m else 10_000


def _read_sysfs_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace").strip()
    except OSError:
        return ""


def _linux_v4l_stable_paths() -> dict[str, str]:
    """
    Mapeia o caminho canonico do no (ex. /dev/video0) para um symlink estavel
    em /dev/v4l/by-id/ ou by-path/ (melhor para identificar a mesma camera apos reboot).
    """
    best: dict[str, str] = {}
    for kind in ("by-id", "by-path"):
        root = Path("/dev/v4l") / kind
        if not root.is_dir():
            continue
        for link in sorted(root.iterdir()):
            if not link.is_symlink():
                continue
            try:
                real = str(link.resolve())
            except OSError:
                continue
            cand = str(link)
            if kind == "by-id":
                prev = best.get(real)
                if prev is None or len(cand) <= len(prev):
                    best[real] = cand
            else:
                if real not in best:
                    best[real] = cand
    return best


def _v4l_usb_friendly_name(video_sub: Path) -> str:
    """Nome amigavel a partir da cadeia USB (product / manufacturer)."""
    device = video_sub / "device"
    if not device.exists():
        return ""
    try:
        cur = device.resolve()
    except OSError:
        return ""
    product, manufacturer = "", ""
    for _ in range(10):
        if not product:
            product = _read_sysfs_text(cur / "product")
        if not manufacturer:
            manufacturer = _read_sysfs_text(cur / "manufacturer")
        if product and manufacturer:
            break
        parent = cur.parent
        if parent == cur:
            break
        cur = parent
    # Produto antes do fabricante costuma ler melhor (ex. "HD Webcam C920 (Logitech)").
    parts = [p for p in (product, manufacturer) if p]
    return " ".join(parts).strip()


def _darwin_avfoundation_ordered_cameras() -> list[dict[str, str]]:
    """
    Mesma regra do OpenCV 4.x (cap_avfoundation_mac.mm): dispositivos de
    AVMediaTypeVideo + AVMediaTypeMuxed, depois ordenados por uniqueID
    (comparacao de NSString). O indice i corresponde a VideoCapture(i, CAP_AVFOUNDATION).
    """
    if platform.system() != "Darwin":
        return []
    try:
        from AVFoundation import (  # type: ignore[import-untyped]
            AVCaptureDevice,
            AVMediaTypeMuxed,
            AVMediaTypeVideo,
        )
    except ImportError:
        return []
    combined: list[Any] = []
    try:
        for media_type in (AVMediaTypeVideo, AVMediaTypeMuxed):
            arr = AVCaptureDevice.devicesWithMediaType_(media_type)
            if arr is not None:
                combined.extend(list(arr))
    except Exception:
        return []
    if not combined:
        return []
    try:
        combined.sort(key=lambda dev: str(dev.uniqueID()))
    except Exception:
        return []
    out: list[dict[str, str]] = []
    for dev in combined:
        try:
            uid = str(dev.uniqueID())
            name = str(dev.localizedName())
        except Exception:
            continue
        if uid:
            out.append({"unique_id": uid, "name": name})
    return out


def _linux_sysfs_cameras() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    base = Path("/sys/class/video4linux")
    if not base.is_dir():
        return items
    stable = _linux_v4l_stable_paths()
    for sub in sorted(base.iterdir(), key=lambda p: _video_sort_key(p.name)):
        if not sub.is_dir() or not sub.name.startswith("video"):
            continue
        dev_path = Path("/dev") / sub.name
        if not dev_path.exists():
            continue
        try:
            real = str(dev_path.resolve())
        except OSError:
            real = str(dev_path)
        pref_id = stable.get(real, str(dev_path))
        card = _read_sysfs_text(sub / "name")
        usb_name = _v4l_usb_friendly_name(sub)
        if usb_name and card and card.lower() not in usb_name.lower():
            title = f"{usb_name} ({card})"
        elif usb_name:
            title = usb_name
        elif card:
            title = card
        else:
            title = "Camera"
        label = f"{title} — {pref_id}"
        items.append(
            {
                "id": pref_id,
                "label": label,
                "source": "v4l2",
                "card": card,
                "usb_name": usb_name or None,
                "_v4l_idx": _v4l_index_from_video_name(sub.name),
            }
        )
    return items


def _linux_dev_glob_extra(known_ids: set[str], stable: dict[str, str]) -> list[dict[str, Any]]:
    extra: list[dict[str, Any]] = []
    for p in Path("/dev").glob("video*"):
        if not p.exists():
            continue
        try:
            real = str(p.resolve())
        except OSError:
            real = str(p)
        sid = stable.get(real, str(p))
        if sid in known_ids:
            continue
        name = _read_sysfs_text(Path("/sys/class/video4linux") / p.name / "name")
        title = name or p.name
        label = f"{title} — {sid}" if name else sid
        extra.append(
            {
                "id": sid,
                "label": label,
                "source": "dev",
                "_v4l_idx": _v4l_index_from_video_name(p.name),
            }
        )
    return extra


def _darwin_opencv_probe(max_idx: int = 6) -> list[dict[str, Any]]:
    try:
        import cv2
    except ImportError:
        return []
    av = getattr(cv2, "CAP_AVFOUNDATION", None)
    if av is None:
        return []
    av_cams = _darwin_avfoundation_ordered_cameras()
    found: list[dict[str, Any]] = []
    for i in range(max_idx):
        cap = None
        try:
            cap = cv2.VideoCapture(i, av)
            if cap.isOpened():
                meta = av_cams[i] if i < len(av_cams) else None
                prof = (meta or {}).get("name", "").strip()
                uid = (meta or {}).get("unique_id")
                label = (
                    f"{prof} — indice {i}"
                    if prof
                    else f"Indice {i} — camera respondeu ao teste"
                )
                found.append(
                    {
                        "id": str(i),
                        "label": label,
                        "source": "opencv",
                        "system_name": prof or None,
                        "av_unique_id": uid,
                    }
                )
        finally:
            if cap is not None:
                cap.release()
    return found


def list_detected_cameras() -> list[dict[str, Any]]:
    """
    Lista cameras para o painel. Alvo principal de implantacao: Linux (Raspberry Pi).

    - Linux/Pi: nome USB (sysfs), nome da placa V4L2, id estavel /dev/v4l/by-id/ quando existir.
    - macOS (dev): nomes alinhados ao OpenCV via AVFoundation + ordenacao por uniqueID;
      depende de pyobjc-framework-AVFoundation (nao instalado no Pi).
    """
    system = platform.system()
    if system == "Linux":
        items = _linux_sysfs_cameras()
        known = {c["id"] for c in items}
        stable = _linux_v4l_stable_paths()
        items.extend(_linux_dev_glob_extra(known, stable))
        items.sort(key=lambda c: (c.get("_v4l_idx", 10_000), str(c["id"])))
        for c in items:
            c.pop("_v4l_idx", None)
        return items
    if system == "Darwin":
        found = _darwin_opencv_probe()
        if found:
            return found
        try:
            import cv2  # noqa: F401
        except ImportError:
            hint = "instale opencv-python-headless para teste automatico"
        else:
            hint = "permissao de Camera no Mac ou nenhuma camera; ainda pode funcionar no preview"
        av_cams = _darwin_avfoundation_ordered_cameras()
        out: list[dict[str, Any]] = []
        for i in range(3):
            meta = av_cams[i] if i < len(av_cams) else None
            prof = (meta or {}).get("name", "").strip()
            uid = (meta or {}).get("unique_id")
            lab = f"{prof} — indice {i}" if prof else f"Indice {i} ({hint})"
            out.append(
                {
                    "id": str(i),
                    "label": lab,
                    "source": "hint",
                    "system_name": prof or None,
                    "av_unique_id": uid,
                }
            )
        return out
    items = []
    for p in sorted(Path("/dev").glob("video*"), key=lambda x: _video_sort_key(str(x))):
        if p.exists():
            sid = str(p)
            items.append({"id": sid, "label": sid, "source": "dev"})
    return items
