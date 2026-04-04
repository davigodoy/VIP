from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import ValidationError

from .db import init_db
from .models import (
    CameraDeviceSelect,
    EventIngestRequest,
    RetentionConfig,
    ServiceScheduleCreate,
    ServiceScheduleOut,
    ServiceScheduleUpdate,
)
from .retention import (
    apply_camera_device,
    camera_status,
    create_schedule,
    get_update_history,
    get_update_status,
    delete_schedule,
    execute_cleanup,
    get_dashboard_charts,
    get_reconciliation_runs,
    get_reconciliation_status,
    get_live_metrics,
    ingest_event,
    latest_cleanup_runs,
    list_camera_devices,
    list_schedules,
    load_config,
    request_reconciliation_run,
    request_system_update_run,
    run_reconciliation_job,
    run_system_update_job,
    save_config,
    systemd_status,
    update_schedule,
)
from .camera_devices import list_detected_cameras
from .camera_preview import (
    HAS_CV2,
    get_last_jpeg,
    get_preview_status,
    iter_mjpeg,
    preview_capability,
    preview_disengage,
    preview_engage,
)
from .sheets_sync import get_sync_status, sync_events_to_google_sheets

BASE_DIR = Path(__file__).resolve().parent.parent
WEEKDAY_LABELS = {
    0: "Segunda",
    1: "Terca",
    2: "Quarta",
    3: "Quinta",
    4: "Sexta",
    5: "Sabado",
    6: "Domingo",
}

logger = logging.getLogger(__name__)

# Strong references so fire-and-forget thread jobs are not GC'd mid-flight.
_background_tasks: set[asyncio.Task[Any]] = set()


def _schedule_thread_job(func: Callable[..., Any], *args: Any) -> None:
    async def _runner() -> None:
        await asyncio.to_thread(func, *args)

    task = asyncio.create_task(_runner())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


app = FastAPI(title="Raspi Frequency Dashboard")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@app.on_event("startup")
async def startup() -> None:
    init_db()
    asyncio.create_task(_auto_cleanup_loop())
    asyncio.create_task(_google_sync_loop())


async def _auto_cleanup_loop() -> None:
    while True:
        config = load_config()
        now = datetime.now()
        should_run = (
            config.auto_cleanup_enabled
            and now.hour == config.auto_cleanup_hour
            and now.minute == 0
        )
        if should_run:
            execute_cleanup(dry_run=False)
            await asyncio.sleep(61)
            continue
        await asyncio.sleep(30)


async def _google_sync_loop() -> None:
    while True:
        cfg = load_config()
        interval = max(30, int(cfg.sync_interval_sec))
        if cfg.sync_google_sheets_enabled:
            try:
                await asyncio.to_thread(sync_events_to_google_sheets, 500)
            except Exception:
                logger.exception("Google Sheets sync loop failed")
        await asyncio.sleep(interval)


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    config = load_config()
    runs = latest_cleanup_runs(limit=15)
    reconciliation_runs = get_reconciliation_runs(limit=10)
    reconciliation_status = get_reconciliation_status()
    update_status = get_update_status(refresh_remote=False)
    schedules = list_schedules()
    cameras = list_camera_devices()
    cam_status = camera_status()
    svc_status = systemd_status()
    sheets_status = get_sync_status()
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "config": config.model_dump(),
            "runs": runs,
            "reconciliation_runs": reconciliation_runs,
            "reconciliation_status": reconciliation_status,
            "update_status": update_status,
            "schedules": schedules,
            "camera_devices": cameras,
            "detected_cameras": list_detected_cameras(),
            "camera_status": cam_status,
            "systemd_status": svc_status,
            "sheets_status": sheets_status,
            "weekday_labels": WEEKDAY_LABELS,
            "camera_preview": preview_capability(),
        },
    )


@app.get("/api/config", response_model=RetentionConfig)
async def get_config() -> RetentionConfig:
    return load_config()


@app.post("/api/config", response_model=RetentionConfig)
async def update_config(payload: RetentionConfig) -> RetentionConfig:
    save_config(payload)
    return load_config()


@app.get("/api/camera/status")
async def get_camera_status() -> JSONResponse:
    status = camera_status()
    status["available_devices"] = list_camera_devices()
    status["cameras"] = await asyncio.to_thread(list_detected_cameras)
    status["preview"] = preview_capability()
    return JSONResponse(content=status)


@app.get("/api/camera/devices")
async def api_camera_devices() -> JSONResponse:
    cameras = await asyncio.to_thread(list_detected_cameras)
    return JSONResponse(content={"cameras": cameras})


@app.post("/api/camera/device")
@app.post("/api/camera/device/")
@app.post("/api/camera/apply-device")
async def api_apply_camera_device(payload: CameraDeviceSelect) -> JSONResponse:
    """Grava o dispositivo na config (SQLite) para o preview e ingestao usarem na hora."""
    cfg = await asyncio.to_thread(apply_camera_device, payload.camera_device)
    return JSONResponse(
        content={"ok": True, "camera_device": cfg.camera_device},
    )


@app.get("/api/camera/preview/status")
async def camera_preview_status() -> JSONResponse:
    return JSONResponse(content=get_preview_status())


@app.post("/api/camera/preview/engage")
async def camera_preview_engage() -> JSONResponse:
    cfg = load_config()
    if not cfg.camera_enabled:
        raise HTTPException(
            status_code=503,
            detail="Camera desabilitada na configuracao.",
        )
    if not HAS_CV2:
        raise HTTPException(
            status_code=503,
            detail="OpenCV nao instalado. Execute: pip install opencv-python-headless",
        )
    return JSONResponse(content=preview_engage())


@app.post("/api/camera/preview/disengage")
async def camera_preview_disengage() -> JSONResponse:
    return JSONResponse(content=preview_disengage())


@app.get("/api/camera/preview/frame")
async def camera_preview_frame() -> Response:
    cfg = load_config()
    if not cfg.camera_enabled:
        raise HTTPException(status_code=503, detail="Camera desabilitada.")
    if not HAS_CV2:
        raise HTTPException(status_code=503, detail="OpenCV nao instalado.")
    jpeg = await asyncio.to_thread(get_last_jpeg)
    return Response(
        content=jpeg,
        media_type="image/jpeg",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
    )


@app.get("/api/camera/preview/stream")
async def camera_preview_stream(request: Request) -> StreamingResponse:
    cfg = load_config()
    if not cfg.camera_enabled:
        raise HTTPException(
            status_code=503,
            detail="Camera desabilitada na configuracao.",
        )
    if not HAS_CV2:
        raise HTTPException(
            status_code=503,
            detail="OpenCV nao instalado. Execute: pip install opencv-python-headless",
        )
    return StreamingResponse(
        iter_mjpeg(request),
        media_type="multipart/x-mixed-replace; boundary=frame",
    )


@app.get("/api/sync/status")
async def api_sync_status() -> JSONResponse:
    return JSONResponse(content=get_sync_status())


@app.post("/api/sync/run")
async def api_sync_run() -> JSONResponse:
    result = await asyncio.to_thread(sync_events_to_google_sheets, 1000)
    return JSONResponse(content=result)


@app.get("/api/schedules", response_model=list[ServiceScheduleOut])
async def get_schedules() -> list[ServiceScheduleOut]:
    return list_schedules()


@app.post("/api/schedules", response_model=list[ServiceScheduleOut])
async def add_schedule(payload: ServiceScheduleCreate) -> list[ServiceScheduleOut]:
    create_schedule(payload)
    return list_schedules()


@app.put("/api/schedules/{schedule_id}", response_model=list[ServiceScheduleOut])
async def put_schedule(
    schedule_id: int, payload: ServiceScheduleUpdate
) -> list[ServiceScheduleOut]:
    ok = update_schedule(schedule_id, payload)
    if not ok:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return list_schedules()


@app.delete("/api/schedules/{schedule_id}", response_model=list[ServiceScheduleOut])
async def remove_schedule(schedule_id: int) -> list[ServiceScheduleOut]:
    ok = delete_schedule(schedule_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return list_schedules()


@app.post("/api/cleanup")
async def cleanup(payload: dict[str, bool]) -> JSONResponse:
    dry_run = bool(payload.get("dry_run", True))
    result = execute_cleanup(dry_run=dry_run)
    return JSONResponse(content=result)


@app.post("/api/events/ingest")
async def api_ingest_event(payload: EventIngestRequest) -> JSONResponse:
    try:
        result = ingest_event(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(content=result)


@app.get("/api/metrics/live")
async def api_live_metrics(culto_id: str | None = None) -> JSONResponse:
    return JSONResponse(content=get_live_metrics(culto_id=culto_id))


@app.get("/api/metrics/charts")
async def api_metrics_charts(culto_id: str | None = None) -> JSONResponse:
    return JSONResponse(content=get_dashboard_charts(culto_id=culto_id))


@app.get("/api/reconciliation/status")
async def api_reconciliation_status() -> JSONResponse:
    return JSONResponse(content=get_reconciliation_status())


@app.get("/api/reconciliation/runs")
async def api_reconciliation_runs(limit: int = 20) -> JSONResponse:
    safe_limit = max(1, min(limit, 100))
    return JSONResponse(content={"runs": get_reconciliation_runs(limit=safe_limit)})


@app.post("/api/reconciliation/run")
async def api_reconciliation_run() -> JSONResponse:
    result = await asyncio.to_thread(request_reconciliation_run)
    if result.get("accepted"):
        run_id = str(result.get("run_id", "")).strip()
        if run_id:
            _schedule_thread_job(run_reconciliation_job, run_id)
    return JSONResponse(content=result)


@app.get("/api/update/status")
async def api_update_status(
    refresh_remote: bool = Query(False, description="Fetch from origin before comparing commits"),
) -> JSONResponse:
    return JSONResponse(content=get_update_status(refresh_remote=refresh_remote))


@app.get("/api/update/history")
async def api_update_history(limit: int = 20) -> JSONResponse:
    safe_limit = max(1, min(limit, 100))
    return JSONResponse(content={"runs": get_update_history(limit=safe_limit)})


@app.post("/api/update/run")
async def api_update_run() -> JSONResponse:
    result = await asyncio.to_thread(request_system_update_run)
    if result.get("accepted"):
        run_id = str(result.get("run_id", "")).strip()
        if run_id:
            _schedule_thread_job(run_system_update_job, run_id)
    return JSONResponse(content=result)


@app.post("/config/save")
async def save_config_form(
    retencao_temp_id_horas: int = Form(...),
    retencao_profile_dias: int = Form(...),
    retencao_eventos_dias: int = Form(...),
    retencao_agregados_meses: int = Form(...),
    retencao_imagens_horas: int = Form(...),
    janela_reentrada_min: int = Form(...),
    limiar_match: float = Form(...),
    auto_cleanup_enabled: str | None = Form(None),
    auto_cleanup_hour: int = Form(...),
    camera_device: str = Form(...),
    camera_label: str = Form(...),
    camera_enabled_hidden: int = Form(1, ge=0, le=1),
    camera_inference_width: int = Form(...),
    camera_inference_height: int = Form(...),
    camera_fps: int = Form(...),
    culto_antecedencia_min: int = Form(...),
    culto_duracao_min: int = Form(...),
    estimar_faixa_etaria: str | None = Form(None),
    estimar_genero: str | None = Form(None),
    sync_google_sheets_enabled: str | None = Form(None),
    sync_interval_sec: int = Form(...),
    sync_spreadsheet_id: str = Form(""),
    sync_worksheet_name: str = Form(...),
    sync_credentials_source: str = Form("env"),
    sync_credentials_env_var: str = Form("VIP_GSHEETS_CREDENTIALS_JSON"),
    sync_credentials_file_path: str = Form(""),
    sync_credentials_json: str = Form(""),
    idade_limite_crianca: int = Form(...),
    idade_limite_junior: int = Form(...),
    idade_limite_adolescente: int = Form(...),
    idade_limite_jovem: int = Form(...),
    idade_limite_adulto: int = Form(...),
) -> RedirectResponse:
    try:
        payload = RetentionConfig(
            retencao_temp_id_horas=retencao_temp_id_horas,
            retencao_profile_dias=retencao_profile_dias,
            retencao_eventos_dias=retencao_eventos_dias,
            retencao_agregados_meses=retencao_agregados_meses,
            retencao_imagens_horas=retencao_imagens_horas,
            janela_reentrada_min=janela_reentrada_min,
            limiar_match=limiar_match,
            auto_cleanup_enabled=auto_cleanup_enabled is not None,
            auto_cleanup_hour=auto_cleanup_hour,
            camera_device=camera_device.strip(),
            camera_label=camera_label.strip(),
            camera_enabled=bool(camera_enabled_hidden),
            camera_inference_width=camera_inference_width,
            camera_inference_height=camera_inference_height,
            camera_fps=camera_fps,
            culto_antecedencia_min=culto_antecedencia_min,
            culto_duracao_min=culto_duracao_min,
            estimar_faixa_etaria=estimar_faixa_etaria is not None,
            estimar_genero=estimar_genero is not None,
            sync_google_sheets_enabled=sync_google_sheets_enabled is not None,
            sync_interval_sec=sync_interval_sec,
            sync_spreadsheet_id=sync_spreadsheet_id.strip(),
            sync_worksheet_name=sync_worksheet_name.strip(),
            sync_credentials_source=sync_credentials_source.strip(),
            sync_credentials_env_var=sync_credentials_env_var.strip(),
            sync_credentials_file_path=sync_credentials_file_path.strip(),
            sync_credentials_json=sync_credentials_json.strip(),
            idade_limite_crianca=idade_limite_crianca,
            idade_limite_junior=idade_limite_junior,
            idade_limite_adolescente=idade_limite_adolescente,
            idade_limite_jovem=idade_limite_jovem,
            idade_limite_adulto=idade_limite_adulto,
        )
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc
    save_config(payload)
    return RedirectResponse(url="/", status_code=303)


@app.post("/cleanup/run")
async def run_cleanup(dry_run: bool = Form(True)) -> RedirectResponse:
    execute_cleanup(dry_run=dry_run)
    return RedirectResponse(url="/", status_code=303)


@app.post("/schedules/create")
async def create_schedule_form(
    service_name: str = Form(...),
    day_of_week: int = Form(...),
    start_time: str = Form(...),
    is_active: str | None = Form(None),
) -> RedirectResponse:
    payload = ServiceScheduleCreate(
        service_name=service_name.strip(),
        day_of_week=day_of_week,
        start_time=start_time,
        is_active=is_active is not None,
    )
    create_schedule(payload)
    return RedirectResponse(url="/", status_code=303)


@app.post("/schedules/{schedule_id}/save")
async def update_schedule_form(
    schedule_id: int,
    service_name: str = Form(...),
    day_of_week: int = Form(...),
    start_time: str = Form(...),
    is_active: str | None = Form(None),
) -> RedirectResponse:
    payload = ServiceScheduleUpdate(
        service_name=service_name.strip(),
        day_of_week=day_of_week,
        start_time=start_time,
        is_active=is_active is not None,
    )
    update_schedule(schedule_id, payload)
    return RedirectResponse(url="/", status_code=303)


@app.post("/schedules/{schedule_id}/delete")
async def delete_schedule_form(schedule_id: int) -> RedirectResponse:
    delete_schedule(schedule_id)
    return RedirectResponse(url="/", status_code=303)
