from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any
from datetime import UTC, datetime
from pathlib import Path

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

from .db import get_connection, init_db
from .models import (
    CameraDeviceSelect,
    EventIngestRequest,
    InvolvementRulesUpdate,
    PersonasResetRequest,
    ReconciliationApplyRequest,
    RetentionConfig,
    ServiceScheduleCreate,
    ServiceScheduleOut,
    ServiceScheduleUpdate,
    SyncAutoSetupRequest,
)
from .retention import (
    apply_camera_device,
    apply_reconciliation_from_browser,
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
    get_people_involvement,
    ingest_event,
    latest_cleanup_runs,
    list_camera_devices,
    list_events_for_reconciliation_export,
    list_schedules,
    load_config,
    reset_identified_personas,
    validate_person,
    reject_person,
    merge_persons,
    wipe_all_test_data,
    request_reconciliation_run,
    request_system_update_run,
    run_reconciliation_job,
    run_system_update_job,
    save_config,
    systemd_status,
    update_involvement_rules,
    update_schedule,
)
from .camera_devices import list_detected_cameras
from .camera_preview import (
    HAS_CV2,
    ensure_background_capture,
    get_last_jpeg,
    get_preview_status,
    iter_mjpeg,
    preview_capability,
    preview_disengage,
    preview_engage,
)
from .sheets_sync import (
    auto_setup_sync_from_spreadsheet,
    get_sync_status,
    inspect_sync_spreadsheet,
    sync_events_to_google_sheets,
)

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
    ensure_background_capture()
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
            "api_root": str(request.base_url).rstrip("/"),
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


@app.get("/healthz")
async def healthz() -> JSONResponse:
    checks: dict[str, Any] = {}
    overall_ok = True

    try:
        with get_connection() as conn:
            conn.execute("SELECT 1").fetchone()
        checks["db"] = {"ok": True}
    except Exception as exc:
        overall_ok = False
        checks["db"] = {"ok": False, "error": str(exc)}

    try:
        checks["camera"] = {"ok": True, "status": camera_status()}
    except Exception as exc:
        overall_ok = False
        checks["camera"] = {"ok": False, "error": str(exc)}

    try:
        checks["sync"] = {"ok": True, "status": get_sync_status()}
    except Exception as exc:
        overall_ok = False
        checks["sync"] = {"ok": False, "error": str(exc)}

    try:
        checks["update"] = {"ok": True, "status": get_update_status(refresh_remote=False)}
    except Exception as exc:
        overall_ok = False
        checks["update"] = {"ok": False, "error": str(exc)}

    status_code = 200 if overall_ok else 503
    return JSONResponse(
        status_code=status_code,
        content={"ok": overall_ok, "status": "ok" if overall_ok else "degraded", "checks": checks},
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


@app.get("/api/detection/models")
async def api_detection_models() -> JSONResponse:
    try:
        from .live_detection import get_detection_models_status

        return JSONResponse(content=get_detection_models_status())
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.get("/api/detection/debug")
async def api_detection_debug() -> JSONResponse:
    try:
        from .live_detection import get_tracking_debug

        return JSONResponse(content=get_tracking_debug())
    except Exception as exc:
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.get("/api/face-crops")
async def api_list_face_crops() -> JSONResponse:
    crops_dir = Path(__file__).resolve().parent.parent / "data" / "face_crops"
    if not crops_dir.is_dir():
        return JSONResponse(content={"crops": []})
    files = sorted(crops_dir.glob("*.jpg"), key=lambda f: f.stat().st_mtime, reverse=True)
    crops = []
    for f in files[:200]:
        name = f.stem
        parts = name.rsplit("_", 2)
        pid = parts[0] if len(parts) >= 3 else name
        crops.append({"filename": f.name, "person_id": pid, "size_kb": round(f.stat().st_size / 1024, 1)})
    return JSONResponse(content={"crops": crops, "total": len(files)})


@app.get("/api/face-crop/{filename}")
async def api_get_face_crop(filename: str) -> Response:
    crops_dir = Path(__file__).resolve().parent.parent / "data" / "face_crops"
    path = crops_dir / filename
    if not path.is_file() or not path.name.endswith(".jpg"):
        raise HTTPException(status_code=404, detail="Crop nao encontrado")
    return Response(content=path.read_bytes(), media_type="image/jpeg")


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


@app.get("/api/sync/spreadsheet-info")
async def api_sync_spreadsheet_info(
    spreadsheet: str | None = Query(
        default=None,
        description="Spreadsheet ID ou URL completa da planilha",
    ),
) -> JSONResponse:
    try:
        data = await asyncio.to_thread(inspect_sync_spreadsheet, spreadsheet)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(content=data)


@app.post("/api/sync/auto-setup")
async def api_sync_auto_setup(payload: SyncAutoSetupRequest) -> JSONResponse:
    try:
        data = await asyncio.to_thread(
            auto_setup_sync_from_spreadsheet,
            spreadsheet_input=payload.spreadsheet,
            worksheet_name=payload.worksheet_name,
            enable_sync=payload.enable_sync,
            run_test_sync=payload.run_test_sync,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(content=data)


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


@app.post("/api/personas/reset")
async def api_personas_reset(payload: PersonasResetRequest) -> JSONResponse:
    try:
        result = reset_identified_personas(
            reset_personas_day=payload.reset_personas_day,
            wipe_all_personas=payload.wipe_all_personas,
            delete_day_events=payload.delete_day_events,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return JSONResponse(content=result)


@app.post("/api/data/wipe")
async def api_wipe_all_data() -> JSONResponse:
    try:
        result = wipe_all_test_data()
        return JSONResponse(content=result)
    except Exception as exc:
        logger.exception("Erro em /api/data/wipe")
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.post("/api/person/validate")
async def api_validate_person(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        pid = str(body.get("person_id", "")).strip()
        if not pid:
            return JSONResponse(status_code=400, content={"error": "person_id obrigatorio"})
        gender = body.get("gender")
        age_band = body.get("age_band")
        result = validate_person(pid, gender, age_band)
        return JSONResponse(content=result)
    except Exception as exc:
        logger.exception("Erro em /api/person/validate")
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.post("/api/person/reject")
async def api_reject_person(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        pid = str(body.get("person_id", "")).strip()
        if not pid:
            return JSONResponse(status_code=400, content={"error": "person_id obrigatorio"})
        result = reject_person(pid)
        return JSONResponse(content=result)
    except Exception as exc:
        logger.exception("Erro em /api/person/reject")
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.post("/api/person/merge")
async def api_merge_persons(request: Request) -> JSONResponse:
    try:
        body = await request.json()
        keep = str(body.get("keep_id", "")).strip()
        merge = str(body.get("merge_id", "")).strip()
        if not keep or not merge:
            return JSONResponse(status_code=400, content={"error": "keep_id e merge_id obrigatorios"})
        result = merge_persons(keep, merge)
        return JSONResponse(content=result)
    except Exception as exc:
        logger.exception("Erro em /api/person/merge")
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.get("/api/metrics/live")
async def api_live_metrics(
    culto_id: str | None = Query(default=None, description="Particao; omisso = culto ativo na agenda ou __global__"),
) -> JSONResponse:
    try:
        return JSONResponse(content=get_live_metrics(culto_id=culto_id))
    except Exception as exc:
        logger.exception("Erro em /api/metrics/live")
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.get("/api/people/involvement")
async def api_people_involvement(
    limit: int = Query(default=200, ge=1, le=2000),
    offset: int = Query(default=0, ge=0, le=500_000),
) -> JSONResponse:
    try:
        data = await asyncio.to_thread(get_people_involvement, limit=limit, offset=offset)
        return JSONResponse(content=data)
    except Exception as exc:
        logger.exception("Erro em /api/people/involvement")
        return JSONResponse(status_code=500, content={"error": str(exc)})


@app.get("/api/metrics/charts")
async def api_metrics_charts(
    culto_id: str | None = Query(
        default=None,
        description="Particao do grafico; omisso = culto ativo na agenda ou global (filtro por horario+agenda, nao por coluna em events)",
    ),
    window_minutes: int = Query(default=180, ge=30, le=24 * 60),
    bucket_seconds: int = Query(default=300, ge=300, le=3600),
    center: str | None = Query(
        default=None,
        description="ISO 8601 do instante central; janela fixa 3 h (±90 min). Omisso = ultimos window_minutes ate agora.",
    ),
) -> JSONResponse:
    center_dt: datetime | None = None
    if center is not None and center.strip():
        try:
            center_dt = datetime.fromisoformat(center.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail="Parametro center invalido (use ISO 8601, ex. 2026-04-05T21:30:00-03:00).",
            ) from exc
        if center_dt.tzinfo is None:
            center_dt = center_dt.replace(tzinfo=UTC)
    try:
        return JSONResponse(
            content=get_dashboard_charts(
                culto_id=culto_id,
                window_minutes=window_minutes,
                bucket_seconds=bucket_seconds,
                center=center_dt,
            )
        )
    except Exception as exc:
        logger.exception("Erro em /api/metrics/charts")
        return JSONResponse(status_code=500, content={"error": str(exc)})


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


@app.get("/api/reconciliation/events")
async def api_reconciliation_events() -> JSONResponse:
    events = await asyncio.to_thread(list_events_for_reconciliation_export)
    return JSONResponse(content={"events": events, "count": len(events)})


@app.post("/api/reconciliation/apply")
async def api_reconciliation_apply(
    payload: ReconciliationApplyRequest,
) -> JSONResponse:
    result = await asyncio.to_thread(apply_reconciliation_from_browser, payload)
    if not result.get("ok"):
        raise HTTPException(
            status_code=409,
            detail=str(result.get("message", "Conflito")),
        )
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
    live_detection_enabled: str | None = Form(None),
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
    envolvimento_janela_dias: int = Form(...),
    envolvimento_max_dias_visitante: int = Form(...),
    envolvimento_max_dias_frequentador: int = Form(...),
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
            live_detection_enabled=live_detection_enabled is not None,
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
            envolvimento_janela_dias=envolvimento_janela_dias,
            envolvimento_max_dias_visitante=envolvimento_max_dias_visitante,
            envolvimento_max_dias_frequentador=envolvimento_max_dias_frequentador,
        )
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc
    save_config(payload)
    return RedirectResponse(url="/?toast=config_saved", status_code=303)


@app.api_route("/api/config/involvement", methods=["POST", "PATCH"])
async def api_patch_involvement_rules(body: InvolvementRulesUpdate) -> JSONResponse:
    """Grava apenas janela e limites de envolvimento (sem recarregar o formulario completo).

    Aceita POST e PATCH: alguns proxies ou caches tratam PATCH de forma estranha; POST e o padrao no painel.
    """

    def _run() -> RetentionConfig:
        return update_involvement_rules(
            envolvimento_janela_dias=body.envolvimento_janela_dias,
            envolvimento_max_dias_visitante=body.envolvimento_max_dias_visitante,
            envolvimento_max_dias_frequentador=body.envolvimento_max_dias_frequentador,
        )

    try:
        cfg = await asyncio.to_thread(_run)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    return JSONResponse(
        content={
            "ok": True,
            "envolvimento_janela_dias": cfg.envolvimento_janela_dias,
            "envolvimento_max_dias_visitante": cfg.envolvimento_max_dias_visitante,
            "envolvimento_max_dias_frequentador": cfg.envolvimento_max_dias_frequentador,
        }
    )


@app.post("/cleanup/run")
async def run_cleanup(dry_run: bool = Form(True)) -> RedirectResponse:
    execute_cleanup(dry_run=dry_run)
    slug = "cleanup_dry" if dry_run else "cleanup_real"
    return RedirectResponse(url=f"/?toast={slug}", status_code=303)


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
    return RedirectResponse(url="/?toast=schedule_created", status_code=303)


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
    return RedirectResponse(url="/?toast=schedule_updated", status_code=303)


@app.post("/schedules/{schedule_id}/delete")
async def delete_schedule_form(schedule_id: int) -> RedirectResponse:
    delete_schedule(schedule_id)
    return RedirectResponse(url="/?toast=schedule_deleted", status_code=303)
