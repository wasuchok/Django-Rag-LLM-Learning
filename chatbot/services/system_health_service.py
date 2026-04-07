from __future__ import annotations

from typing import Any

import requests
from django.conf import settings
from django.utils import timezone

from ..models import SyncCheckpoint
from .sqlserver_service import (
    SQLServerConfigurationError,
    SQLServerDependencyError,
    is_sqlserver_configured,
    test_sqlserver_connection,
)

STATUS_OK = "ok"
STATUS_WARNING = "warning"
STATUS_ERROR = "error"
STATUS_SKIPPED = "skipped"

STATUS_LABELS = {
    STATUS_OK: "ปกติ",
    STATUS_WARNING: "เตือน",
    STATUS_ERROR: "ผิดปกติ",
    STATUS_SKIPPED: "ข้ามการตรวจ",
}


def _status_rank(status: str) -> int:
    if status == STATUS_ERROR:
        return 3
    if status == STATUS_WARNING:
        return 2
    if status == STATUS_OK:
        return 1
    return 0


def _merge_status(current: str, new_status: str) -> str:
    return new_status if _status_rank(new_status) > _status_rank(current) else current


def _build_service_result(
    *,
    name: str,
    label: str,
    status: str,
    message: str,
    details: dict[str, Any] | None = None,
    alerts: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "label": label,
        "status": status,
        "status_label": STATUS_LABELS.get(status, status),
        "message": message,
        "details": details or {},
        "alerts": alerts or [],
    }


def check_ollama_health() -> dict[str, Any]:
    tags_url = f"{settings.OLLAMA_BASE_URL.rstrip('/')}/api/tags"
    timeout_seconds = max(1, getattr(settings, "SYSTEM_HEALTH_OLLAMA_TIMEOUT_SECONDS", 5))

    try:
        response = requests.get(tags_url, timeout=timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        models = payload.get("models") or []
        model_names = [str(item.get("name") or "").strip() for item in models if item.get("name")]

        missing_models: list[str] = []
        if settings.OLLAMA_MODEL and settings.OLLAMA_MODEL not in model_names:
            missing_models.append(settings.OLLAMA_MODEL)
        if settings.OLLAMA_EMBED_MODEL and settings.OLLAMA_EMBED_MODEL not in model_names:
            missing_models.append(settings.OLLAMA_EMBED_MODEL)

        if missing_models:
            return _build_service_result(
                name="ollama",
                label="Ollama",
                status=STATUS_WARNING,
                message="Ollama ตอบกลับได้ แต่ยังไม่พบ model ที่ตั้งค่าไว้ครบ",
                details={
                    "base_url": settings.OLLAMA_BASE_URL,
                    "available_models": model_names[:20],
                    "missing_models": missing_models,
                },
                alerts=[
                    "Ollama ทำงานอยู่ แต่ model บางตัวที่ระบบต้องใช้ยังไม่พบ: "
                    + ", ".join(missing_models)
                ],
            )

        return _build_service_result(
            name="ollama",
            label="Ollama",
            status=STATUS_OK,
            message="Ollama พร้อมใช้งาน",
            details={
                "base_url": settings.OLLAMA_BASE_URL,
                "chat_model": settings.OLLAMA_MODEL,
                "embed_model": settings.OLLAMA_EMBED_MODEL,
                "available_models": model_names[:20],
            },
        )
    except Exception as exc:
        return _build_service_result(
            name="ollama",
            label="Ollama",
            status=STATUS_ERROR,
            message=f"Ollama ไม่พร้อมใช้งาน: {exc}",
            details={"base_url": settings.OLLAMA_BASE_URL},
            alerts=["Ollama ล่มหรือเชื่อมต่อไม่ได้"],
        )


def check_sqlserver_health(*, include_live_check: bool = True) -> dict[str, Any]:
    if not is_sqlserver_configured():
        return _build_service_result(
            name="sqlserver",
            label="SQL Server",
            status=STATUS_WARNING,
            message="ยังไม่ได้ตั้งค่า SQL Server ใน .env",
            alerts=["ยังไม่ได้ตั้งค่า SQL Server สำหรับ sync/analytics"],
        )

    if not include_live_check:
        return _build_service_result(
            name="sqlserver",
            label="SQL Server",
            status=STATUS_SKIPPED,
            message="ยังไม่ได้เช็กการเชื่อมต่อ live ในรอบนี้",
            details={
                "host": settings.SQLSERVER_HOST,
                "database": settings.SQLSERVER_DATABASE,
                "client": settings.SQLSERVER_CLIENT,
            },
        )

    try:
        result = test_sqlserver_connection()
        return _build_service_result(
            name="sqlserver",
            label="SQL Server",
            status=STATUS_OK,
            message="เชื่อมต่อ SQL Server สำเร็จ",
            details={
                "host": settings.SQLSERVER_HOST,
                "database": result.get("database_name") or settings.SQLSERVER_DATABASE,
                "server_name": result.get("server_name"),
                "login_name": result.get("login_name"),
                "checked_at": str(result.get("checked_at") or ""),
                "client": settings.SQLSERVER_CLIENT,
            },
        )
    except (SQLServerConfigurationError, SQLServerDependencyError) as exc:
        return _build_service_result(
            name="sqlserver",
            label="SQL Server",
            status=STATUS_ERROR,
            message=str(exc),
            details={
                "host": settings.SQLSERVER_HOST,
                "database": settings.SQLSERVER_DATABASE,
                "client": settings.SQLSERVER_CLIENT,
            },
            alerts=["SQL Server ตั้งค่าไม่ครบหรือ dependency ยังไม่พร้อม"],
        )
    except Exception as exc:
        return _build_service_result(
            name="sqlserver",
            label="SQL Server",
            status=STATUS_ERROR,
            message=f"เชื่อมต่อ SQL Server ไม่สำเร็จ: {exc}",
            details={
                "host": settings.SQLSERVER_HOST,
                "database": settings.SQLSERVER_DATABASE,
                "client": settings.SQLSERVER_CLIENT,
            },
            alerts=["SQL Server เชื่อมต่อไม่ได้"],
        )


def check_sync_checkpoint_health() -> dict[str, Any]:
    stale_minutes = max(1, getattr(settings, "SYSTEM_HEALTH_CHECKPOINT_STALE_MINUTES", 1440))
    running_stale_minutes = max(
        1,
        getattr(settings, "SYSTEM_HEALTH_CHECKPOINT_RUNNING_STALE_MINUTES", 120),
    )
    now = timezone.now()
    checkpoints = list(SyncCheckpoint.objects.order_by("source_type", "source_name"))

    if not checkpoints:
        return _build_service_result(
            name="sync_checkpoints",
            label="Sync Checkpoint",
            status=STATUS_WARNING,
            message="ยังไม่มี checkpoint ในระบบ",
            alerts=["ยังไม่เคยมีการ sync ที่บันทึก checkpoint"],
        )

    overall_status = STATUS_OK
    alerts: list[str] = []
    items: list[dict[str, Any]] = []

    for checkpoint in checkpoints:
        item_status = STATUS_OK
        item_message = "checkpoint ปกติ"
        age_minutes: float | None = None

        if checkpoint.last_run_finished_at is not None:
            age_minutes = round(
                (now - checkpoint.last_run_finished_at).total_seconds() / 60.0,
                1,
            )

        if checkpoint.last_status == SyncCheckpoint.STATUS_FAILED:
            item_status = STATUS_ERROR
            item_message = "sync ล่าสุดล้มเหลว"
            alerts.append(
                f"{checkpoint.source_name}: sync ล่าสุดล้มเหลว - {checkpoint.last_error or 'unknown error'}"
            )
        elif checkpoint.last_status == SyncCheckpoint.STATUS_RUNNING:
            started_at = checkpoint.last_run_started_at or checkpoint.updated_at
            running_minutes = round((now - started_at).total_seconds() / 60.0, 1)
            if running_minutes > running_stale_minutes:
                item_status = STATUS_ERROR
                item_message = f"checkpoint ค้างในสถานะ running นาน {running_minutes} นาที"
                alerts.append(
                    f"{checkpoint.source_name}: checkpoint ค้างเกิน {running_stale_minutes} นาที"
                )
            else:
                item_status = STATUS_WARNING
                item_message = f"กำลัง sync อยู่ ({running_minutes} นาที)"
        elif checkpoint.last_status == SyncCheckpoint.STATUS_NEVER:
            item_status = STATUS_WARNING
            item_message = "ยังไม่เคย sync สำเร็จ"
            alerts.append(f"{checkpoint.source_name}: ยังไม่เคย sync สำเร็จ")
        elif checkpoint.last_status == SyncCheckpoint.STATUS_SUCCESS and age_minutes is not None:
            if age_minutes > stale_minutes:
                item_status = STATUS_WARNING
                item_message = f"checkpoint เก่า {age_minutes} นาที"
                alerts.append(
                    f"{checkpoint.source_name}: checkpoint เก่าเกิน {stale_minutes} นาที"
                )
            else:
                item_message = f"sync ล่าสุดเมื่อ {age_minutes} นาทีที่แล้ว"

        overall_status = _merge_status(overall_status, item_status)
        items.append(
            {
                "key": checkpoint.key,
                "source_type": checkpoint.source_type,
                "source_name": checkpoint.source_name,
                "status": item_status,
                "status_label": STATUS_LABELS.get(item_status, item_status),
                "message": item_message,
                "cursor_field": checkpoint.cursor_field,
                "cursor_value": checkpoint.cursor_value,
                "last_status": checkpoint.last_status,
                "last_run_started_at": checkpoint.last_run_started_at.isoformat()
                if checkpoint.last_run_started_at
                else None,
                "last_run_finished_at": checkpoint.last_run_finished_at.isoformat()
                if checkpoint.last_run_finished_at
                else None,
                "last_error": checkpoint.last_error,
                "age_minutes": age_minutes,
            }
        )

    message = "checkpoint ทั้งหมดปกติ" if overall_status == STATUS_OK else "พบประเด็นที่ควรตรวจสอบใน checkpoint"
    return _build_service_result(
        name="sync_checkpoints",
        label="Sync Checkpoint",
        status=overall_status,
        message=message,
        details={"items": items},
        alerts=alerts,
    )


def get_system_health_report(*, include_live_checks: bool = True) -> dict[str, Any]:
    services = {
        "ollama": check_ollama_health(),
        "sqlserver": check_sqlserver_health(include_live_check=include_live_checks),
        "sync_checkpoints": check_sync_checkpoint_health(),
    }

    overall_status = STATUS_OK
    alerts: list[str] = []

    for service in services.values():
        overall_status = _merge_status(overall_status, service["status"])
        alerts.extend(service.get("alerts") or [])

    return {
        "status": overall_status,
        "status_label": STATUS_LABELS.get(overall_status, overall_status),
        "generated_at": timezone.now().isoformat(),
        "include_live_checks": include_live_checks,
        "services": services,
        "alerts": alerts,
    }


def build_system_health_message(report: dict[str, Any]) -> str:
    lines = [
        f"สถานะระบบ: {report.get('status_label') or report.get('status')}",
    ]

    alerts = report.get("alerts") or []
    if alerts:
        lines.append("")
        lines.append("การแจ้งเตือน:")
        for alert in alerts[:10]:
            lines.append(f"- {alert}")

    lines.append("")
    lines.append("รายละเอียด:")
    services = report.get("services") or {}
    for service in services.values():
        lines.append(
            f"- {service.get('label')}: {service.get('status_label')} | {service.get('message')}"
        )

        if service.get("name") == "sync_checkpoints":
            items = ((service.get("details") or {}).get("items") or [])[:5]
            for item in items:
                lines.append(
                    f"  - {item.get('source_name')}: {item.get('status_label')} | {item.get('message')}"
                )

    return "\n".join(lines).strip()
