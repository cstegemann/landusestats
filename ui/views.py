from pathlib import Path

from celery.result import AsyncResult
from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import render
from django.urls import reverse

from geo.models import BaseDataset, DerivedDataset




def _available_drivers():
    from geo.custom_drivers import AVAILABLE_DRIVERS

    return AVAILABLE_DRIVERS

def _resolve_geo_data_dir() -> Path:
    geo_data_dir = getattr(settings, "GEO_DATA_DIR", None)
    if geo_data_dir is None:
        geo_data_dir = Path(getattr(settings, "BASE_DIR", Path.cwd())) / "data"
    return Path(geo_data_dir).expanduser().resolve()


def _discover_base_rows():
    geo_data_dir = _resolve_geo_data_dir()
    base_dir = geo_data_dir / "base"

    base_qs = BaseDataset.objects.all()
    base_by_rel = {obj.relative_path: obj for obj in base_qs}

    rows = []
    if base_dir.is_dir():
        for path in sorted(base_dir.glob("*")):
            if not path.is_file() or path.suffix.lower() not in {".gpkg", ".sqlite", ".db"}:
                continue
            relative_path = path.relative_to(geo_data_dir).as_posix()
            base_obj = base_by_rel.get(relative_path)
            derived_db_exists = False
            derived_file_exists = False
            derived_in_sync = False
            current_version = None
            if base_obj is not None:
                current_version = base_obj.current_version
                expected_rel = f"derived/{base_obj.name}_v{base_obj.current_version}_adminboundaries.gpkg"
                derived_db_exists = DerivedDataset.objects.filter(
                    base=base_obj,
                    version=base_obj.current_version,
                    kind=DerivedDataset.Kind.ADMINBOUNDARIES,
                    relative_path=expected_rel,
                ).exists()
                derived_file_exists = (geo_data_dir / expected_rel).is_file()
                derived_in_sync = derived_db_exists and derived_file_exists

            rows.append(
                {
                    "relative_path": relative_path,
                    "base": base_obj,
                    "current_version": current_version,
                    "derived_db_exists": derived_db_exists,
                    "derived_file_exists": derived_file_exists,
                    "derived_in_sync": derived_in_sync,
                    "needs_processing": base_obj is None or not derived_in_sync,
                }
            )

    return rows


def base_data_overview(request):
    return render(request, "ui/base_data_overview.html", {"rows": _discover_base_rows()})


def process_page(request):
    relative_path = request.GET.get("relative_path", "")
    base_obj = BaseDataset.objects.filter(relative_path=relative_path).first() if relative_path else None
    context = {
        "relative_path": relative_path,
        "drivers": sorted(_available_drivers().keys()),
        "base_obj": base_obj,
        "overview_url": reverse("base_data_overview"),
    }
    return render(request, "ui/process_page.html", context)


def start_process(request):
    if request.method != "POST":
        return HttpResponse(status=405)

    relative_path = (request.POST.get("relative_path") or "").strip()
    name = (request.POST.get("name") or "").strip()
    driver_str = (request.POST.get("driver_str") or "").strip()
    source_label = (request.POST.get("source_label") or "").strip()
    source_date = (request.POST.get("source_date") or "").strip()

    errors = []
    if not relative_path:
        errors.append("A base file path is required.")
    if not name:
        errors.append("A dataset name is required.")
    if driver_str not in _available_drivers():
        errors.append("Please select a valid driver.")

    if errors:
        return render(
            request,
            "ui/process_page.html",
            {
                "relative_path": relative_path,
                "drivers": sorted(_available_drivers().keys()),
                "base_obj": BaseDataset.objects.filter(relative_path=relative_path).first(),
                "overview_url": reverse("base_data_overview"),
                "errors": errors,
                "form_values": {
                    "name": name,
                    "driver_str": driver_str,
                    "source_label": source_label,
                    "source_date": source_date,
                },
            },
            status=400,
        )

    from geo.tasks import run_gpkg_init

    async_result = run_gpkg_init.delay(
        name=name,
        base_file_path=relative_path,
        driver_str=driver_str,
        source_label=source_label,
        source_date=source_date or None,
    )
    task_id = async_result.id

    return render(
        request,
        "ui/_process_started.html",
        {"task_id": task_id, "overview_url": reverse("base_data_overview")},
    )


def process_status(request, task_id):
    res = AsyncResult(task_id)

    info = res.info if isinstance(res.info, dict) else {}
    state = res.state

    context = {
        "task_id": task_id,
        "state": state,
        "current": info.get("current", 0),
        "total": info.get("total", 1),
        "message": info.get("message", ""),
        "done": state in ("SUCCESS", "FAILURE", "REVOKED"),
        "error": str(res.info) if state == "FAILURE" else "",
        "overview_url": reverse("base_data_overview"),
    }
    return render(request, "ui/_process_status.html", context)
