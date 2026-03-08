from pathlib import Path

from celery.result import AsyncResult
from django.conf import settings
from django.db.models import Min, Q
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, render
from django.urls import reverse

from geo.models import AdminBoundary, BaseDataset, DerivedDataset


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
            inspect_url = None
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
                if derived_in_sync:
                    inspect_url = reverse("inspect_dataset", kwargs={"base_id": base_obj.id})

            rows.append(
                {
                    "relative_path": relative_path,
                    "base": base_obj,
                    "current_version": current_version,
                    "derived_db_exists": derived_db_exists,
                    "derived_file_exists": derived_file_exists,
                    "derived_in_sync": derived_in_sync,
                    "needs_processing": base_obj is None or not derived_in_sync,
                    "inspect_url": inspect_url,
                }
            )

    return rows


def _get_current_admin_dataset(base_obj: BaseDataset):
    return (
        DerivedDataset.objects.filter(
            base=base_obj,
            version=base_obj.current_version,
            kind=DerivedDataset.Kind.ADMINBOUNDARIES,
        )
        .order_by("id")
        .first()
    )


def _serialize_boundary(boundary: AdminBoundary):
    return {
        "id": boundary.id,
        "name": boundary.name,
        "admin_level": boundary.admin_level,
        "source": boundary.source,
        "external_id": boundary.external_id,
        "area_m2": boundary.area_m2,
        "bbox": {
            "minx": boundary.bbox_minx,
            "miny": boundary.bbox_miny,
            "maxx": boundary.bbox_maxx,
            "maxy": boundary.bbox_maxy,
        },
        "properties": boundary.properties,
        "geom_geojson": boundary.geom_geojson,
    }


def _lowest_level_overlay_boundaries(dataset: DerivedDataset):
    level = (
        AdminBoundary.objects.filter(dataset=dataset, version=dataset.version)
        .exclude(admin_level__isnull=True)
        .aggregate(lowest=Min("admin_level"))
        .get("lowest")
    )

    if level is None:
        return [], None

    boundaries = AdminBoundary.objects.filter(
        dataset=dataset,
        version=dataset.version,
        admin_level=level,
    ).order_by("name")[:150]

    return [_serialize_boundary(boundary) for boundary in boundaries], level


def base_data_overview(request):
    return render(request, "ui/base_data_overview.html", {"rows": _discover_base_rows()})


def inspect_dataset(request, base_id):
    base_obj = get_object_or_404(BaseDataset, pk=base_id)
    dataset = _get_current_admin_dataset(base_obj)

    if dataset is None:
        return HttpResponse("No in-sync admin boundary dataset for this base dataset.", status=404)

    lowest_boundaries, lowest_level = _lowest_level_overlay_boundaries(dataset)

    context = {
        "base_obj": base_obj,
        "dataset": dataset,
        "lowest_admin_level": lowest_level,
        "lowest_boundaries": lowest_boundaries,
        "overview_url": reverse("base_data_overview"),
    }
    return render(request, "ui/inspect_dataset.html", context)


def inspect_search(request, base_id):
    base_obj = get_object_or_404(BaseDataset, pk=base_id)
    dataset = _get_current_admin_dataset(base_obj)
    if dataset is None:
        return HttpResponse("", status=404)

    query = (request.GET.get("q") or "").strip()
    if len(query) < 2:
        return render(request, "ui/_boundary_search_results.html", {"results": [], "query": query})

    results = (
        AdminBoundary.objects.filter(dataset=dataset, version=dataset.version, admin_level__lte=8)
        .filter(Q(name__icontains=query) | Q(external_id__icontains=query))
        .order_by("admin_level", "name")[:20]
    )

    context = {
        "results": results,
        "query": query,
    }
    return render(request, "ui/_boundary_search_results.html", context)


def inspect_boundary(request, base_id, boundary_id):
    base_obj = get_object_or_404(BaseDataset, pk=base_id)
    dataset = _get_current_admin_dataset(base_obj)
    if dataset is None:
        return HttpResponse("", status=404)

    boundary = get_object_or_404(
        AdminBoundary,
        pk=boundary_id,
        dataset=dataset,
        version=dataset.version,
        admin_level__lte=8,
    )

    return render(request, "ui/_selected_boundary.html", {"boundary": boundary, "boundary_json": _serialize_boundary(boundary)})


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
