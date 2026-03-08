
from django.http import HttpResponse
from django.shortcuts import render
from django.urls import reverse
from celery.result import AsyncResult

from geo.custom_drivers import OSMDriver
from geo.tasks import run_gpkg_init

def process_page(request):
    # initial page with button + empty status area
    return render(request, "ui/process_page.html")

def start_process(request):
    if request.method != "POST":
        return HttpResponse(status=405)

    # Start the celery task
    async_result = run_gpkg_init.delay(name="niedersachsen_osm", base_file_path="data/base/niedersachsen_osm.gpkg", driver_str="osm")
    task_id = async_result.id

    # Return a partial that includes a polling element
    return render(request, "ui/_process_started.html", {"task_id": task_id})

def process_status(request, task_id):
    res = AsyncResult(task_id)

    # Normalize info/meta
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
    }
    return render(request, "ui/_process_status.html", context)
