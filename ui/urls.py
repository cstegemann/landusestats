from django.urls import path
from . import views

urlpatterns = [
    path("", views.base_data_overview, name="base_data_overview"),
    path("process/", views.process_page, name="process_page"),
    path("process/start/", views.start_process, name="start_process"),
    path("process/status/<str:task_id>/", views.process_status, name="process_status"),
    path("inspect/<int:base_id>/", views.inspect_dataset, name="inspect_dataset"),
    path("inspect/<int:base_id>/search/", views.inspect_search, name="inspect_search"),
    path("inspect/<int:base_id>/boundary/<int:boundary_id>/", views.inspect_boundary, name="inspect_boundary"),
]
