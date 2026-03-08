from django.urls import path
from . import views

urlpatterns = [
    path("process/", views.process_page, name="process_page"),
    path("process/start/", views.start_process, name="start_process"),
    path("process/status/<str:task_id>/", views.process_status, name="process_status"),
]
