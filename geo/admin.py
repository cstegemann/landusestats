from django.contrib import admin

from .models import AdminBoundary, BaseDataset, DerivedDataset


@admin.register(BaseDataset)
class BaseDatasetAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "relative_path", "srid", "source_label", "source_date")
    search_fields = ("id", "name", "relative_path", "source_label")


@admin.register(DerivedDataset)
class DerivedDatasetAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "relative_path", "srid", "source_label", "source_date")
    search_fields = ("id", "name", "relative_path", "source_label")


@admin.register(AdminBoundary)
class AdminBoundaryAdmin(admin.ModelAdmin):
    list_display = ("id",)
    search_fields = ("id",)
