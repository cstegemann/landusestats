from django.contrib import admin

# Register your models here.
from .models import *

@admin.register(BaseDataset)
class BaseDatasetAdmin(admin.ModelAdmin):
    list_display = ("id", "name" ,"srid")
    search_fields = ("id", "name")


@admin.register(DerivedDataset)
class DerivedDatasetAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "srid")
    search_fields = ("id", "name")


@admin.register(AdminBoundary)
class AdminBoundaryAdmin(admin.ModelAdmin):
    list_display = ("id",)
    search_fields = ("id",)
