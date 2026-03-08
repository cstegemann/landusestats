from pathlib import Path
from typing import Optional

from django.conf import settings
from django.db import models
from django.utils import timezone


class FileBackedDataset(models.Model):
    """
    Shared metadata for file-backed datasets (GPKG/SQLite/etc).
    Stores only paths + metadata, never the data itself.
    """

    # Path relative to a configured data directory (settings.GEO_DATA_DIR).
    # Example: "base/osm_berlin.gpkg" or "derived/landuse_clean_v3.gpkg"
    relative_path = models.CharField(max_length=500)

    file_format = models.CharField(max_length=50, default="GPKG")  # "GPKG", "SQLite", ...
    srid = models.IntegerField(null=True, blank=True)

    # Optional provenance
    source_label = models.CharField(max_length=200, blank=True, default="")  # e.g. "OSM Geofabrik"
    source_date = models.DateField(null=True, blank=True)

    created_at = models.DateTimeField(default=timezone.now, editable=False)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True
        indexes = [
            models.Index(fields=["name"]),
        ]

    def resolved_path(self) -> Path:
        geo_data_dir = getattr(settings, "GEO_DATA_DIR", None)
        if geo_data_dir is None:
            geo_data_dir = Path(getattr(settings, "BASE_DIR", Path.cwd())) / "data"
        return Path(geo_data_dir) / self.relative_path

    def __str__(self) -> str:
        return self.name


class BaseDataset(FileBackedDataset):
    """
    The raw/bulk dataset (e.g., OSM extract). We use current_version as the
    invalidation mechanism: when we create a new derived dataset, we bump it.
    """
    current_version = models.PositiveIntegerField(default=1)
    name = models.CharField(max_length=200, unique=True)

    class Meta:
        indexes = [
            models.Index(fields=["name"]),
            models.Index(fields=["current_version"]),
        ]


class DerivedDataset(FileBackedDataset):
    """
    A derived dataset generated from a BaseDataset at a specific version.
    Example: dominance-cleaned landuse layer.
    """

    class Kind(models.TextChoices):
        ADMINBOUNDARIES = "ADMINB", "Admin Boundaries"
        LANDUSE = "LANDUSE", "Landuse"
    
    base = models.ForeignKey(
        BaseDataset,
        on_delete=models.PROTECT,
        related_name="derived_versions",
    )
    name = models.CharField(max_length=200, unique=True)
    kind = models.CharField(max_length=10, choices=Kind.choices, default=Kind.ADMINBOUNDARIES)
    version = models.PositiveIntegerField()

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["base", "version"], name="uniq_derived_per_base_version"),
        ]
        indexes = [
            models.Index(fields=["base", "version"]),
        ]

    def __str__(self) -> str:
        return f"{self.name} (derived v{self.version} of {self.base_id})"


class AdminBoundary(models.Model):
    """
    Searchable boundary index used by the UI.

    The idea is to cache only admin_level <= NUTS 5 here (osm lvl 8)

    Boundaries are rebuilt whenever we (re)create derived data, so we track a 
    simple integer version.
    """
    dataset = models.ForeignKey(
        DerivedDataset,
        on_delete=models.CASCADE,
        related_name="admin_boundaries",
    )
    version = models.PositiveIntegerField()
    extraction_algo_version = models.PositiveIntegerField()

    # Source-agnostic identity (works for OSM, BKG, custom, ...)
    source = models.CharField(max_length=50, blank=True, default="")  # e.g. "OSM"
    external_id = models.CharField(max_length=200)  # e.g. "R62422" or "DE:AGS:11000000"

    admin_level = models.PositiveSmallIntegerField(null=True, blank=True)
    name = models.CharField(max_length=255, db_index=True)
    area_m2 = models.FloatField(null=True, blank=True)

    parent = models.ForeignKey(
        "self",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="children",
    )

    # Coarse spatial metadata for UI convenience
    bbox_minx = models.FloatField(null=True, blank=True)
    bbox_miny = models.FloatField(null=True, blank=True)
    bbox_maxx = models.FloatField(null=True, blank=True)
    bbox_maxy = models.FloatField(null=True, blank=True)

    # Optional simplified geometry for instant Leaflet display (store *simplified* only)
    geom_geojson = models.JSONField(null=True, blank=True)

    # Optional metadata/tags
    properties = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(default=timezone.now, editable=False)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["dataset", "version", "source", "external_id"],
                name="uniq_boundary_id_per_base_version",
            ),
        ]
        indexes = [
            models.Index(fields=["dataset", "version"]),
            models.Index(fields=["name"]),
            models.Index(fields=["admin_level"]),
        ]

    def __str__(self) -> str:
        return f"{self.name} (v{self.version}, {self.source}:{self.external_id})"


class StatsCache(models.Model):
    """
    Cached computed stats for (derived_dataset version, boundary).
    When you regenerate derived data, it gets a new DerivedDataset(version),
    so caches naturally become obsolete without extra fields.
    """
    derived = models.ForeignKey(
        DerivedDataset,
        on_delete=models.CASCADE,
        related_name="stats_entries",
    )
    boundary = models.ForeignKey(
        AdminBoundary,
        on_delete=models.PROTECT,
        related_name="stats_entries",
    )

    # Your computed output (whatever shape you like)
    result_json = models.JSONField()

    created_at = models.DateTimeField(default=timezone.now, editable=False)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["derived", "boundary"], name="uniq_stats_per_derived_boundary"),
        ]
        indexes = [
            models.Index(fields=["derived"]),
            models.Index(fields=["boundary"]),
        ]

    def __str__(self) -> str:
        return f"StatsCache(derived={self.derived_id}, boundary={self.boundary_id})"
