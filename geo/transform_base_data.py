#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@author: christian

python version 3.12

'''

# =============================================================================
# Standard Python modules
# =============================================================================
import argparse
import logging
import os
import json 
from typing import Dict, Literal
from pathlib import Path

# =============================================================================
# External Python modules
# =============================================================================
import fiona
import geopandas as gpd
import pandas as pd
from pydantic import BaseModel
import rapidfuzz
from shapely import union_all
from django.utils import timezone
from django.conf import settings
from django.db import transaction

# =============================================================================
# Extension modules
# =============================================================================
from .models import BaseDataset, DerivedDataset, AdminBoundary
from geo.custom_drivers import OSMDriver, AbstractDriver, AVAILABLE_DRIVERS
from geo.boundaries import (
        BOUNDARY_EXTRACTION_ALGO_VERSION, 
        boundaries_gdf_from_base_file,
        compute_parent_ids,
        fix_sub_boundaries
)

# =====================================
# script-wide declarations
# =====================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

LANDUSE_EXTRACTION_ALGO_VERSION = 1

DATA_SUBDIR_BASE = "base"
DATA_SUBDIR_DERIVED = "derived"


def get_or_create_basedb_obj(name: str, path_in: str | Path) -> BaseDataset:
    """
    path_in either has to be a "relative path" as gotten from the db object,
    or just the filename, or the full path. Only one of them _should_ exist. 
    We first check for the "relative path", than just the filename, then the 
    full path.
    """
    geo_data_dir = getattr(settings, "GEO_DATA_DIR", None)
    if geo_data_dir is None:
        geo_data_dir = Path(getattr(settings, "BASE_DIR", Path.cwd())) / "data"
    geo_data_dir = Path(geo_data_dir).expanduser().resolve()
    data_dir = Path(geo_data_dir)/ DATA_SUBDIR_BASE

    # check relative
    path = geo_data_dir / path_in
    if not path.is_file():
        # now check just filename
        path = data_dir / path_in
        if not path.is_file():
            path = Path(path_in).expanduser().resolve()
            if not path.is_file():
                raise ValueError(f"file does not exist: {path}")

    ext = path.suffix.lower()
    ext_to_format = {
        ".gpkg": "GPKG",
        ".sqlite": "SQLite",
        ".db": "SQLite",
    }
    file_format = ext_to_format.get(ext, None)
    if file_format is None:
        raise ValueError(f"unsupported file type by extension: {ext}")

    try:
        relative_path = path.relative_to(geo_data_dir).as_posix()
    except ValueError as e:
        raise ValueError(f"file must be inside GEO_DATA_DIR ({geo_data_dir}): {path}") from e
    
    obj = BaseDataset.objects.filter(name=name).first()
    if obj is None:
        obj = BaseDataset(
            name=name,
            relative_path=relative_path,
            file_format=file_format,
        )
    else:
        obj.relative_path = relative_path
        obj.file_format = file_format

    obj.updated_at = timezone.now()
    obj.save()
    if not obj.resolved_path().is_file():
        raise ValueError(f"something went wrong with the path: {obj.resolved_path}")
    return obj

def fetch_precomputed_admin_boundaries(base_dataset):
    version = int(getattr(base_dataset, "current_version", 0) or 0)
    if version <= 0:
        raise ValueError("base_dataset.current_version must be a positive integer")

    geo_data_dir = getattr(settings, "GEO_DATA_DIR", None)
    if geo_data_dir is None:
        geo_data_dir = Path(getattr(settings, "BASE_DIR", Path.cwd())) / "data"
    relative_path = Path(DATA_SUBDIR_DERIVED) / f"{base_dataset.name}_v{version}_adminboundaries.gpkg"
    derived_path = geo_data_dir / relative_path
    if derived_path.is_file() and derived_path.suffix.lower() == ".gpkg":
        # If derived GPKG exists, read  directly
        return gpd.read_file(derived_path)
    return None

class AdminBoundaryReader: 
    """
    Ensures AdminBoundary rows exist for (base_dataset, base_dataset.current_version, extraction_algo_version).
    Returns a GeoDataFrame either by reading an existing derived GPKG (if present)
    or by building it from:
      - the base dataset file via _boundaries_gdf_from_base_file(...)
    """

    def __init__(self, base_dataset: BaseDataset, driver_str, target_srid) -> None:
        self.target_srid = target_srid
        self.driver = AVAILABLE_DRIVERS[driver_str]()
        self.base_dataset = base_dataset
        self.version = int(getattr(base_dataset, "current_version", 0) or 0)
        if self.version <= 0:
            raise ValueError("base_dataset.current_version must be a positive integer")

        geo_data_dir = getattr(settings, "GEO_DATA_DIR", None)
        if geo_data_dir is None:
            geo_data_dir = Path(getattr(settings, "BASE_DIR", Path.cwd())) / "data"
        self.relative_path = Path(DATA_SUBDIR_DERIVED) / f"{base_dataset.name}_v{self.version}_adminboundaries.gpkg"
        self.derived_path = geo_data_dir / self.relative_path
        if self.derived_path.is_file() and self.derived_path.suffix.lower() == ".gpkg":
            # If derived GPKG exists, read  directly
            raise RuntimeError("file already exists, please check code!")

        # Otherwise, build from base file 
        self.source_default = getattr(self.driver, "SOURCE", "") or ""

    def read_file(self):
        file_path = self.base_dataset.resolved_path().expanduser().resolve()
        gdf = boundaries_gdf_from_base_file(file_path, self.driver, self.target_srid)
        if gdf is None or len(gdf) == 0:
            raise ValueError("empty boundaries gdf")
        return gdf

    def add_parent_ids(self, gdf):
        return compute_parent_ids(gdf, id_col = self.driver.PK_COLUMN)
   
    def fix_sub_boundaries(self, gdf):
        return fix_sub_boundaries(gdf, id_col=self.driver.PK_COLUMN)

    def simplify_and_save(self, gdf, base_dataset, simplify_tolerance = 10):
        
        # Store simplified geojson (prefer simplified for UI)
        geom_for_store = gdf.geometry
        if simplify_tolerance > 0:
            try:
                geom_for_store = geom_for_store.simplify(simplify_tolerance, preserve_topology=True)
            except Exception:
                geom_for_store = gdf.geometry

        gdf["_geom_geojson_store"] = geom_for_store.apply(lambda g: g.__geo_interface__ if g is not None else None)

        # as used in boundaries.py
        parent_key = "parent_id"

        # Persist cache
        with transaction.atomic():
            derived_dataset = DerivedDataset.objects.create(
                base = base_dataset,
                name = base_dataset.name,
                kind = DerivedDataset.Kind.ADMINBOUNDARIES,
                version=self.version,
                srid = self.target_srid,
                relative_path=self.relative_path,
            )

            # Clear any partial leftovers for this version/algo (safe idempotency)
            AdminBoundary.objects.filter(
                dataset=derived_dataset,
                version=self.version,
                extraction_algo_version=BOUNDARY_EXTRACTION_ALGO_VERSION,
            ).delete()

            # First pass: create boundaries without parent
            objs = []
            for _, row in gdf.iterrows():
                props = {}
                admin_level = (int(row.get("admin_level")) if pd.notna(row.get("admin_level")) else None)
                if admin_level > 8:
                    continue
                # we gotta defer this...
                #if "properties" in gdf.columns and isinstance(row.get("properties"), dict):
                #    props = row.get("properties") or {}
                #else:
                #    # If driver wants properties from selected columns
                #    keep_cols = getattr(driver, "properties_columns", None)
                #    if keep_cols:
                #        props = {c: row.get(c) for c in keep_cols if c in gdf.columns}

                objs.append(
                    AdminBoundary(
                        dataset=derived_dataset,
                        version=self.version,
                        extraction_algo_version=BOUNDARY_EXTRACTION_ALGO_VERSION,
                        source=row.get("source") or self.source_default,
                        external_id=str(row.get(self.driver.EXTERNAL_ID)),
                        admin_level=admin_level,
                        name=str(row.get("name") or ""),
                        area_m2=(float(row.get("area_m2")) if pd.notna(row.get("area_m2")) else None),
                        bbox_minx=(float(row.get("bbox_minx")) if pd.notna(row.get("bbox_minx")) else None),
                        bbox_miny=(float(row.get("bbox_miny")) if pd.notna(row.get("bbox_miny")) else None),
                        bbox_maxx=(float(row.get("bbox_maxx")) if pd.notna(row.get("bbox_maxx")) else None),
                        bbox_maxy=(float(row.get("bbox_maxy")) if pd.notna(row.get("bbox_maxy")) else None),
                        geom_geojson=row.get("_geom_geojson_store"),
                        properties=props or {},
                    )
                )

            created = AdminBoundary.objects.bulk_create(objs, batch_size=1000)

            # Second pass: set parent if available
            if parent_key:
                by_external = {o.external_id: o for o in created}
                updates = []
                for obj, (_, row) in zip(created, gdf.iterrows()):
                    pext = row.get(parent_key)
                    if pd.isna(pext) or pext in (None, "", 0):
                        continue
                    parent = by_external.get(str(pext))
                    if parent and obj.parent_id != parent.id:
                        obj.parent = parent
                        updates.append(obj)
                if updates:
                    AdminBoundary.objects.bulk_update(updates, ["parent"], batch_size=1000)
            gdf = gdf.drop(columns=["_geom_geojson_store"], errors="ignore")
            gdf.to_file(self.derived_path, driver="GPKG")

        # Return the computed gdf 
        return gdf


