#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@author: christian

python version 3.12

'''

# =============================================================================
# Standard Python modules
# =============================================================================
import logging
from typing import Dict, Literal, override
from collections import defaultdict, Counter

# =============================================================================
# External Python modules
# =============================================================================
import numpy as np
import geopandas as gpd
import sqlite3

# =============================================================================
# Extension modules
# =============================================================================

# =====================================
# script-wide declarations
# =====================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def has_value(v) -> bool:
    """ helper to avoid different kinds of empty fields
    """
    if v is None:
        return False
    # pandas NA / numpy nan
    try:
        if np.isnan(v):
            return False
    except Exception:
        pass
    if isinstance(v, str) and v.strip().lower() in ["none", "null", ""]:
        return False
    return True


def within_boundary_rtree(gpkg_path, table, geom_col, pk_col, boundary_id, spatialite_path="mod_spatialite"):
    """
    gpkg_path: path to the .gpkg
    table: the GeoPackage feature table name (often equals the layer name in Fiona/OGR, but not always)
    geom_col: geometry column name (often geom, sometimes geometry)
    pk_col: the primary key column in that table (often fid, id, etc.)
    boundary_id: the pk value of the boundary feature row
    spatialite_path: the name/path of the SpatiaLite extension shared library (depends on OS)

    Returns: list[pk, name]
    """
    rtree = f'rtree_{table}_{geom_col}'  # common GPKG naming

    con = sqlite3.connect(gpkg_path)
    con.enable_load_extension(True)
    con.load_extension(spatialite_path)
    con.enable_load_extension(False)

    con.execute("SELECT EnableGpkgAmphibiousMode()")

    # WHERE {pk_col} = ? -> parameterized to let sqlite handle the type/ escaping etc
    sql = f"""
    WITH b AS (
      SELECT
        {geom_col} AS g,
        MbrMinX({geom_col}) AS minx,
        MbrMaxX({geom_col}) AS maxx,
        MbrMinY({geom_col}) AS miny,
        MbrMaxY({geom_col}) AS maxy
      FROM "{table}"
      WHERE {pk_col} = ?
    )
    SELECT t.{pk_col}, t.name
    FROM "{table}" t
    JOIN "{rtree}" r
      ON r.id = t.{pk_col}
    JOIN b
    WHERE r.minx <= b.maxx AND r.maxx >= b.minx
      AND r.miny <= b.maxy AND r.maxy >= b.miny
      AND ST_CoveredBy(t.{geom_col}, b.g) = 1
    """
    rows = con.execute(sql, (boundary_id,)).fetchall()
    con.close()
    return rows



class AbstractDriver:
    def get_all_admin_boundaries(self, file_path) -> gpd.GeoDataFrame:
        """load named admin boundaries from base file into a geopandas df. 
        """
        raise NotImplementedError("overwrite in implementing class")

    def is_admin_level_subcity(self, gdf_row):
        raise NotImplementedError("overwrite in implementing class")

### OSM driver plus some look ups
OSM_ECON_LANDUSE = {"industrial", "commercial", "retail", "construction", "farmyard"}
OSM_GREEN_LANDUSE = {"forest", "grass", "meadow", "recreation_ground", "village_green", "allotments", "farmland"}
OSM_GREEN_NATURAL = {"wood", "grassland", "heath", "scrub", "wetland"}
# leisure/tourism is usually "special_use", but parks are "green" instead.
OSM_GREEN_LEISURE = {"park", "garden", "nature_reserve"}

class OSMDriver(AbstractDriver):
    PK_COLUMN="osm_id"
    GEOM_COLUMN="geometry"
    EXTERNAL_ID="osm_id"
    SOURCE="OSM"

    @override
    def get_all_admin_boundaries(self, file_path):
        gdf = gpd.read_file(
                file_path,
                layer = "multipolygons",
                where = f'"boundary"="administrative" AND "name" IS NOT NULL AND "osm_id" IS NOT NULL'
                )
        return gdf

    def set_subcity_admin_level(self, admin_boundaries_gdf, city_gdf):
        self.base_admin_level = city_gdf.iloc[0]['admin_level']
        valid = admin_boundaries_gdf[
            admin_boundaries_gdf["admin_level"] > self.base_admin_level
        ]
        self.sub_admin_level = None
        if not valid.empty:
            c = Counter(valid.admin_level)
            default = valid["admin_level"].min()
            q = input(f"sub level counts: {c}, which should we use? [default:{default}]: ")
            if q == '':
                q = default
            self.sub_admin_level = int(q)

        logging.debug(f"setting sub admin level to {self.sub_admin_level}")
        if self.sub_admin_level is None:
            raise ValueError("cant determine sub_admin_level")

    @override
    def is_admin_level_subcity(self, gdf):
        # super ugly hack for now with string comparison, whatever - osm data 
        # is clean I hope...
        return gdf['admin_level'].map(lambda x: str(x) == str(self.sub_admin_level))

    @override
    def not_admin_boundary(self, gdf):
        return gdf["boundary"].map(lambda x: x!= "administrative")
   
    def get_use_priority(self):
        #residential fairly low because the areas in my test set are very big
        return [
                "special_use", 
                "economic", 
                "water", 
                "green", 
                "residential", 
                "building_only",
                "null"
        ]

    def classify_use(self, row) -> str:
        amenity = row.get("amenity", None)
        leisure = row.get("leisure", None)
        tourism = row.get("tourism", None)
        public_transport = row.get("public_transport", None)
        landuse = row.get("landuse", None)
        natural = row.get("natural", None)
        building = row.get("building", None)
        other_tags = row.get("other_tags", None)

        # special_use 
        if has_value(amenity) or has_value(public_transport) or has_value(tourism):
            return "special_use"

        if has_value(landuse) and landuse == "cemetery":
            return "special_use"

        if has_value(leisure):
            # park/garden/nature_reserve is "green", the rest special_use
            if str(leisure) in OSM_GREEN_LEISURE:
                return "green"
            return "special_use"

        # economic
        if has_value(landuse) and str(landuse) in OSM_ECON_LANDUSE:
            return "economic"

        # residential
        if has_value(landuse) and str(landuse) == "residential":
            return "residential"

        # green
        if has_value(natural) and str(natural) in OSM_GREEN_NATURAL:
            return "green"
        if has_value(landuse) and str(landuse) in OSM_GREEN_LANDUSE:
            return "green"

        # building_only
        if has_value(building):
            return "building_only"
       
        # water
        if (
                (has_value(other_tags) and "water" in other_tags) or 
                (has_value(natural) and natural=="water")
        ):
            return "water"

        # null 
        # in my test set, these where mostly highways, memorials, bare rocks
        # and benches
        #if row["boundary"]!= "administrative":
        #    print(row)
        return "null"

    def within_boundary_rtree(self, path_in, city_id, layer="multipolygons") -> List[int, str]:
        return within_boundary_rtree(
                gpkg_path=path_in, 
                table=layer, 
                geom_col=self.GEOM_COLUMN, 
                pk_col=self.PK_COLUMN, 
                boundary_id=city_id,
        )


AVAILABLE_DRIVERS={
        "osm":OSMDriver,
        }
