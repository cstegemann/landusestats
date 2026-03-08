# =============================================================================
# Standard Python modules
# =============================================================================
import argparse
import logging
import time
import os
from pathlib import Path

# =============================================================================
# External Python modules
# =============================================================================
import geopandas as gpd
import pandas as pd
from shapely import union_all

# =============================================================================
# Extension modules
# =============================================================================
from geo.custom_drivers import OSMDriver, AbstractDriver

# =====================================
# script-wide declarations
# =====================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

BOUNDARY_EXTRACTION_ALGO_VERSION = 1


def boundaries_gdf_from_base_file(file_path, driver, target_srid):
    gdf = driver.get_all_admin_boundaries(file_path)
    gdf = gdf.set_index(driver.PK_COLUMN, drop=False)
    gdf.geometry.make_valid()
    gdf = gdf.to_crs(target_srid)
    gdf["area_m2"] = gdf.area
    # since we want to compare them later, we need to cast to numeric!
    gdf["admin_level"] = pd.to_numeric(gdf["admin_level"], errors="coerce")
    # centroid/bbox from geometry (in whatever CRS it is); store numeric values
    try:
        bounds = gdf.geometry.bounds
        gdf["bbox_minx"] = bounds["minx"]
        gdf["bbox_miny"] = bounds["miny"]
        gdf["bbox_maxx"] = bounds["maxx"]
        gdf["bbox_maxy"] = bounds["maxy"]
    except Exception:
        gdf["bbox_minx"] = None
        gdf["bbox_miny"] = None
        gdf["bbox_maxx"] = None
        gdf["bbox_maxy"] = None
    return gdf


def compute_parent_ids(
    gdf,
    id_col="id",
    parent_id_col="parent_id",
):
    gdf = gdf.copy()

    left = gdf[[id_col, "admin_level", "geometry"]].rename(
        columns={id_col: "child_id", "admin_level": "child_lvl"}
    )
    right = gdf[[id_col, "admin_level", "area_m2", "geometry"]].rename(
        columns={id_col: "par_id", "admin_level": "par_lvl", "area_m2": "par_area"}
    )

    # All within/covered_by pairs (uses spatial index internally)
    pairs = gpd.sjoin(left, right, how="left", predicate="covered_by")

    # Keep only *coarser* parents
    pairs = pairs[pairs["par_lvl"] < pairs["child_lvl"]].copy()

    # Choose closest parent:
    # 1) highest par_lvl below child_lvl
    # 2) if multiple, smallest par_area
    pairs.sort_values(
        by=["child_id", "par_lvl", "par_area"],
        ascending=[True, False, True],
        inplace=True,
    )
    best = pairs.drop_duplicates(subset=["child_id"], keep="first")

    # Attach back to gdf
    parent_map = best.set_index("child_id")["par_id"]
    gdf[parent_id_col] = gdf[id_col].map(parent_map)
    return gdf

def fix_sub_boundaries(
    gdf: gpd.GeoDataFrame,
    id_col: str = "id",
    parent_col: str = "parent_id",
    level_col: str = "admin_level",
    area_col: str = "area_m2",
    geom_col: str = "geometry",
    min_level: int = 4,
    max_level: int = 11,
    # if children cover parent within this tolerance, skip creating remainder
    rel_area_tol: float = 1e-6,
    abs_area_tol_m2: float = 1.0,
    remainder_suffix: str = "__remaining",
) -> gpd.GeoDataFrame:

    if gdf.empty:
        return gdf.copy()

    req = {id_col, parent_col, level_col, area_col, geom_col}
    missing = req - set(gdf.columns)
    if missing:
        raise ValueError(f"gdf missing required columns: {sorted(missing)}")

    out = gdf.copy()

    # Fast lookups for parent geometry/attrs
    parents = out.set_index(id_col, drop=False)
    if parents.index.has_duplicates:
        raise ValueError(f"Duplicate values found in {id_col}; it must be unique.")

    # Group direct children by parent
    children = out[out[parent_col].notna()].copy()
    if children.empty:
        return out

    # Ensure parent ids align with parent index dtype
    children[parent_col] = children[parent_col].astype(parents.index.dtype)

    new_rows = []

    # Iterate parents that actually have children
    for pid, grp in children.groupby(parent_col, sort=False):
        if pid not in parents.index:
            continue

        parent_row = parents.loc[pid]
        parent_geom = parent_row[geom_col]
        parent_area = float(parent_row[area_col]) if pd.notna(parent_row[area_col]) else float(parent_geom.area)

        # Union children geometries once
        child_geoms = grp[geom_col].values
        if len(child_geoms) == 0:
            continue

        child_union = union_all(child_geoms)

        if child_union.is_empty:
            continue

        child_union_area = float(child_union.area)

        # Area-based skip (coarse but fast); if close enough, assume no meaningful remainder
        gap = parent_area - child_union_area
        tol = max(abs_area_tol_m2, rel_area_tol * parent_area)
        if gap <= tol:
            continue

        remainder = parent_geom.difference(child_union)
        if remainder.is_empty:
            continue

        # Determine remainder admin level
        parent_lvl = int(parent_row[level_col])
        rem_lvl = parent_lvl + 1

        rem_lvl = max(min_level, min(max_level, rem_lvl))

        # Build new feature row (carry parent attrs, but new id + level + geom + area)
        new_id = f"{parent_row[id_col]}{remainder_suffix}"
        new_row = parent_row.copy()
        new_row[id_col] = new_id
        new_row["name"] = parent_row["name"] + remainder_suffix
        new_row[parent_col] = parent_row[id_col]
        new_row[level_col] = rem_lvl
        new_row[geom_col] = remainder
        new_row[area_col] = float(remainder.area)

        new_rows.append(new_row)

    if not new_rows:
        return out

    logger.debug(f"adding {len(new_rows)} remainder areas")
    add = gpd.GeoDataFrame(new_rows, columns=out.columns, crs=out.crs)
    return pd.concat([out, add], ignore_index=True)

if __name__=="__main__":
    """ run this script using -m notation: python -m geo.boundaries INPUTFILE
    so that imports work correctly
    """
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    logger.warning("THIS IS ONLY FOR TESTING")
    tmp_out = Path("TEMP_CHECK_OUTPUT")
    os.mkdir(tmp_out)
    parser = argparse.ArgumentParser()
    parser.add_argument('inputfile', metavar='input', type=str, 
                        help='set input')
    args = parser.parse_args()
    logger.debug(f"extract boundaries from base file, start...")
    start_ts = time.time()
    driver = OSMDriver()
    gdf = boundaries_gdf_from_base_file(args.inputfile, driver, 25832)
    gdf.to_file(tmp_out / "boundaries.gpkg", driver="GPKG", index=False)
    logger.debug(f"computing parents...")
    gdf = compute_parent_ids(gdf, id_col=driver.PK_COLUMN)
    gdf.to_file(tmp_out / "with_parents.gpkg", driver="GPKG", index=False)
    logger.debug(f"fixing sub boundaries...")
    gdf = fix_sub_boundaries(gdf, id_col=driver.PK_COLUMN)
    gdf.to_file(tmp_out / "fixed.gpkg", driver="GPKG", index=False)
    time_cost = time.time() - start_ts
    logger.debug(f"extracted boundaries from base file, took {time_cost}")

