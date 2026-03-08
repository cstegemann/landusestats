import geopandas as gpd

def clean_and_snap(gdf: gpd.GeoDataFrame, grid: float = 0.1) -> gpd.GeoDataFrame:
    """
    - drops empty/null geometries
    - makes geometries valid (best-effort)
    - snaps to a precision grid (reduces precision to stabilize overlays)
    - drops empties again
    """
    gdf = gdf.copy()
    gdf = gdf[gdf.geometry.notna()]
    gdf = gdf[~gdf.geometry.is_empty]

    # 1) make valid
    try:
        import shapely
        gdf.geometry = shapely.make_valid(gdf.geometry.values)
    except Exception:
        # fallback: classic trick; not perfect but often helps
        gdf.geometry = gdf.geometry.buffer(0)

    gdf = gdf[gdf.geometry.notna()]
    gdf = gdf[~gdf.geometry.is_empty]

    # 2) snap / reduce precision
    try:
        import shapely
        # mode="valid_output" helps keep results valid if supported by your GEOS version
        try:
            gdf.geometry = shapely.set_precision(gdf.geometry.values, grid, mode="valid_output")
        except TypeError:
            gdf.geometry = shapely.set_precision(gdf.geometry.values, grid)
    except Exception:
        # If set_precision isn't available, you can skip snapping or implement a slower round-to-grid.
        pass

    # 3) optional: make valid again (helps after snapping)
    try:
        import shapely
        gdf.geometry = shapely.make_valid(gdf.geometry.values)
    except Exception:
        gdf.geometry = gdf.geometry.buffer(0)

    gdf = gdf[gdf.geometry.notna()]
    gdf = gdf[~gdf.geometry.is_empty]
    return gdf
