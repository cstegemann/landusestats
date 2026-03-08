
# =============================================================================
# Standard Python modules
# =============================================================================
import time
import logging

# =============================================================================
# External Python modules
# =============================================================================
from celery import shared_task

# =============================================================================
# Extension modules
# =============================================================================
from geo.transform_base_data import get_or_create_basedb_obj, AdminBoundaryReader, fetch_precomputed_admin_boundaries

# =====================================
# script-wide declarations
# =====================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

@shared_task(bind=True)
def run_gpkg_init(self, name, base_file_path, driver_str, source_label="", source_date=None):
    total_steps = 6
    start_time = time.time()
    def report(step, message):
        # step is 1..total_steps
        rts = round(time.time()-start_time, 2)
        logger.debug(f"PROGRESS {rts} - {step}/{total_steps}: {message}")
        self.update_state(
            state="PROGRESS",
            meta={
                "current": step,
                "total": total_steps,
                "message": message,
            },
        )
    report(1, "check base")
    base_db_obj = get_or_create_basedb_obj(
        name=name,
        path_in=base_file_path,
        source_label=source_label or "",
        source_date=source_date,
    )
    report(2, "check cached")
    admin_gdf = fetch_precomputed_admin_boundaries(base_db_obj)
    if admin_gdf is None:
        abr = AdminBoundaryReader(base_db_obj, driver_str, 25832)
        report(3, "read admin boundaries")
        admin_gdf = abr.read_file()
        report(4, "add parents")
        admin_gdf = abr.add_parent_ids(admin_gdf)
        report(5, "fixing boundaries")
        admin_gdf = abr.fix_sub_boundaries(admin_gdf)
        report(6, "simplify and save")
        admin_gdf = abr.simplify_and_save(admin_gdf, base_db_obj)

    # Celery will set state=SUCCESS automatically when returning
    return {
        "current": total_steps,
        "total": total_steps,
        "message": "Done.",
        "result": {"whatever": "you want"},
    }
