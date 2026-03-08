

def add_use_classification(self):
    logger.debug("adding use classification")
    mdc = self.cache_meta.cities.get(self.name, None)
    if mdc is None:
        raise ValueError("frfr, why is there no cache at his point?")
    self.all_mp_within["georef_use_type"] = self.all_mp_within.apply(
            self.processor.classify_use, 
            axis=1
        )
    logger.debug(self.all_mp_within["georef_use_type"].value_counts(dropna=False))
    self.all_mp_within.to_file(mdc.filename_all_mp_within)
    mdc.state = "use_type_added"
    self._dump_meta()

def compute_statistics(self):
    prio_list = self.processor.get_use_priority()
    use_type_unions = dict()
    self.all_mp_within["geometry"] = self.all_mp_within.geometry.make_valid()
    full_union = self.all_mp_within.geometry.union_all()
    full_area = full_union.area
    for prio in prio_list:
        part = self.all_mp_within[self.all_mp_within["georef_use_type"] == prio]
        if len(part) == 0:
            use_type_unions[prio] = None
        else:
            use_type_unions[prio] = part.geometry.union_all()

    diffed_areas = dict()
    higher_prio_union = None
    for prio in prio_list:
        geom = use_type_unions[prio]
        if geom is None:
            diffed_areas[prio] = None
            continue
        if higher_prio_union is None:
            diffed_areas[prio] = geom
            higher_prio_union = geom
        else:
            diffed_areas[prio] = geom.difference(higher_prio_union)
            higher_prio_union = higher_prio_union.union(geom)

    # compute statistics for each sub boundary
    def _get_area_dict(geom):
        ret = dict()
        # we use diffed_areas and priority from surrounding scope
        remaining = geom.area
        computed_total = 0
        for prio in prio_list:
            d_geom = diffed_areas[prio]
            if d_geom is None:
                ret[prio] = 0.0
                continue

            area = geom.intersection(d_geom).area
            ret[prio] = area
            computed_total += area
            remaining -= area
        null_area = max(0.0, remaining)
        if "null" in ret:
            ret["null"] += null_area
        else:
            ret["null"] = null_area
        ret["total_area"] = geom.area
        return ret

    main = _get_area_dict(full_union)
    main["name"] = f"all ({self.name})"
    stat_rows = [main]
    for i, row in self.boundaries_within.iterrows():
        sub_name = row["name"]
        sub_geom = row.geometry
        rec = _get_area_dict(sub_geom)
        rec["name"] = sub_name
        stat_rows.append(rec)
    stats_df = pd.DataFrame(stat_rows)
    for prio in prio_list:
        stats_df[f"{prio}_pct"] = (stats_df[prio] / stats_df["total_area"])*100.0
    stats_df = stats_df.round(2)
    stats_df.to_csv(f"{self.name}_stats_output.csv", index=False)

