> These are internal notes, different from docs. I use them in my obsidian setup, these are the "public" parts, as they may provide some insights into the thought process while not being as definitive as docs.

## Ressources

for the osm admin levels
https://wiki.openstreetmap.org/wiki/Tag:boundary%3Dadministrative#Country_specific_values_%E2%80%8B%E2%80%8Bof_the_key_admin_level=*

## Outline

Given a gpkg data dump, e.g. from osm (and for now, only osm ; ), extract administrative boundaries and compute a land-use map of mutually exclusive, annotated multipolygons, basically creating a complete coverage using custom land-use categories, given a requested area of interest. The results should be inspectable in a UI and stats should be available for a chosen administrative area (e.g. a city (NUTS 5) or "Landkreis" (NUTS 3))