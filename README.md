
## Hobby project to dive into GIS with python
>This is a hobby project, the code is provided without any claims as to its safety or usefulness. I made this public on github simply as an addon to my CV ; )

This is a simple hobby project to load data from a .gpkg file (in this case an OSM dump, extract the specified city  and then classify how its area is used, split by administrative sub-levels. This was tested on the osm-dump of a German federal state and will (so far) only work for similar types of data due to hard coded CRS values and names of columns.

### Setup
>if using mamba+pip, you may need to install gdal via mamba before installing the requirements with pip


### Main learnings covered in this project
> Note that these learnings are not "covered" or described, but building this yourself may produce these learnings
 * Using qgis to check and inspect data
 * Getting and parsing data in python with geopandas
 * Different types of mapping data, e.g. OSM vs cadastral data 
 * Specific style of OSM data, layers
 * Impact of CRS, (broken) Geometries, overlaying objects
 * Classifications of types of use on mapped areas using hierarchies
 * being aware of double counting, some performance considerations

