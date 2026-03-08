config / path config is messy...


I now have the necessary model fields, but need to adjust my earlier script to be used in django, which includes several tasks:
* I need a simple view where the base data dir is globbed and checked against the db to display discovered base data files and whether a derived file already exists and which allows to create a derived file as well as adding provenance info
* I should update the way areas are compared for analysis, especially with areas on the boundaries that extend beyond the current area (a forest extending along the boundary between to cities, for example)
* I should add a little bit of safeguarding against slivers and such, do a bit of clean and snap 
* I need to change the transform_script to run not as a main but as a module -> I can still add a main script outside django to do the transform by hand -> NO! managing manual transformations in django will become a nuisance. 
* I need to add the logic to store the relevant results in the django db for quick access.
* 