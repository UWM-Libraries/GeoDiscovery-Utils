# UpdateMetadata.py
import arcpy_metadata as md # Only works in 32 bit python. Will not work in Pro
from pathlib import Path
import arcpy # best practice is to always import arcpy last

pathToShapefile = 'S:\_H_GML\Departments\AGSL\GIS\Projects\METADATA\Metadata_Python\DaneAccess\DaneCounty_BuildingFootprints_2005_UW\DaneCounty_BuildingFootprints_2005.shp'

metadata = md.MetadataEditor(pathToShapefile)

contactEmail = metadata.point_of_contact.email

print(contactEmail)

identifierCode = metadata.file_identifier

print(identifierCode)

# Some examples of stuff we can grab from the Describe method
description = arcpy.Describe(pathToShapefile)

print('Shape Type: ' + description.shapeType)
print('spatialReference: ' + description.spatialReference.exportToString())
print('Extent (XMin): ' + str(description.Extent.XMin))