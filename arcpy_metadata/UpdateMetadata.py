# UpdateMetadata.py
import arcpy_metadata as md # # Only works in 32 bit python. Will not work in Pro
import os
import arcpy # best practice is to always import arcpy last

workspace = "S:\_H_GML\Departments\AGSL\GIS\Projects\METADATA\Metadata_Python\DaneAccess"

# Walk the workspace
print('Walking the workspace ' + workspace)
walk = arcpy.da.Walk(workspace, datatype="FeatureClass")
print('Walk complete.')


i = 1
for dirpath, dirnames, filenames in walk:
    for filename in filenames:
        featureClass = (os.path.join(dirpath, filename))

        # Check that walk actually found at least one file:
        if not featureClass is None:
            featureClassMetadata = md.MetadataEditor(featureClass)
            print('Shapefile ' + str(i) + ': ' + featureClassMetadata.title)
            i += 1

            # Do something for each feature class


        else:
            print('No Feature Class(es) detected')

