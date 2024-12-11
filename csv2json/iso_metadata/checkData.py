from osgeo import gdal, osr, ogr
import os
import csv
import numpy

#Create a csv of filenames,spatial reference systems (SRS), geometry types types, and bounding boxes.

output_file = open( 'datasets.csv', 'w' )
headers = ['filename','spatial reference','type','format', 'west','east', 'north', 'south', 'dcat_bbox' ]
writer = csv.writer(output_file)
writer.writerow(headers)

#Find shapefiles and geojsons in a directory. Open the data and get the SRS and the geometry type.


def get_fileSize(f):
    for dirName, subDirs, fileNames in os.walk('data'):
        for s in fileNames:
            filePath = os.path.join(dirName, s)
            s = s.split('.')[0]
            if f[:-4] == s:
                fileSize = os.path.getsize (filePath)
                fileSize = fileSize/1000/1000
                fileSize = numpy.around(fileSize, decimals=1)
                fileSize = str(fileSize)
                #print (filePath, fileSize)
   
for dirName, subDirs, fileNames in os.walk('.'):
    for f in fileNames:
        if f.endswith('.shp') or f.endswith('.geojson'):
            print ('Filename: ' + f)
            filePath = os.path.join(dirName, f)
            if f.endswith('.shp'):
                fileFormat = 'Shapefile'
            if f.endswith('.geojson'):
                fileFormat = 'GeoJSON'
            ds = ogr.Open(filePath)
            
            for lyr in ds:
                srs = lyr.GetSpatialRef()
                try:
                   srs
                   srsAuth = srs.GetAttrValue('AUTHORITY',0)
                   srsCode = srs.GetAttrValue('AUTHORITY',1)
                   spatialRef = srsAuth + ':' + srsCode
                except:
                    srsAuth = 'NONE'
                    srsCode = 'NONE' 
                    print ('Missing Spatial Reference ' + f)
                geomType = ogr.GeometryTypeToName(lyr.GetGeomType())
                if geomType == 'Point':
                    geomType = 'Point data'
                if 'Polygon' in geomType:
                    geomType = 'Polygon data'
                if 'Line' in geomType:
                    geomType = 'Line data'
                west, east, south, north = lyr.GetExtent()
                west, east, south, north = str(west), str(east), str(south), str(north)
                boundingBox = 'ENVELOPE(' + west + ',' + east + ',' + north + ',' + south + ')'
            get_fileSize(f)

            output_file.write(f + ',' + spatialRef + ',' + geomType + ',' + fileFormat + ',' +west + ',' + east + ',' + north + ',' + south + ',' + '"' + boundingBox + '"' + '\n')

 #Find geotiffs and get the SRS            
        else:
            if f.endswith('.tif'):
                print ('Filename: ' + f)           
                filePath = os.path.join(dirName, f)
                ds = gdal.Open(filePath)
                prj = ds.GetProjection()
                resType = 'Raster data'
                fileFormat = 'GeoTIFF'
                srs=osr.SpatialReference(wkt=prj)
                try:
                    srs
                    if srs.IsProjected:
                        srs = srs.GetAttrValue('authority', 0) + '::' + srs.GetAttrValue('authority', 1)
                        srsAuth =srs[0:4]
                        srsCode =srs[6:]
                        spatialRef = srsAuth + ':' + srsCode
                except:
                    srsAuth = 'NONE'
                    srsCode = 'NONE'
                    print ('Missing spatial reference ' +f)
                geoTransform = ds.GetGeoTransform()
                west = geoTransform[0]
                south = geoTransform[3]
                east = geoTransform[1]
                north = geoTransform[5]
                east = west + geoTransform[1] * ds.RasterXSize
                north = south + geoTransform[5] * ds.RasterYSize
                west, east, south, north = str(west), str(east), str(south), str(north)
                boundingBox = 'ENVELOPE(' + west + ',' + east + ',' + north + ',' + south + ')'
                output_file.write(f + ',' + spatialRef + ',' + 'Raster data' + ',' + 'GeoTIFF' + ',' + west + ',' + east + ',' + north + ',' + south + ',' + '"' + boundingBox + '"' + '\n')            

output_file.close()



