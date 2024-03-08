# GeoBlacklight Metadata 1.0 to OpenGeoMetadata version Aardvark

This script will convert the crosswalkable fields in a batch of JSON files in the [GeoBlacklight 1.0 schema](https://opengeometadata.org/docs/gbl-1.0) into the [OpenGeoMetadata Aardvark schema](https://opengeometadata.org/docs/ogm-aardvark)

A set of sample JSONs in the 1.0 schema is included for testing.

## How it works
The script will open the existing JSONs, scan the field names (keys), use the crosswalk.csv file to convert the names, and write new JSONs with the new field names. If will also update the field type from single to array if needed.

If a field name is present in the existing JSONs, but not in the crosswalk, it will include them unchanged in the new JSONS.

## Limitations
This script only changes the field name and type for elements that can be directly crosswalked. Any fields that are not listed in crosswalk.csv will be carried over to the new Aardvark JSONS. This prevents data loss, but means that the new documents will still include deprecated fields.


## How to run
1. Add 1.0 metadata JSON documents to a folder in the directory
2. Open the 1.0-to-aardvark.py script in a text editor
3. Change the value for dir_old_schema to the name of your folder holding the JSON documents
4. Run the script from within the editor or using the command line

