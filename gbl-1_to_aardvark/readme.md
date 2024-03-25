# UW-Milwaukee AGSL
# GeoBlacklight Metadata 1.0 to OpenGeoMetadata version Aardvark

## Last modified:
3/14/2024 by SRA (See [UWM Changelog](#UWM-Changelog))

This script will convert the crosswalkable fields in a batch of JSON files in the
[GeoBlacklight 1.0 schema](https://opengeometadata.org/docs/gbl-1.0)
into the [OpenGeoMetadata Aardvark schema](https://opengeometadata.org/docs/ogm-aardvark)

A set of sample JSONs in the 1.0 schema is included for testing.

## How it works

The script will open the existing JSONs, scan the field names (keys), use the crosswalk.csv file
to convert the names, and write new JSONs with the new field names. If will also update the field
type from single to array if needed.

If a field name is present in the existing JSONs,
but not in the crosswalk, it will include them unchanged in the new JSONs.

## Limitations

This script only changes the field name and type for elements that can be directly crosswalked.
Any fields that are not listed in crosswalk.csv will be carried over to the new Aardvark JSONS.
This prevents data loss, but means that the new documents ~~will still include deprecated fields~~
include local fields from other institutions that aren't removed in the remove_deprecated() function.

## How to run

1. Add 1.0 metadata JSON documents to a folder in the directory
2. Open the 1.0-to-aardvark.py script in a text editor
3. Change the value for dir_old_schema to the name of your folder holding the JSON documents
4. Run the script from within the editor or using the command line

## UWM Changelog:

1. [Add support for Pathlib](https://github.com/UWM-Libraries/gbl-1_to_aardvark/commit/2ba511a26b4730c5c77fab7e937b02a3201d8d75)
to allow more robust directory structure 
2. Adds new functions for dealing with common metadata issues and adds those functions as subfunctions of `schema_update()`:
    - `check_required()` checks for the presence of required fields (defined in an array) and warns if missing and
    tries to make logical replacements or fill with default values defined at the top of the script
    - `remove_deprecated()` will remove deprecated fields (defined in an array)
3. Adds an output folder (./aardvark) with a .gitkeep file and ignores contents of that folder to avoid tracking output.
(Part of [PR](https://github.com/OpenGeoMetadata/gbl-1_to_aardvark/pull/4))
4. Force the `gbl_mdVersion_s` field to 'Aardvark' instead of crosswalking from the deprecated `geoblacklight_version` field.
(Part of [PR](https://github.com/OpenGeoMetadata/gbl-1_to_aardvark/pull/4))
5. Removes `geoblacklight_version,gbl_mdVersion_s` from the crosswalk.
(Part of [PR](https://github.com/OpenGeoMetadata/gbl-1_to_aardvark/pull/4))
6. [Adds recursive searching](https://github.com/UWM-Libraries/gbl-1_to_aardvark/commit/4c80f4585e8855d175faf7f9208bf6bfa35e953a)
using pathlib and glob.
