import updateMetadata
import csv
from pathlib import Path

warnings = []
CSV_OUTPUT = Path(r"C:\Users\srappel\Desktop\GeoDiscovery_Log.csv")

# Take as an argument a directory containing a bunch of datasets (in dirs)
target_directory = Path(r"S:\_R_GML_Archival_AGSL\GIS_Data\GeoBlacklight\public")

### Add it to a CSV file so we have a log of all these!
csvfile = open(CSV_OUTPUT, 'w', newline='')
logwriter = csv.writer(csvfile)
logwriter.writerow(["INPATH", "STATUS", "ARKID", "WARNING", "ERROR"])

def main():
    ### Purge BIND if status is failing:
    def purge():
        if status == "failing":
            try:
                dataset.metadata.bind(purge=True)
            except Exception as error:
                print(error)
                warnings.append(error)
            print()
            print('###PURGE###')
            print()
            # Delete the zipfile:
            try:
                if dataset.fileserver_zip.exists():
                    dataset.fileserver_zip.unlink()
            except Exception as error:
                print(error)
                warnings.append(error)
            # Delete the directory
            try:
                if dataset.fileserver_dir.exists():
                    dataset.fileserver_dir.rmdir()
            except Exception as error:
                print(error)
                warnings.append(error)
            # Delete the metadata
            try:
                if dataset.fileserver_metadata.exists():
                    dataset.fileserver_metadata.unlink()
            except Exception as error:
                print(error)
                warnings.append(error)

    # Loop through each directory in the parent folder 
    def list_all_dirs(rootdir) -> list[tuple[Path,int]]:
        rootdir = Path(rootdir)
        all_directories = []
        for path in sorted(rootdir.rglob("*")):
            if path.is_dir():
                depth = len(path.relative_to(rootdir).parts)
                path_tuple = (path, depth)
                all_directories.append(path_tuple)
        return all_directories

    for dataset_directory in list_all_dirs(target_directory):
        status = "passing"
        if dataset_directory[1] == 1: # Only children of root

    # Create a updateMetadata.Dataset object by passing each dir Path
    # to the Dataset factory Dataset(Path)
    # This will create:
    #   - Dataset.data (Path to the actual data)
    #   - Dataset.datatype (Integer code)
    #   - Dataset.metadata (A AGSLMetadata Object)
            try:
                dataset = updateMetadata.Dataset(Path(dataset_directory[0]))
            except Exception as error:
                warning = f"Failed to create Dataset object for {str(dataset_directory[0])}\n"
                print(warning)
                print(error)
                warnings.append(warning)
                warnings.append(error)
                status = "failing"
                logwriter.writerow([dataset_directory[0], status, dataset.metadata.identifier.assignedName, warning, error])
                purge()
                continue # Will go to the next Dataset.

    # Creating the Dataset object will create the AGSLMetadata object:
    # This creates:
    #   - Dataset.metadata.xml_text
    #   - Dataset.metadata.md_object
    #   - Dataset.metadata.rootElement
    #   - Dataset.metadata.altTitle
    #   - Dataset.metadata.rights

    ### Now we need to run through some functions on the data:

    ## Create and Write Identifiers:
    ## Dataset.metadata.create_and_write_identifiers()
            try:
                dataset.metadata.create_and_write_identifiers()
            except Exception as error:
                warning = f"Failed to create and write identifiers for {str(dataset_directory[0])}\n"
                print(warning)
                print(error)
                warnings.append(warning)
                warnings.append(error)
                status = "failing"
                logwriter.writerow([dataset_directory[0], status, dataset.metadata.identifier.assignedName, warning, error])
                purge()
                continue
    # First, this will mint a new Identifier object.
    # This creates:
    #   -Dataset.metadata.identifier.arkid
    #   -Dataset.metadata.identifier.nameAuthorityNumber
    #   -Dataset.metadata.identifier.assignedName

    # Next, it will write the infomration from the Identifier object
    # into the correct places in the xml metadata.

    # Finally, it saves the metadata object.

    ## Update AGSL Hours
    ## Dataset.metadata.update_agsl_hours() 
            try:
                dataset.metadata.update_agsl_hours()
            except Exception as error:
                warning = f"Failed to update AGSL hours for {str(dataset_directory[0])}\n"
                print(warning)
                print(error)
                warnings.append(warning)
                warnings.append(error)
                status = "failing"
                logwriter.writerow([dataset_directory[0], status, dataset.metadata.identifier.assignedName, warning, error])
                purge()
                continue
    # Updates the hours in the metadata and saves the metadata object.

    ## Dual Metadata Export
    ## Dataset.metadata.dual_metadata_export
            try:
                dataset.metadata.dual_metadata_export()
            except Exception as error:
                warning = f"Failed to export metadata for {str(dataset_directory[0])}\n"
                print(warning)
                print(error)
                warnings.append(warning)
                warnings.append(error)
                status = "failing"
                logwriter.writerow([dataset_directory[0], status, dataset.metadata.identifier.assignedName, warning, error])
                purge()
                continue
    # This exports the metadata to the parent directory for the dataset

    ## Bind the ArkID using NOID
    ## Dataset.metadata.bind()
            try:
                dataset.metadata.bind()
            except Exception as error:
                warning = f"Failed to NOID bind for {str(dataset_directory[0])}\n"
                print(warning)
                print(error)
                warnings.append(warning)
                warnings.append(error)
                status = "failing"
                logwriter.writerow([dataset_directory[0], status, dataset.metadata.identifier.assignedName, warning, error])
                purge()
                continue
    # Creates the bind parameters:
    # who, what, when, where, meta-who, meta-when, meta-uri, rights, download
    # Sends the bind request using the requests library

    ### Now we need to "ingest" the dataset, which involves
    # Creating a new zip archive containing the data and xml metadata
    # The zip file will be named after the alt-title
    # Put it in a directory named after the assignedName from the arkid
    # Put that directory in the appropriate folder on the webserver (by rights)
    # Create a copy of the ISO Xml (assignedName_ISO.xml) in the metadata directory on the webserver
            try:
                dataset.ingest()
            except Exception as error:
                warning = f"Failed to ingest {str(dataset_directory[0])}\n"
                print(warning)
                print(error)
                warnings.append(warning)
                warnings.append(error)
                status = "failing"
                logwriter.writerow([dataset_directory[0], status, dataset.metadata.identifier.assignedName, warning, error])
                purge()
                continue
                
    ### Testing/Logging
            print(f"Successfully updated and ingested {str(dataset_directory[0])}!\n")
            status = "passing"
            logwriter.writerow([dataset_directory[0], status, dataset.metadata.identifier.assignedName,"",""])

    # if it is successful:
    # write to log the path on the webserver
    if len(warnings) >= 1:
        print(f"Finished with the following {str(len(warnings) / 2)} errors:")
        for warning in warnings:
            print(warning)
            print()
    else:
        print("Finished with no errors!")

    csvfile.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt as error:
        csvfile.close()
        print(error)
