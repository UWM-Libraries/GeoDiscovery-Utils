import updatemetadata
import csv
import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

warnings = []

CSV_OUTPUT = Path(r'S:\GeoBlacklight\project-files\fileProcessing\logs\LOG20231023.csv')

# Take as an argument a directory containing a bunch of datasets (in dirs)
target_directory = Path(r"S:\GeoBlacklight\project-files\fileProcessing\ready_to_process")

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

            try:
                dataset = updatemetadata.Dataset(Path(dataset_directory[0]))
            except Exception as error:
                warning = f"Failed to create Dataset object for {str(dataset_directory[0])}\n"
                print(warning)
                print(error)
                warnings.append(warning)
                warnings.append(error)
                status = "failing"
                logwriter.writerow([dataset_directory[0], status, "none assigned", warning, error])
                purge()
                continue # Will go to the next Dataset.

            try:
                existing_identifier = dataset.metadata.get_existing_identifier_or_mint()
            except Exception as error:
                warning = f"Failed to create existing_identifier object for {str(dataset_directory[0])}\n"
                print(warning)
                print(error)
                warnings.append(warning)
                warnings.append(error)
                status = "failing"
                logwriter.writerow([dataset_directory[0], status, "none assigned", warning, error])
                purge()
                continue # Will go to the next Dataset.

            try:
                dataset.metadata.write_identifiers(existing_identifier)
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
