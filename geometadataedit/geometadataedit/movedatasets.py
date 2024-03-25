"""
Move datasets according to a csv of dataset directory paths
"""

from pathlib import Path
import csv

movecsv_path = Path(r"C:\Users\srappel\Desktop\redo.csv")
destpath = Path(
    r"S:\GeoBlacklight\project-files\fileProcessing\failed_metadata_processing"
)
movecsv = open(movecsv_path, "r")
reader = csv.reader(movecsv)

for row in reader:
    dirpath = Path(row[0])
    output = destpath / dirpath.stem
    dirpath.rename(output)
