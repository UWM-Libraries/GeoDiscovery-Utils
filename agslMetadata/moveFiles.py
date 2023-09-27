from pathlib import Path
import csv

movecsv_path = Path(r"C:\Users\srappel\Desktop\move.csv")
movecsv = open(movecsv_path, "r")
reader = csv.reader(movecsv)

for row in reader:
    dirpath = Path(row[0])
    destpath = dirpath.parent.parent / "redo" / dirpath.stem
    dirpath.rename(destpath)
