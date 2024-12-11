# Medley of Metadata Workflows

This repository contains set of Jupyter Notebooks for batch metadata processes.

**Requirements**:

* Python 3
* Jupyter Notebook (https://jupyter.org/install)
* Pandas (https://pandas.pydata.org/getting_started.html)
* Numpy* (https://numpy.org/install/)

*only needed for `04_clean-validate`

## 01_CSV-2-JSON

* CSV template for OGM Aardvark metadata
* Jupyter Notebook that transforms the CSV into OGM Aardvark JSONs

## 04_clean-validate

* CSV of sample OGM Aardvark metadata that has missing or incorrect values
* Jupyter Notebook scans the metadata, fixes it, and produces a log of actions taken


## aardvark-profile

* `aardvark.csv`: Documentation of the OGM Aardvark profile
* `referenceURIs.csv`: Keys and values of the types of references specified in the OGM Aardvark profile and viewable with GeoBlacklight
