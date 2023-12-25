# importNaPTAN
A python script to import a NaPTAN Stops.csv file into a database

The NaPTAN (National Public Transport Access Nodes) is a dataset of Great Britain's public transport access points, ie anywhere you can get on or off public transport (including bus, rail, tram, metro, underground, air and ferry services).

You can download the latest Stops.csv file from:
- [https://beta-naptan.dft.gov.uk/](https://beta-naptan.dft.gov.uk/) the beta indicates that this will go live at some point and the URL will change.

This script cleans up the data, sets columns to int, float or string as appropriate and where the latitude and longitude are missing it updates these from the provided northings and eastings. Some columns are omitted as I found them to be empty or of no use to me - these are in the notTheseCols function.

### Setup
I am assuming familiarty with a MySQL database, and that you have the appropriate permissions to create, truncate, insert and update tables.

Set your database details at the top of the importNaPTAN.py file, set the location of your Stops.csv and the name of the table (currently: NaPTANdata) you wish to import into,
then create the table with:
```
python importNaPTAN.py -m
```
on success, import the file:
```
python importNaPTAN.py -i
```

### Usage
```
usage: importNaPTAN.py [-h] [-m] [-i]

Import NaPTAN csv file to database

options:
  -h, --help         show this help message and exit
  -m, --make-table   Create the NaPTANdata table in your database
  -i, --import-data  Import the '../Stops.csv' CSV Data
```
