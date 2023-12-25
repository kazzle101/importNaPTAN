#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import math
import csv
import os
import pyproj                # sudo apt install python3-pyproj    / pip install pyproj
import argparse
import mysql.connector
from progress.bar import Bar # sudo apt install python3-progress  / pip install progress

_mydb = mysql.connector.connect(
        host="localhost",
        user="<your_username>",
        password="<your_password>",
        database="<your_database>")


## download the CSV from: https://beta-naptan.dft.gov.uk/download
## old site: https://www.data.gov.uk/dataset/ff93ffc1-6656-47d8-9155-85ea0b8f2251/national-public-transport-access-nodes-naptan
_NaPTANfile = "Stops.csv"
_NaPTANtable = "NaPTANdata"

class ImportBusStops:

    def __init__(self):
        # self.busStopsFile = _NaPTANfile
        # self.busStopsTable = _NaPTANtable
        self.keyNames  = self.columns()
        self.notThese  = self.notTheseCols()
        self.theseCols = self.onlyTheseCols(self.keyNames, self.notThese)

    ## this conatins all the columns in the CSV file, they are used for the database table - less the notTheseCols[]
    def columns(self):
        cols = ["ATCOCode","NaptanCode","PlateCode","CleardownCode","CommonName","CommonNameLang","ShortCommonName",
                "ShortCommonNameLang","Landmark","LandmarkLang","Street","StreetLang","Crossing","CrossingLang","Indicator",
                "IndicatorLang","Bearing","NptgLocalityCode","LocalityName","ParentLocalityName","GrandParentLocalityName",
                "Town","TownLang","Suburb","SuburbLang","LocalityCentre","GridType","Easting","Northing","Longitude",
                "Latitude","StopType","BusStopType","TimingStatus","DefaultWaitTime","Notes","NotesLang","AdministrativeAreaCode",
                "CreationDateTime","ModificationDateTime","RevisionNumber","Modification","Status"]

        return cols
    
    ## this is a list of columns that I don't want to import. I found them to be either empty or I have no use for.
    def notTheseCols(self):

        cols = ["CommonNameLang", "ShortCommonNameLang","LandmarkLang","StreetLang","Crossing", "CrossingLang",
                "IndicatorLang","TownLang","SuburbLang","GrandParentLocalityName","TimingStatus","DefaultWaitTime",
                "Notes","NotesLang","AdministrativeAreaCode", "CleardownCode", "ParentLocalityName", "LocalityCentre"]

        return cols
    
    def onlyTheseCols(self, cols, notThese):
        theseCols = []
        for k in cols:
             if k not in notThese:
                  theseCols.append(k)

        return theseCols

    def setupMaxLengths(self, cols):
         
        data = {key: 0 for key in cols}
        return data

        # Easting INT(11),
        # Northing INT(11),
        # Longitude FLOAT,
        # Latitude FLOAT,
        # CreationDateTime DATETIME,
        # ModificationDateTime DATETIME,
        # RevisionNumber INT(11),

    def setType(self, key, value, dbType=False, dbLen=0):

        dateFormat = "%Y-%m-%dT%H:%M:%S"
        trueList = ["1", "true", "yes"]

        if key == "Easting" or key == "Northing" or key == "RevisionNumber":
            if dbType:
                return "INT(11)"
            if value is None or not value:
                return None
            return int(value)
        if key == "Longitude" or key == "Latitude":
            if dbType:
                return "FLOAT"
            if value is None or not value:
                return 0.0
            return float(value)
        if key == "CreationDateTime" or key == "ModificationDateTime":
            if dbType:
                return "DATETIME"
            if value is None or not value:
                return datetime.datetime(1800, 1, 1, 0, 0, 0)
            ds = value.split(".")[0]
            dtObj = datetime.datetime.strptime(ds, dateFormat)
            return dtObj
        if key == "LocalityCentre":
            if dbType:
                return "TINYINT(1)"
            if value is None or not value:
                return False            
            value = str(value).lower()
            if value in trueList:
                return True
            return False
        
        if dbType:
            return f"VARCHAR({dbLen})"

        return str(value)

    def createTable(self):
        db = _mydb.cursor(dictionary=True)
        maxCols = {key: 0 for key in self.theseCols}

        if self.checkForTable():
            print (f"{_NaPTANtable}.{_mydb.database} already exists")
            print ("exiting...")
            return

        print("Finding maximum line lengths")
        data = self.readData()
        if not data:
            return False
        
        for r in data:
            for key in self.theseCols:
                l = len(str(r.get(key, '')))
                if l > maxCols[key]:
                    maxCols[key] = len(str(r.get(key, '')))

        print(maxCols)
        sql = f"\nCREATE TABLE {_NaPTANtable} (\n"
        for key, value in maxCols.items():
            value = math.ceil(value)
            if value % 2 != 0:
                value += 1

            sql += "\t{} {} DEFAULT NULL,\n".format(key, self.setType(key, None, True, value+2))

        sql = sql[0:-2]+"\n"
        sql += ");\n"
        sql += " \n"
        sql += f"ALTER TABLE {_NaPTANtable} \n"
        sql += "\tADD UNIQUE KEY ATCOCode (ATCOCode), \n"
        sql += "\tADD KEY LatLong_IDX (Longitude,Latitude);\n"
        db.execute(sql)
        _mydb.commit()
        return True

    def checkForTable(self):
        db = _mydb.cursor(dictionary=True)

        db.execute(f"SHOW TABLES LIKE '{_NaPTANtable}'")
        result = db.fetchone()

        if result:
            return True
        
        return False

    def checkForFile(self):
        if os.path.exists(_NaPTANfile):
            if os.access(_NaPTANfile, os.R_OK):
                return True
            else:
                print(f"file '{_NaPTANfile}' exists but is not readable.")
                return False
        
        print(f"file '{_NaPTANfile}' not found.")
        return False

    def readData(self):

        data = []
        lineCount = 0
        try:
            with open(_NaPTANfile, 'r') as fp:
                for _ in fp:
                    lineCount += 1
        except FileNotFoundError:
            print(f"file not found: {_NaPTANfile}")
            exit()


        with open(_NaPTANfile, newline='') as csvfile:
            reader = csv.DictReader(csvfile, fieldnames=self.keyNames)
            next(reader)
            with Bar('Reading...  ', max = lineCount-1) as bar:
                for row in reader:
                    filteredRow = {key: value.strip() for key, value in row.items() if key not in self.notThese}
                    data.append(filteredRow)
                    bar.next()
        return data 

    def confirmDeletion(self):
        confirmation = input(f"This will delete all existing data from '{_NaPTANtable}.{_mydb.database}' and import the '{_NaPTANfile}' file.\nDo you wish to proceed? (y/n): ")
        if confirmation.lower() == 'y':
            return True
        elif confirmation.lower() == 'n':
            print("exiting...")
            exit()
        else:
            print("Invalid input. Please enter 'y' for yes or 'n' for no.")
            self.confirmDeletion()

    def importData(self):
         
        db = _mydb.cursor(dictionary=True)

        if not self.checkForTable():
            print (f"Cannot find {_NaPTANtable} in database")
            print ("exiting..")
            exit()

        if not self.checkForFile():
            print ("exiting..")
            exit()

        self.confirmDeletion()

        print(f"Loading: '{_NaPTANfile}'")
        data = self.readData()
        if not data:
            return False
        
        dataOut = []

        db.execute(f"TRUNCATE TABLE {_NaPTANtable}")
        _mydb.commit()

        ## clean the data, convert strings to float or int as appropriate
        print (f"Processing {len(data)} records")
        for row in data:
            processedRow = {}
            for key, value in row.items():
                processedValue = self.setType(key, value)
                processedRow[key] = processedValue

            dataOut.append(processedRow)

        print("Records to insert: {}".format(len(dataOut)))

        with Bar('Importing...', max = len(dataOut)) as bar:
            for row in dataOut:
                columns = ', '.join(row.keys())
                values = ', '.join(['%s' for _ in row.keys()])
                sql = f"INSERT INTO {_NaPTANtable} ({columns}) VALUES ({values})"
                db.execute(sql, tuple(row.values()))
                bar.next()
        
        _mydb.commit()
        return True

    ## some records don't have a Latitude and Longitude so these get set from the Eastings and Northings
    def fixLatLong(self):

        db = _mydb.cursor(dictionary=True)

        db.execute(f"SELECT ATCOCode, Easting, Northing FROM {_NaPTANtable} WHERE Latitude=0 AND Longitude=0")
        results = db.fetchall()

        if not results:
            print ("nothing to do")
            return 

        sourceCRS = pyproj.CRS("EPSG:27700") 
        targetCRS = pyproj.CRS("EPSG:4326")  # WGS84 

        transformer = pyproj.Transformer.from_crs(sourceCRS, targetCRS)

        print ("Setting Latitude and Longitude on records without these values")
        print ("Records to update: {}".format(len(results)))
        with Bar('Setting...  ', max = len(results)) as bar:
            for r in results:
                easting = r["Easting"]
                northing = r["Northing"]
                atc = r["ATCOCode"]    

                lon, lat = transformer.transform(easting, northing)

                sql = f"UPDATE {_NaPTANtable} SET Latitude=%s, Longitude=%s WHERE ATCOCode=%s"
                db.execute(sql, (lat, lon, atc))
                bar.next()

        _mydb.commit()

        return

def main():
    importBusStops = ImportBusStops()

    parser = argparse.ArgumentParser(description='Import NaPTAN csv file to database ')
    parser.add_argument('-m', '--make-table', action='store_true', help=f'Create the {_NaPTANtable} table in your database')
    parser.add_argument('-i', '--import-data', action='store_true', help=f"Import the '{_NaPTANfile}' CSV Data")
    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        return

    if args.make_table:
        importBusStops.createTable()
        return

    if args.import_data:
        chk = importBusStops.importData()
        if chk:
            importBusStops.fixLatLong()   
    
    return

if __name__ == "__main__":
    main()