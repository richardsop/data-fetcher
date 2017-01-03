
# coding: utf-8

import pandas as pd
import xmltodict
import glob
import os
from pymysql import *

import mysql.connector
from sqlalchemy import create_engine
import json
from pandas.io.json import json_normalize

import os, gzip
from sqlalchemy import create_engine

import shutil

import glob
import pytz
import time
from datetime import datetime, timedelta

engine = create_engine('mysql+pymysql://database:pw@server:3306/database?charset=utf8mb4', echo=False)

class GlobalVariables:
    pma_archive = max(glob.iglob('Z:\\*\*\\*\performance\\test\????????_????.*_????????_??????.xml.gz'), key=os.path.getctime)
    targetFolder = r"C:\\Users\*\Documents\*\*"
    latestfile_time = os.path.getctime(pma_archive)
    last_24hrs_delta = pytz.utc.localize(datetime.today()) - timedelta(days=1) #latest 24 hours
    last_24hrs_microsec = time.mktime(last_24hrs_delta.timetuple()) + last_24hrs_delta.microsecond * 1e-6 # #latest 24 hours in microseconds

GlobalVariables.pma_archive

# process files and writes file to target folder
def processfile():
    if GlobalVariables.latestfile_time > GlobalVariables.last_24hrs_microsec:
        file = GlobalVariables.pma_archive
        filepath = file
        base = os.path.basename(file)
        dest_name = os.path.join(GlobalVariables.targetFolder, base[:-3])
        with gzip.open(file, 'rb') as infile:
            with open(dest_name, 'wb') as outfile:
                for line in infile:
                    try:
                        doc = xmltodict.parse(infile.read())
                        doc1 = doc["AutomaticPerformanceMonitoringOutput"]["Header"]
                        doc2 = doc["AutomaticPerformanceMonitoringOutput"]["CalculationResults"]

                        #infil.seek(0)
                        header = json_normalize(doc1)
                        CalculationResults = json_normalize(doc2)
                        result = pd.concat([header, CalculationResults], axis=1)
                        result_m = result[["MamTime.Value", "EndTime.#text","MachineNumber.#text","MachineType.#text","Release.#text","StartTime.#text", "Status.#text", "TaskName.#text"]]
                        result_m = result_m.rename(index=str, columns={"MamTime.Value": "average_mam_time_s", "EndTime.#text": "endTime", "MachineNumber.#text": "machineNumber",                              
						"Release.#text": "release","MachineType.#text":"machineType", "StartTime.#text": "startTime",                                 
						"Status.#text": "status_mam", "TaskName.#text": "taskName" })
                        result_m.to_sql(name='ovl_mam_time', con=engine, if_exists = 'append', index=False)
                        #print header
                        outfile.write(line)
                    except Exception:
					
                        pass



if __name__ == "__main__":
    while True:
        processfile()
        time.sleep(86400) # 86400 seconds = 24 hour


