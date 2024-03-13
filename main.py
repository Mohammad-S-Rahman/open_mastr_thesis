#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
open-MaStR - Main file

Bulk: Download XML-Dump and fill in local SQLite database.
API: Download latest entries using the SOAP-API.

SPDX-License-Identifier: AGPL-3.0-or-later
"""

from open_mastr import Mastr
import os

## specify download parameter

# set custom output path for: csv-export, database, xml-export.
# os.environ['OUTPUT_PATH'] = r"/your/custom/output_path"

# bulk download
bulk_date = "today"
bulk_cleansing = True
data_bulk = [
    "biomass",
    "combustion",
    "gsgk",
    "hydro",
    "nuclear",
    "solar",
    "storage",
    "wind",
    "balancing_area",
    "electricity_consumer",
    "gas",
    "grid",
    "location",
    "market",
    "permit",
]

# API download
# for parameter explanation see: https://open-mastr.readthedocs.io/en/latest/advanced/#soap-api-download

api_date = "latest"
api_chunksize = 1000
api_limit = 10000 #Do NOT exceed this Limit. NEVER!
api_processes = None

data_api = [
    "biomass",
    "combustion",
    "gsgk",
    "hydro",
    "nuclear",
    "solar",
    "storage",
    "wind",
]

api_data_types = ["unit_data", "eeg_data", "kwk_data", "permit_data"]

api_location_types = [
    "location_elec_generation",
    "location_elec_consumption",
    "location_gas_generation",
    "location_gas_consumption",
]
#to get user input added code from line 66 to 76
database_connection = None

download_method = None

#Choosing the Database 
while database_connection not in ('sqlite', 'postgresql'):
    user_input = input("Enter 'sqlite' or 'postgresql' to choose the database: ")

    if user_input == 'sqlite':
        database_connection = 'sqlite'
    elif user_input == 'postgresql':
        database_connection = 'postgresql'
    else:
        print("Invalid input. Please enter 'sqlite' or 'postgresql'.")

# Choosing Between API or bulk downloading method
while download_method not in ('API', 'bulk'):
    user_input2 = input("Enter 'API' or 'bulk' downloading system: ")

    if user_input2 == 'API':
        download_method = 'API'
    elif user_input2 == 'bulk':
        download_method = 'bulk'
    else:
        print("Invalid input. Please enter 'bulk' or 'API'.")

# instantiate Mastr class, added parameter database_connection to specify database engine
db = Mastr(database_connection)

if __name__ == "__main__":

    ## download Markstammdatenregister
    # bulk download
    if download_method =='bulk':
        db.download(method="bulk", data=data_bulk, date=bulk_date, bulk_cleansing=True)
    elif download_method == 'API':
    # API download
        db.download(
        method="API",
        data=data_api,
        date=api_date,
        api_processes=api_processes,
        api_limit=api_limit,
        api_chunksize=api_chunksize,
        api_data_types=api_data_types,
        api_location_types=api_location_types,
        )

    ## export to csv
    """
    Technology-related tables are exported as joined, whereas additional tables
    are duplicated as they are in the database. 
    """
    db.to_csv()
