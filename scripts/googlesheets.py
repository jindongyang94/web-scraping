"""
The purpose of this script is to export the csv from the s3 csv file to google sheets periodically as well. 
"""

from __future__ import print_function

import csv
import json
import os
from glob import glob
from pprint import pformat

import gspread
from gspread_formatting import *
from oauth2client.service_account import ServiceAccountCredentials
from yaml import Loader, load

from webscraping.gebiz.scripts.gebiz_scraping import (CONFIG, delete_csv,
                                                      download_csv, logger)


def main():
    """
    Main script to download csv from s3 and then export it to google sheets. 
    """
    config = load(open(CONFIG), Loader=Loader)
    s3bucket = config['s3bucket']
    s3path = config['s3path']
    csvname = config['csvname']

    # Download CSV from s3
    download = download_csv(s3bucket, s3path, csvname)

    # Send to Google Sheets
    sheetid = '1crg3PNJeZow3qh-NNn55zaODVJEn02trJj-U04sgTY8'
    sheetname = 'Sheet1'
    credentialpath = glob('config/*.json')
    # If there is no json authentication, create the json file from the secrets volume mounted
    if not credentialpath:
        credentialpath = create_credentials('config/credentials.json')
    else:
        credentialpath = str(credentialpath[0])

    uploaded = upload_googlesheets(csvname, sheetid, sheetname, credentialpath)

    # Format cells if possible
    if uploaded:
        format_googlesheets(sheetid, sheetname, credentialpath)

    # Delete CSV from local directory
    deleted = delete_csv(csvname)

    # Return Success Signal
    if deleted:
        logger.info('Successfully exported CSV and deleted from local directory.')
        return True



def upload_googlesheets(csvname, sheetid, sheetname, credentialpath):
    """
    After generating the needed credential json file, you can use that to export the file 
    to needed the sheets as selected.
    """

    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentialpath, scope)
    client = gspread.authorize(credentials)

    # wks = client.open("gebiz").sheet1
    wks = client.open_by_key(sheetid)

    wks.values_update(
        sheetname,
        params={'valueInputOption': 'USER_ENTERED'},
        body={'values': list(csv.reader(open(csvname)))}
    )

    return True

def format_googlesheets(sheetid, sheetname, credentialpath):
    """
    1. Bold the headers 2. Freeze the headers and Reference Numbers 
    """
    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentialpath, scope)
    client = gspread.authorize(credentials)

    wks = client.open_by_key(sheetid)
    worksheet = wks.worksheet(sheetname)

    fmt = cellFormat(
        backgroundColor=color(1, 1, 1),
        textFormat=textFormat(bold=True, foregroundColor=color(0, 0, 0)),
        horizontalAlignment='CENTER'
        )
    row_index = 1
    format_cell_range(worksheet, rowcol_to_a1(row_index,1)+':' + rowcol_to_a1(row_index, worksheet.col_count), fmt)

    # Freeze Headers and Ref Numbers
    set_frozen(worksheet, rows=1, cols=1)

    return True


def create_credentials(credentialpath):
    """
    Extract credentials from secret mount path and create json file to put in
    modules/webscraping/gebiz/config/credentials.json
    """
    secret_googlesheetpath = '/etc/googlesheets_secret'
    secrets = glob(os.path.join(secret_googlesheetpath, "*"))
    secret_dict = {}

    for secret in secrets:
        with open(secret, 'r') as f:
            basename = os.path.basename(secret)
            secret_dict[basename] = f.read()

    with open(credentialpath, 'w') as f:
        json.dump(secret_dict, f)

    logger.info('GoogleSheets Credentials:\n')
    for line in pformat(secret_dict).split('\n'):
        logger.info(line)
    logger.info('\n')

    if glob(credentialpath):
        return credentialpath
        
    

if __name__ == '__main__':
    main()
