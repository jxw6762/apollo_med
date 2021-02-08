import json
import os
import requests
from zipfile import ZipFile

def download_zip(url, output_location):
    """takes a url and output location to download file"""
    r = requests.get(url)
    r.raise_for_status()

    os.makedirs(os.path.dirname(output_location), exist_ok=True)
    with open(output_location, 'wb') as f:
        f.write(r.content)


def unzip(input_location, output_location):
    """takes a zipped file, unzips it and stores it"""
    zp = ZipFile(input_location)
    try:
        zp.extractall(output_location)
    except:
        raise Exception("unzipping failed")