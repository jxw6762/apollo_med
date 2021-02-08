import json
import os
import re
import requests
from zipfile import ZipFile
# If i were productionalizing, i would create a requirements file for the imports for easier dependencies management

# If i were productionalizing, i would consider turning these helper functions into a separate library
def download_zip(url, output_location):
    """takes a url and output location to download file"""
    r = requests.get(url)
    r.raise_for_status()

    with open(output_location, 'wb') as f:
        f.write(r.content)


def unzip(input_location, output_location):
    """takes a zipped file, unzips it and stores it"""
    zp = ZipFile(input_location)
    try:
        zp.extractall(output_location)
    except:
        raise Exception("unzipping failed")


def convert_ndc(ndc_key):
    """takes an ndc string and formats it"""
    re_442 = "\d\d\d\d\-\d\d\d\d-\d\d"
    re_532 = "\d\d\d\d\d\-\d\d\d\-\d\d"
    re_541 = "\d\d\d\d\d\-\d\d\d\d\-\d"

    if re.search(re_442, ndc_key):
        return "0" + ndc_key
    elif re.search(re_532, ndc_key):
        return ndc_key[0:6] + "0" + ndc_key[6:]
    elif re.search(re_541, ndc_key):
        return ndc_key[0:10] + "0" + ndc_key[10:]
    else:
        return "invalid_input_key_format"

def process(fda_list):
    for product in fda_list:
        for package in product["packaging"]:
            package["formatted_package_ndc"] = convert_ndc(package["package_ndc"])

    return None


if __name__ == '__main__':
    # If i were productionalizing, i would probably parametrize these variables or take them in as inputs to the program. Also sanitize the path (via something like os.path)
    # Setting up some parameters
    url = "https://download.open.fda.gov/drug/ndc/drug-ndc-0001-of-0001.json.zip"
    raw_data_location = "./raw_data/"
    zip_filename = "input_data_raw.json.zip"
    unzip_filename = "drug-ndc-0001-of-0001.json"
    zip_output = os.path.join(raw_data_location, zip_filename)
    final_output_location = "./output_data/"
    final_output_filename = "formatted-drug-ndc-0001-of-0001.json"
    final_output = os.path.join(final_output_location, final_output_filename)

    # In case folders aren't setup correctly
    os.makedirs(os.path.dirname(zip_output), exist_ok=True)
    os.makedirs(os.path.dirname(final_output), exist_ok=True)

    download_zip(url, zip_output)
    unzip(zip_output, raw_data_location)

    with open(os.path.join(raw_data_location, unzip_filename), 'r') as f:
        raw_data = json.load(f)

    process(raw_data["results"])

    with open(final_output, 'w') as fp:
        json.dump(raw_data, fp)