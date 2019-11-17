"""
Download and returns all paintings and metadata from Wikiart
in the Ink Wash Painting category
Upload all downloaded paintings and metadata to Amazon S3
"""

import json
import shutil
import sys
from glob import glob
import boto3
import requests
from pathlib import Path

# Wikiart
BASE_URL= "https://www.wikiart.org/"
STYLE_URL = "en/paintings-by-style/ink-and-wash-painting?json=2"
PAGINATION_URL = "page="
# Timeout for WikiArt
METADATA_REQUEST_TIMEOUT = 2 * 60
PAINTINGS_REQUEST_TIMEOUT = 5 * 60
BASE_BUCKET  = 'ink-wash-painting'
# Local filepaths
TOP_LEVEL_PATH = Path(r"C:\Users\Julie\Desktop\ink-wash-art")
ASSET_PATH = TOP_LEVEL_PATH/ "assets"
# Metadata filename
METADATA_FILENAME = 'metadata.json'
MEDTADATA_FILE = ASSET_PATH.joinpath(METADATA_FILENAME)
# intialize connection to S3 resources
s3_client = boto3.client('s3', 'us-east-1')

def parse_data(paints_list,data):
    """
    Extends a list of paintings
    :param paints_list:
    :param data:
    :return:
    """
    paints_list.extend(data)

def get_json():
    """
    Get JSON with art name and location from WikiArt
    :return: dictionary of filenames from WikiArt
    """
    data_list = []

    for page in range(1,13):
        url = BASE_URL + STYLE_URL + "&" + PAGINATION_URL + str(page)
        print(page, "pages processed")
        try:
            response = requests.get(url, timeout=METADATA_REQUEST_TIMEOUT)
            data = response.json()['Paintings']
            parse_data(data_list, data)
        except requests.exceptions.RequestException as e:
            print(e)
            sys.exit(1)

    return data_list


def save_json(data):
    """
    Converts list to JSON, writes to file
    :param data: Data (list)
    :return:
    """
    data = json.dumps(data)

    with MEDTADATA_FILE.open('w') as outfile:
        outfile.write(data)

def get_image_links(data):
    """
    Passes in a list of image links
    :param data: Data (list)
    :return: List of painting links
    """
    painting_links = []

    print(data)

    for painting in data:
        painting_links.append(painting['image'])

    return painting_links


def download_images(links):
    """
    Passes in a list of links pointing to image files to download
    :param links (list):
    :return Images downloaded into the assets folder:
    """

    for link in links:
        print("Processing", link)
        try:
            response = requests.get(link,
                                    timeout=METADATA_REQUEST_TIMEOUT, stream=True)
        except requests.exceptions.RequestException as e:
            print(e)
            sys.exit(1)

        artist_name = link.rsplit('/', 2)[1]
        image_name  = link.rsplit('/', 2)[2]
        image_name = artist_name + image_name

        file_location = ASSET_PATH.joinpath(image_name)

        with open(str(file_location), 'wb') as outfile:
                shutil.copyfileobj(response.raw, outfile)


def upload_images_to_s3(directory):
    """
    Upload images to S3 bucket if they end with png or jpg
    :param directory:
    :return: null
    """
    for f in directory.iterdir():
        if str(f).endswith(('.png', '.jpg', '.jpeg')):
            full_file_path = str(f.parent) + "/" + str(f.name)
            file_name = str(f.name)
            s3_client.upload_file(full_file_path, BASE_BUCKET, file_name)
            print(f,"put")


def upload_json_to_s3(directory):
    """
    Upload metadata json to directory
    :param directory:
    :return: null
    """
    for f in directory.iterdir():
        if str(f).endswith('.json'):
            full_file_path = str(f.parent) + "/" + str(f.name)
            file_name  = str(f.name)
            s3_client.upload_file(full_file_path, BASE_BUCKET, file_name)


def main():
    data = get_json()
    save_json(data)
    links = get_image_links(data)
    download_images(links)
    # upload_images_to_s3(ASSET_PATH)
    # upload_json_to_s3(ASSET_PATH)

if __name__ == '__main__':
   main()