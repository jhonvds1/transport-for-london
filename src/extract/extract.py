import requests
import json


def extract_data():
    try:
        url = f"https://api.tfl.gov.uk/BikePoint"

        response = requests.get(url)

        data = response.json()

    except:
        ...

def run_extract():
    ...







