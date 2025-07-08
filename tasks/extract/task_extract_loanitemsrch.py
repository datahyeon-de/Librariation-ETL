import requests
import json
import math
import os

from concurrent.futures import ThreadPoolExecutor, as_completed


def extract_default_loan_item_data(base_url, params):
    
    response = requests.get(base_url, params=params)

    return response.json()

