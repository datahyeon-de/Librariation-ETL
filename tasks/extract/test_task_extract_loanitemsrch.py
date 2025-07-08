import requests
import json
import math
import os

from concurrent.futures import ThreadPoolExecutor, as_completed


def extract_default_loan_item_data(base_url, params):
    response = requests.get(base_url, params=params)

    return response.json()



def mt_extract_default_loan_item_data(base_url, params, mt_num=4, page_size=None, total_page=None):
    response = extract_default_loan_item_data(base_url, params)
    all_docs = response["response"]["docs"]
    
    total_count = response['response']['resultNum']
    page_size = page_size or int(params['pageSize'])
    total_page = total_page or math.ceil(total_count / page_size)
    
    def fetch_page(page_no):
        local_params = params.copy()
        local_params['pageNo'] = str(page_no)

        mt_response = extract_default_loan_item_data(base_url, local_params)
        mt_result = mt_response['response']['docs']
        
        return mt_result
    
    with ThreadPoolExecutor(max_workers=mt_num) as executor:
        page_to_future = {
            p: executor.submit(fetch_page, p) for p in range(2, total_page + 1)
        }
        
        for page_no, future in page_to_future.items():
            try:
                all_docs.extend(future.result())
            except Exception as e:
                print(f"Error fetching page {page_no}: {e}")
                continue
        
    return all_docs

    


