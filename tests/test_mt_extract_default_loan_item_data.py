import os
import json
import pytest

from config.config_setting import APISettingManager
from tasks.extract.test_task_extract_loanitemsrch import mt_extract_default_loan_item_data
from utils.util import save_to_file


def test_unit_mt_extract_default_loan_item_data():
    api_manager = APISettingManager()
    base_url = api_manager.get_api_base_url("loanItemSrch")
    params = api_manager.get_api_params("data4library", "loanItemSrch")

    params["startDt"] = "2024-06-01"
    params["endDt"] = "2025-06-01"
    
    result = mt_extract_default_loan_item_data(base_url, params, mt_num=4, page_size=10, total_page=8)

    save_to_file(result, "/Users/datahyeon/Pratice/Librariation-ETL/tests/test_results/test_mt_extract_default_loan_item_data")
    
    assert isinstance(result, list)
    assert len(result) > 0
    print(f"✅ 총 {len(result)}건 수집됨")
