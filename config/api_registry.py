from api_config_model import BASIC_APISpec

class Data4Library_APIRegistry:
    
    LoanItemSrch = BASIC_APISpec(
        api_group = "data4library",
        api_id="LoanItemSrch",
        name="인기대출도서 조회",
        base_url="http://data4library.kr/api/loanItemSrch",
        method="GET",
        auth_type="api_key",
        required_params=["authKey"],
        field_mapping={
            "authKey": "",
            "startDt": "",
            "endDt": "",
            "gender": "",
            "from_age": "",
            "to_age": "",
            "age": "",
            "region": "",
            "dtl_region": "",
            "book_dvsn": "",
            "addCode": "",
            "kdc": "",
            "dtl_kdc": "",
            "pageNo": "1",
            "pageSize": "200",
            "format": "json",
        }
    )
        
    HotTrend = BASIC_APISpec(
        api_group = "data4library",
        api_id="HotTrend",
        name="대출 급상승 도서",
        base_url="http://data4library.kr/api/hotTrend",
        method="GET",
        auth_type="api_key",
        required_params=["authKey", "searchDt"],
        field_mapping={
            "authKey": "",
            "searchDt": "",
            "format": "json",
        }
    )
    