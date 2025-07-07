def transform_default_loan_item_data(**kwargs):
    ti = kwargs["ti"]
    response = ti.xcom_pull(task_ids="extract_default_loan_item_data")
    docs = response["response"]["docs"]
    start_dt = response["response"]["request"]["startDt"]
    end_dt = response["response"]["request"].get("endDt", start_dt)

    # 컬럼 순서에 맞는 튜플로 변환
    result = []
    for item in docs:
        doc = item["doc"]
        row = (
            int(doc["no"]),
            int(doc["ranking"]),
            doc["bookname"],
            doc["authors"],
            doc["publisher"],
            int(doc["publication_year"]),
            doc["isbn13"],
            doc["addition_symbol"],
            doc["vol"],
            doc["class_no"],
            doc["class_nm"],
            doc["bookImageURL"],
            doc["bookDtlUrl"],
            int(doc["loan_count"]),
            start_dt,
            end_dt,
        )
        result.append(row)
    
    return result