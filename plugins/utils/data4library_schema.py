from typing import Dict, Tuple, Optional
import re
from datetime import datetime

class Data4LibraryLoanItemValidator:
    REQUIRED_FIELDS = [
        'id', 'startDt', 'endDt', 'age',
    ]
    INT_FIELDS = ['ranking', 'loan_count']
    DATE_FIELDS = ['startDt', 'endDt']
    MAX_LENGTHS = {
        'id': 128,
        'age': 4,
        'bookname': 255,
        'authors': 255,
        'publisher': 255,
        'publication_year': 8,
        'isbn13': 32,
        'addition_symbol': 32,
        'vol': 32,
        'class_no': 32,
        'class_nm': 255,
        'bookImageURL': 512,
        'bookDtlUrl': 512,
    }

    # ✅ 새로운 ID 포맷 정규식 (yyyymm_age_isbn13_[추가심볼]_[vol])
    ID_PATTERN = re.compile(r'^[0-9]{6}_[0-9]{1,3}_[0-9Xx]{10,13}(?:_[^_]+){0,2}$')

    @classmethod
    def validate_row(cls, row: Dict) -> Tuple[bool, Optional[str]]:
        # 필수 컬럼 체크
        for field in cls.REQUIRED_FIELDS:
            if not row.get(field):
                return False, f"필수값 누락: {field}"

        # 정수 타입 체크
        for field in cls.INT_FIELDS:
            if row.get(field) is not None:
                try:
                    int(row[field])
                except Exception:
                    return False, f"정수형 변환 실패: {field}={row[field]}"

        # 날짜 포맷 체크
        for field in cls.DATE_FIELDS:
            if row.get(field):
                try:
                    datetime.strptime(row[field], '%Y-%m-%d')
                except Exception:
                    return False, f"날짜 포맷 오류: {field}={row[field]}"
        
        # 길이 체크
        for field, maxlen in cls.MAX_LENGTHS.items():
            if row.get(field) and len(str(row[field])) > maxlen:
                return False, f"{field} 길이 초과: {row[field]}"

        # ✅ isbn13 필드 존재 및 형식 검사
        isbn = row.get("isbn13")
        if not isbn or not isbn.strip():
            return False, "isbn13 누락 또는 공백"
        
        # ✅ ID 정규식 체크
        if not cls.ID_PATTERN.match(row['id']):
            return False, f"id 포맷 오류: {row['id']}"

        return True, None
