import re

def extract_authors(text):
    if not text:
        return ""
    # 1. 제거할 키워드 및 괄호 제거
    text = re.sub(r'\(.*?\)', '', text)  # 괄호 안 제거
    text = re.sub(r'(옮김|옮긴이|감수|기획|정보글|편집|역)\s*[:：]?\s*\w+', '', text)

    # 2. 저자 키워드 제거
    text = re.sub(r'(지은이|글·그림|글|그림|저자|원작|지음|쓰고 그림)\s*[:：]?\s*', '', text)

    # 3. 구분자 통일
    text = re.sub(r'[;,|]', ',', text)
    
    # 4. 공백 정리 및 중복 제거
    names = [n.strip() for n in text.split(',') if n.strip()]
    return ', '.join(sorted(set(names))) 