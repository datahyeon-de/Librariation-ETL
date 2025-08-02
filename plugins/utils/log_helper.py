from datetime import datetime
import logging
import os

def get_logger(
    name: str, log_dir: str, log_file: str,
    level: str = None, stream=False
    ) -> logging.Logger:
    
    # 로깅 레벨 매핑 딕셔너리
    level_mapping = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    # 로깅 레벨 결정
    if level is None:
        log_level = logging.INFO  # 기본값
        
    else:
        log_level = level_mapping.get(level.upper(), logging.INFO)
    
    os.makedirs(log_dir, exist_ok=True)
    
    # 로그 이름 생성
    logger = logging.getLogger(name)
    
    # 로그 레벨 설정
    logger.setLevel(log_level)
    
    # 포맷터 정의
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    
    # 파일 핸들러 생성
    fh = logging.FileHandler(f'{log_dir}/{log_file}', encoding='utf-8')
    fh.setFormatter(formatter)
    
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    
    logger.addHandler(fh)
    logger.addHandler(sh) if stream else None
    
    return logger
    