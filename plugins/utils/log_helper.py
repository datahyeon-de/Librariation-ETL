from datetime import datetime
import logging
import os

def get_logger(name: str, log_dir: str, ds_nodash: str, level: str = None, log_file: str = None, stream=False) -> logging.Logger:
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
    if log_file is not None:
        log_path = log_file
    else:
        log_path = os.path.join(log_dir, f"{name}.log")
    logger = logging.getLogger(f"{name}_{ds_nodash}")
    logger.setLevel(log_level)
    
    # 포맷터 정의
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    
    # 핸들러 중복 방지: 파일핸들러가 이미 같은 파일을 바라보는지 체크
    if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == os.path.abspath(log_path) for h in logger.handlers):
        fh = logging.FileHandler(log_path)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    
    if stream:
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        
    return logger