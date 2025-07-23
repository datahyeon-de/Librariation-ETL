from datetime import datetime

import logging
import os

def get_logger(name: str, log_dir: str, ds_nodash: str, level: int = logging.INFO) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, f"{name}_{ds_nodash}.log")
    logger = logging.getLogger(f"{name}_{ds_nodash}")
    logger.setLevel(level)
    # 핸들러 중복 방지: 파일핸들러가 이미 같은 파일을 바라보는지 체크
    if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == os.path.abspath(log_path) for h in logger.handlers):
        formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
        fh = logging.FileHandler(log_path)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger