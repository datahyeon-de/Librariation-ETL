import os
import json
from dotenv import load_dotenv

class APISettingManager:
    def __init__(self, env_path="config/.env", json_path="config/api_setting.json"):
        """
        API 설정 관리자 초기화
        """
        self.env_path = env_path
        self.json_path = json_path
        self._load_env()
        self._load_api_info()
    
    def _load_env(self):
        """환경변수 파일 로드"""
        load_dotenv(self.env_path)
        self.env = dict(os.environ)
    
    def _load_api_info(self):
        """API 정보 JSON 파일 로드"""
        with open(self.json_path, "r", encoding="utf-8") as f:
            self.api_info = json.load(f)
    
    def get_api_base_url(self, api_id):
        """
        API 그룹을 받아서 환경변수에서 API 키를 주입한 파라미터 반환
        """
        return self.api_info[api_id]["base_url"]
    
    def get_api_params(self, api_group, api_id):
        """
        API 그룹을 받아서 환경변수에서 API 키를 주입한 파라미터 반환
        """
        
        # API 키 가져오기
        api_key = self.env.get(f"{api_group}_key".upper(), "")
        
        # 파라미터 복사 후 API 키 주입
        params = self.api_info[api_id]["params"].copy()
        params["authKey"] = api_key
        
        return params
    
class DBSettingManager:
    def __init__(self, env_path="config/.env", json_path="config/db_setting.json"):
        """
        DB 설정 관리자 초기화
        """
        self.env_path = env_path
        self.json_path = json_path
        self._load_env()
        self._load_db_info()
    
    def _load_env(self):
        """환경변수 파일 로드"""
        load_dotenv(self.env_path)
        self.env = dict(os.environ)
    
    def _load_db_info(self):
        """DB 정보 JSON 파일 로드 (옵션: db_setting.json이 없으면 무시)"""
        try:
            with open(self.json_path, "r", encoding="utf-8") as f:
                self.db_info = json.load(f)
        except FileNotFoundError:
            self.db_info = {}

    def get_db_config(self):
        """
        .env 파일에서 DB 접속 정보를 딕셔너리로 반환
        """
        db_config = {
            "host": self.env.get("MYSQL_HOST", "localhost"),
            "port": int(self.env.get("MYSQL_PORT", 3306)),
            "database": self.env.get("MYSQL_DATABASE", ""),
            "user": self.env.get("MYSQL_USER", ""),
            "password": self.env.get("MYSQL_PASSWORD", ""),
            "root_password": self.env.get("MYSQL_ROOT_PASSWORD", "")
        }
        return db_config