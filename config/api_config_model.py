from dataclasses import dataclass
from typing import Dict, List
from dotenv import load_dotenv
import os

@dataclass
class BASIC_APISpec:
    api_group: str
    api_id: str
    name: str
    base_url: str
    method: str
    auth_type: str
    required_params: List[str]
    field_mapping: Dict[str, str]
    timeout: int = 30
    retry_count: int = 3
    
    def get_api_keys(self) -> str:
        """
        환경변수에서 API 키를 가져옴
        """
        
        load_dotenv()
        
        env_key = f"{self.api_group}_key".upper()
        
        return os.getenv(env_key, "")
        