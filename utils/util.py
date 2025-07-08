import json
import csv
import os
import yaml
import pandas as pd

def save_to_file(data, path: str, format: str = "json"):
    format = format.lower()
    file_path = f"{path}.{format}"

    # 유효성 검사 함수
    def validate_format(data, format):
        if format == "json":
            if not isinstance(data, (dict, list)):
                raise TypeError("JSON 저장은 dict 또는 list만 지원됩니다.")
        elif format == "csv":
            if isinstance(data, dict):
                return
            if isinstance(data, list) and all(isinstance(row, dict) for row in data):
                return
            raise TypeError("CSV 저장은 dict 또는 dict의 list만 지원됩니다.")
        elif format == "yaml":
            if not isinstance(data, (dict, list)):
                raise TypeError("YAML 저장은 dict 또는 list만 지원됩니다.")
        elif format == "txt":
            if not isinstance(data, (str, dict, list, set, tuple)):
                raise TypeError("TXT 저장은 str, list, set, dict, tuple만 지원됩니다.")
        elif format == "dot":
            if not isinstance(data, dict):
                raise TypeError("DOT 저장은 dict 형태만 지원됩니다.")
        elif format == "excel":
            if not isinstance(data, pd.DataFrame):
                raise TypeError("EXCEL 저장은 pandas.DataFrame만 지원됩니다.")
        else:
            raise ValueError(f"지원하지 않는 포맷입니다: {format}")

    # validate
    validate_format(data, format)

    # 저장 로직
    if format == "json":
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    elif format == "csv":
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if isinstance(data, dict):
                writer.writerow(data.keys())
                writer.writerow(data.values())
            elif isinstance(data, list):
                writer.writerow(data[0].keys())
                for row in data:
                    writer.writerow(row.values())

    elif format == "yaml":
        with open(file_path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, allow_unicode=True, indent=2)

    elif format == "txt":
        with open(file_path, "w", encoding="utf-8") as f:
            if isinstance(data, (list, set, tuple)):
                for item in data:
                    f.write(str(item) + "\n")
            elif isinstance(data, dict):
                for k, v in data.items():
                    f.write(f"{k}: {v}\n")
            else:
                f.write(str(data))

    elif format == "dot":
        with open(file_path, "w", encoding="utf-8") as f:
            f.write("digraph G {\n")
            for k, v in data.items():
                f.write(f'    "{k}" -> "{v}";\n')
            f.write("}")

    elif format == "excel":
        data.to_excel(file_path, index=False)

    print(f"✅ 저장 완료: {file_path}")