import json

def replace_null(json_data):
    if isinstance(json_data, dict):
        for key, value in json_data.items():
            if value is None:
                json_data[key] = "Not Supported"
            else:
                replace_null(value)
    elif isinstance(json_data, list):
        for i in range(len(json_data)):
            if json_data[i] is None:
                json_data[i] = "Not Supported"
            else:
                replace_null(json_data[i])

# JSON 파일 경로
PATH = "/Users/kimdohoon/desktop/253_players.json"

# JSON 파일 로드
with open(PATH, "r") as file:
    data = json.load(file)

# null 값 대체
replace_null(data)

# 대체된 JSON 파일 저장
with open(PATH, "w") as file:
    json.dump(data, file, indent=4)
