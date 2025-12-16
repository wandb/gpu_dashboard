import yaml
from easydict import EasyDict

# 設定ファイルの読み込み
try:
    with open("config.yaml", "r") as f:
        CONFIG = EasyDict(yaml.safe_load(f))
except FileNotFoundError:
    print("Warning: config.yaml file not found.")
    raise