import yaml
from easydict import EasyDict

import os

# 設定ファイルの読み込み
config_path = os.getenv("CONFIG_FILE", "config.yaml")
try:
    with open(config_path, "r") as f:
        CONFIG = EasyDict(yaml.safe_load(f))
except FileNotFoundError:
    print("Warning: config.yaml file not found.")
    raise