import yaml
from easydict import EasyDict

with open("config.yaml", "r") as f:
    CONFIG = EasyDict(yaml.safe_load(f))
