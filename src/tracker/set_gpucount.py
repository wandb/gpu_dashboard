from typing import Dict, Any
from easydict import EasyDict
import json
import logging

# ロギングの設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 定数
TEAM_CONFIGS = {
    "stockmark-geniac": None,
    "datagrid-geniac": None,
    "kotoba-geniac": ("num_nodes", "num_gpus"),
    "syntheticgestalt-geniac": None,
    "humanome-geniac": None,
    "eques-geniac": None,
    "karakuri-geniac": ("world size", None),
    "aidealab-geniac": None,
    "aihub-geniac": None,
    "abeja-geniac": (("NUM_NODES", "trainer.num_nodes"), None),
    "alt-geniac": ("NNODES", None),
    "ricoh-geniac": ("NNODES", "NUM_GPUS"),
    "aiinside-geniac": None,
    "future-geniac": None,
    "ubitus-geniac": None,
    "nablas-geniac": ("num_nodes", "num_gpus_per_node"),
    "jamstec-geniac": None,
}

def get_config_value(config: Dict[str, Any], key: str) -> int:
    """設定から値を取得し、整数に変換する"""
    value = config.get(key, {}).get('value', 0)
    try:
        return int(value)
    except (ValueError, TypeError):
        logger.warning(f"Could not convert '{value}' to int. Using 0 instead.")
        return 0

def get_config_value_multi(config: Dict[str, Any], keys: tuple) -> int:
    """複数のキーから最初に見つかった値を取得し、整数に変換する"""
    for key in keys:
        value = get_config_value(config, key)
        if value != 0:
            return value
    return 0

def set_gpucount(node: EasyDict, team: str) -> int:
    """チームごとのGPUカウントを設定する"""
    # デフォルト値の処理
    gpu_count = node.runInfo.gpuCount if node.runInfo else 0
    
    if team not in TEAM_CONFIGS:
        logger.warning(f"Unknown team {team}. Using default GPU count.")
        return gpu_count

    config_dict = json.loads(node.config)
    
    try:
        team_config = TEAM_CONFIGS[team]
        if team_config is None:
            return gpu_count
        
        node_key, gpu_key = team_config
        if isinstance(node_key, tuple):
            num_nodes = get_config_value_multi(config_dict, node_key)
        else:
            num_nodes = get_config_value(config_dict, node_key)
        
        if team in ["abeja-geniac", "alt-geniac"]:
            num_gpus = 8
        elif gpu_key:
            num_gpus = get_config_value(config_dict, gpu_key)
        else:
            num_gpus = 1  # デフォルト値として1を使用
        
        gpu_count = num_nodes * num_gpus
        
        logger.info(f"Calculated GPU count for {team} ({node.name}): {gpu_count}")
    except Exception as e:
        logger.error(f"Error calculating GPU count for {team} ({node.name}): {str(e)}")
    
    return gpu_count