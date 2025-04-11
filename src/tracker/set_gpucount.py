from typing import Dict, Any, Tuple, Optional
from easydict import EasyDict
from datetime import datetime
import json
import re
import wandb

TEAM_CONFIGS = {
    "abeja-geniac": (("NUM_NODES", "trainer"), None),
    "aidealab-geniac": ("gpus", None),
    "aihub-geniac": (("nnodes", "trainer"), None),
    "aiinside-geniac": ("nnodes", None),
    "alt-geniac": ("NNODES", None),
    "datagrid-geniac": None,
    "eques-geniac": None,
    "future-geniac": ("world_size", None),
    "humanome-geniac": None,
    "jamstec-geniac": None,
    "karakuri-geniac": ("world_size", None),
    "kotoba-geniac": ("num_nodes", "num_gpus"),
    "nablas-geniac": ("num_nodes", "num_gpus_per_node"),
    "ricoh-geniac": ("NNODES", "NUM_GPUS"),
    "stockmark-geniac": None,
    "syntheticgestalt-geniac": None,
    "ubitus-geniac": None
}

def get_config_value(config: Dict[str, Any], key: str) -> int:
    """設定から値を取得し、整数に変換する"""
    if key == "NUM_NODES" and isinstance(config.get(key), (int, str)):
        try:
            return int(config[key])
        except (ValueError, TypeError):
            return 0

    keys = key.split('.')
    value = config
    for k in keys:
        if isinstance(value, dict):
            value = value.get(k, {})
        else:
            return 0
            
    if isinstance(value, dict) and 'value' in value:
        value = value['value']
        
    if isinstance(value, dict):
        value = value.get('num_nodes', 0)
        
    try:
        return int(value)
    except (ValueError, TypeError):
        print(f"Could not convert '{value}' to int. Using 0 instead.")
        return 0

def get_config_value_multi(config: Dict[str, Any], keys: Tuple[str, ...]) -> int:
    """複数のキーから最初に見つかった値を取得し、整数に変換する"""
    for key in keys:
        value = get_config_value(config, key)
        if value != 0:
            return value
    return 0

def calculate_gpu_count(num_nodes: int, gpu_key: Optional[str], config_dict: Dict[str, Any], team: str, node: EasyDict, run_path: str) -> int:
    """GPUカウントを計算する"""
    if team == "abeja-geniac":
        if "NUM_NODES" in config_dict:
            num_nodes = int(config_dict["NUM_NODES"])
        elif 'trainer' in config_dict and 'value' in config_dict['trainer']:
            num_nodes = config_dict['trainer']['value'].get('num_nodes', num_nodes)
        else:
            api = wandb.Api()
            run = api.run(run_path)
            file = run.file("wandb-metadata.json").download(replace=True)
            with open(file.name, 'r') as f:
                content = json.load(f)
                if 'slurm' in content and 'nnodes' in content['slurm']:
                    num_nodes = int(content['slurm']['nnodes'])
                else:
                    print(f"Warning: Could not find SLURM nnodes in metadata for {run_path}")
        return num_nodes * 8
    elif team == "alt-geniac" or team == "aiinside-geniac":
        return num_nodes * 8
    elif team == "ricoh-geniac":
        run_name = node.description
        match = re.match(r'(\d+)Node', run_name)
        if match:
            num_nodes = int(match.group(1))
        else:
            num_nodes = 1
        return num_nodes * 8
    elif team == "aidealab-geniac":
        summary_dict = json.loads(node.summaryMetrics)
        gpu_count = summary_dict.get("gpus", 0)
        return gpu_count
    elif team == "aihub-geniac":
        if "nnodes" in config_dict:
            try:
                num_nodes = int(config_dict["nnodes"])
                return num_nodes * 8
            except (ValueError, TypeError):
                pass
        elif isinstance(config_dict.get('trainer', {}).get('value', {}), dict):
            num_nodes = config_dict['trainer']['value'].get('nnodes', num_nodes)
            return int(num_nodes) * 8
        else:
            num_nodes_str = node.description if node else "0Node"
            num_gpus = node.runInfo.gpuCount if node.runInfo else 0
            if num_nodes_str and num_nodes_str[-5].isdigit() and num_nodes_str.endswith("Node"):
                num_nodes = int(num_nodes_str[-5])
            else:
                num_nodes = 0
        return num_nodes * num_gpus
    elif gpu_key:
        num_gpus = get_config_value(config_dict, gpu_key)
        if num_gpus == 0:
            print(f"num_gpus is 0 for {team} ({node.name}). Using num_nodes as GPU count.")
            return num_nodes
        return num_nodes * num_gpus
    else:
        return num_nodes

def set_gpucount(node: EasyDict, team: str, run_path: str, createdAt, updatedAt) -> int:
    """チームごとのGPUカウントを設定する"""
    default_gpu_count = node.runInfo.gpuCount if node.runInfo else 0
    
    if team == "syntheticgestalt-geniac":
        if default_gpu_count < 8 or createdAt < datetime.fromisoformat("2025-02-17"):
            gpu_count = default_gpu_count
        else:
            num_nodes_str = node.description if node else "0Nodes"
            if num_nodes_str[-6].isdigit() and num_nodes_str.endswith("Nodes"):
                num_nodes = int(num_nodes_str[-6])
            else:
                num_nodes = 3
            gpu_count = num_nodes * 8
        return gpu_count
    
    elif team == "datagrid-geniac":
        if node.runInfo and not node.runInfo.gpu:
            return 8
        else:
            pass

    if team not in TEAM_CONFIGS:
        print(f"Unknown team {team}. Using default GPU count.")
        return default_gpu_count

    config_dict = json.loads(node.config)
    
    try:
        team_config = TEAM_CONFIGS[team]
        if team_config is None:
            return default_gpu_count
        
        node_key, gpu_key = team_config
        num_nodes = get_config_value_multi(config_dict, node_key) if isinstance(node_key, tuple) else get_config_value(config_dict, node_key)
        
        if num_nodes == 0:
            print(f"num_nodes is 0 for {team} ({node.name}).")
        
        gpu_count = calculate_gpu_count(num_nodes, gpu_key, config_dict, team, node, run_path)
        
        print(f"Calculated GPU count for {team} ({node.name}): {gpu_count}")
        return gpu_count if gpu_count > 0 else default_gpu_count
    
    except Exception as e:
        print(f"Error calculating GPU count for {team} ({node.name}): {str(e)}")
        return default_gpu_count