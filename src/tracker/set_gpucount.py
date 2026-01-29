from typing import Dict, Any, Union, Tuple, Optional
from easydict import EasyDict
import json
import logging
import re

# ロギングの設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Governance & Methodology Layer (True Compute vs Fake Busy) ---
# TEAM_CONFIGS is not just configuration - it defines the governance rules for GPU counting.
# Core Methodology:
# 1. True Compute: 
#    - High-density computation with clear information gain.
#    - Config pattern: explicit num_nodes * num_gpus per node.
#    - Sustained high utilization is expected and valid.
# 2. Fake Busy:
#    - Complex workflow but low compute value (e.g., Agent Fan-out, duplicate Runs).
#    - Symptom: High per-run utilization but N^2 explosion in run count.
#    - Governance: Force key to None, fallback to per-run mode, avoid double counting.
#
# Case Study: Airion (2025-08-14)
# - Symptom: GPU utilization spiked dramatically.
# - Cause: Switched from single-node to multi-node, but config was set to ("world_size", None).
#          Each node spawned a Run, causing N nodes to be counted as N*N.
# - Ruling: Force to None. Acknowledge utilization growth as valid business, but enforce de-duplication.

TEAM_CONFIGS: Dict[str, Optional[Tuple[Union[str, Tuple[str, ...]], Optional[str]]]] = {
    # --- Standard Group (Default to WandB GPU Count) ---
    "stockmark-geniac": None,
    "datagrid-geniac": None,
    "humanome-geniac": None,
    "eques-geniac": None,
    "aidealab-geniac": None,
    "aihub-geniac": None,
    "aiinside-geniac": None,
    "future-geniac": None,
    "ubitus-geniac": None,
    "jamstec-geniac": None,
    "zenintelligence-geniac": None,
    "onestruction-geniac": None,
    "nri-geniac": None,
    "nishika-org": None,
    "sdio-geniac": None,
    "nexascience-geniac": None,
    "degas-geniac": None,
    "premedi-geniac": None, # Waiting for User Split logic (Kamata)
    
    # --- Governance: Forced Correction Group ---
    "airion-geniac": None, # [Governance] Force None to avoid N^2 inflation on multi-node.

    # --- Explicit Config Group (Trusted Compute Contexts) ---
    "kotoba-geniac": ("num_nodes", "num_gpus"),
    "syntheticgestalt-geniac": None,
    "karakuri-geniac": ("world size", None),
    "abeja-geniac": (("NUM_NODES", "trainer.num_nodes"), None),
    "alt-geniac": ("NNODES", None),
    "ricoh-geniac": ("NNODES", "NUM_GPUS"),
    "nablas-geniac": ("num_nodes", "num_gpus_per_node"),
}

def get_config_value(config: Dict[str, Any], key: str) -> int:
    """Extract integer value from config"""
    # WandB Config structure: { "key": { "value": <data>, "desc": ... } }
    value = config.get(key, {}).get('value', 0)
    try:
        return int(value)
    except (ValueError, TypeError):
        # logging.warning(f"Could not convert '{value}' to int. Using 0 instead.")
        return 0

def get_config_value_multi(config: Dict[str, Any], keys: tuple) -> int:
    """Try multiple keys to find a valid integer value from config"""
    for key in keys:
        value = get_config_value(config, key)
        if value != 0:
            return value
    return 0

def set_gpucount(node: EasyDict, team: str) -> int:
    """
    Core Logic: Calculate 'True Compute' GPU Count for a given Run.
    
    This function acts as the 'Judge'. It decides how many GPUs a run *actually* represents,
    filtering out configuration noise and enforcing the governance rules defined in TEAM_CONFIGS.
    """
    # 1. Default Baseline (WandB's reported GPU count)
    default_gpu_count = node.runInfo.gpuCount if node.runInfo else 0
    
    # 2. Check Governance Rules
    if team not in TEAM_CONFIGS:
        # Unknown team -> Fallback to raw data (Safe default)
        # logger.warning(f"Unknown team {team}. Using default GPU count.")
        return default_gpu_count

    # 3. Parse Config (Control Plane Data)
    try:
        config_dict = json.loads(node.config)
    except Exception:
        config_dict = {}
    
    try:
        team_config = TEAM_CONFIGS[team]
        
        # [Governance] If config is None, we explicitly trust the per-run report (avoiding duplication)
        if team_config is None:
            return default_gpu_count
        
        node_key, gpu_key = team_config
        
        # 4. Extract Node Count (The Multiplier)
        if isinstance(node_key, tuple):
            num_nodes = get_config_value_multi(config_dict, node_key)
        else:
            num_nodes = get_config_value(config_dict, node_key)
        
        # 5. Fallback Logic for Zero Nodes
        if num_nodes == 0:
            # Special Case: Ricoh (Extract from Run Name)
            # [Governance] Ricoh's discipline is to put node count in the run name.
            if team == "ricoh-geniac": 
                m = re.search(r"(\d+)\s*Node", node.name, re.IGNORECASE)
                per_node = default_gpu_count or 8
                nodes = int(m.group(1)) if m else 1
                return per_node * nodes
            
            # Default fallback
            # logger.warning(f"num_nodes is 0 for {team} ({node.name}). Using default GPU count.")
            return default_gpu_count

        # 6. Calculate Final Count based on Team Archetype
        if team == "karakuri-geniac":
            # Karakuri uses 'world size' which usually means Total GPUs, not Nodes.
            # But here logic implies node count? Need to verify if this logic holds.
            gpu_count = num_nodes
        elif team in ["abeja-geniac", "alt-geniac", "kotoba-geniac"]:
            # Standard Multi-Node: Nodes * 8 (Standard A100/H100 Node)
            gpu_count = num_nodes * 8
        elif gpu_key:
            # Explicit Per-Node GPU Count in config
            num_gpus = get_config_value(config_dict, gpu_key)
            if num_gpus == 0:
                return default_gpu_count
            gpu_count = num_nodes * num_gpus
        else:
            # Default multiplier
            gpu_count = num_nodes
        
        # logger.info(f"Calculated GPU count for {team} ({node.name}): {gpu_count}")
        return gpu_count if gpu_count > 0 else default_gpu_count

    except Exception as e:
        logger.error(f"Error calculating GPU count for {team} ({node.name}): {str(e)}")
        return default_gpu_count
