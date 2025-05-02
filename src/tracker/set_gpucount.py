from typing import Dict, Any, Tuple, Optional, Callable, List, Union
from easydict import EasyDict
from datetime import datetime
import json
import re
import wandb
from abc import ABC, abstractmethod

class TeamGpuCounter(ABC):
    """チームごとのGPUカウント計算を行う基底クラス"""
    
    def __init__(self, team_name: str):
        self.team_name = team_name
    
    def calculate_gpu_count(self, node: EasyDict, run_path: str, created_at: datetime, updated_at: datetime) -> int:
        """GPUカウントを計算する"""
        default_gpu_count = node.runInfo.gpuCount if node.runInfo else 0
        
        try:
            config_dict = json.loads(node.config)
            gpu_count = self._calculate_gpu_count_impl(node, config_dict, run_path, created_at, updated_at)
            return gpu_count if gpu_count > 0 else default_gpu_count
        except Exception as e:
            print(f"Error calculating GPU count for {self.team_name} ({node.name}): {str(e)}")
            return default_gpu_count
    
    @abstractmethod
    def _calculate_gpu_count_impl(self, node: EasyDict, config_dict: Dict[str, Any], 
                                 run_path: str, created_at: datetime, updated_at: datetime) -> int:
        """チーム固有のGPUカウント計算ロジックを実装する"""
        pass
    
    def get_config_value(self, config: Dict[str, Any], key: str) -> int:
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
    
    def get_config_value_multi(self, config: Dict[str, Any], keys: Tuple[str, ...]) -> int:
        """複数のキーから最初に見つかった値を取得し、整数に変換する"""
        for key in keys:
            value = self.get_config_value(config, key)
            if value != 0:
                return value
        return 0


class DefaultTeamGpuCounter(TeamGpuCounter):
    """デフォルトのGPUカウント計算クラス"""
    
    def _calculate_gpu_count_impl(self, node: EasyDict, config_dict: Dict[str, Any], 
                                 run_path: str, created_at: datetime, updated_at: datetime) -> int:
        return node.runInfo.gpuCount if node.runInfo else 0


class SimpleNodeGpuCounter(TeamGpuCounter):
    """ノード数 × 8 でGPUカウントを計算するクラス"""
    
    def __init__(self, team_name: str, node_key: Union[str, Tuple[str, ...]]):
        super().__init__(team_name)
        self.node_key = node_key
    
    def _calculate_gpu_count_impl(self, node: EasyDict, config_dict: Dict[str, Any], 
                                 run_path: str, created_at: datetime, updated_at: datetime) -> int:
        if isinstance(self.node_key, tuple):
            num_nodes = self.get_config_value_multi(config_dict, self.node_key)
        else:
            num_nodes = self.get_config_value(config_dict, self.node_key)
        
        if num_nodes == 0:
            print(f"num_nodes is 0 for {self.team_name} ({node.name}).")
            
        return num_nodes * 8


class NodeGpuMultiplierCounter(TeamGpuCounter):
    """ノード数 × GPUs/ノード でGPUカウントを計算するクラス"""
    
    def __init__(self, team_name: str, node_key: Union[str, Tuple[str, ...]], gpu_key: str):
        super().__init__(team_name)
        self.node_key = node_key
        self.gpu_key = gpu_key
    
    def _calculate_gpu_count_impl(self, node: EasyDict, config_dict: Dict[str, Any], 
                                 run_path: str, created_at: datetime, updated_at: datetime) -> int:
        if isinstance(self.node_key, tuple):
            num_nodes = self.get_config_value_multi(config_dict, self.node_key)
        else:
            num_nodes = self.get_config_value(config_dict, self.node_key)
        
        num_gpus = self.get_config_value(config_dict, self.gpu_key)
        
        if num_nodes == 0:
            print(f"num_nodes is 0 for {self.team_name} ({node.name}).")
        
        if num_gpus == 0:
            print(f"num_gpus is 0 for {self.team_name} ({node.name}). Using num_nodes as GPU count.")
            return num_nodes
            
        return num_nodes * num_gpus


class AbejaGpuCounter(TeamGpuCounter):
    """ABEJA用のGPUカウント計算クラス"""
    
    def _calculate_gpu_count_impl(self, node: EasyDict, config_dict: Dict[str, Any], 
                                 run_path: str, created_at: datetime, updated_at: datetime) -> int:
        num_nodes = 0
        if "NUM_NODES" in config_dict:
            num_nodes = int(config_dict["NUM_NODES"])
        elif 'trainer' in config_dict and 'value' in config_dict['trainer']:
            num_nodes = config_dict['trainer']['value'].get('num_nodes', 0)
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


class RicohGpuCounter(TeamGpuCounter):
    """RICOH用のGPUカウント計算クラス"""
    
    def _calculate_gpu_count_impl(self, node: EasyDict, config_dict: Dict[str, Any], 
                                 run_path: str, created_at: datetime, updated_at: datetime) -> int:
        run_name = node.description
        match = re.match(r'(\d+)Node', run_name)
        if match:
            num_nodes = int(match.group(1))
        else:
            num_nodes = 1
        return num_nodes * 8


class AidealabGpuCounter(TeamGpuCounter):
    """AIDEALAB用のGPUカウント計算クラス"""
    
    def _calculate_gpu_count_impl(self, node: EasyDict, config_dict: Dict[str, Any], 
                                 run_path: str, created_at: datetime, updated_at: datetime) -> int:
        summary_dict = json.loads(node.summaryMetrics)
        gpu_count = summary_dict.get("gpus", 0)
        return gpu_count


class AihubGpuCounter(TeamGpuCounter):
    """AIHUB用のGPUカウント計算クラス"""
    
    def _calculate_gpu_count_impl(self, node: EasyDict, config_dict: Dict[str, Any], 
                                 run_path: str, created_at: datetime, updated_at: datetime) -> int:
        if "nnodes" in config_dict:
            try:
                num_nodes = int(config_dict["nnodes"])
                return num_nodes * 8
            except (ValueError, TypeError):
                pass
        elif isinstance(config_dict.get('trainer', {}).get('value', {}), dict):
            num_nodes = config_dict['trainer']['value'].get('nnodes', 0)
            return int(num_nodes) * 8
        else:
            num_nodes_str = node.description if node else "0Node"
            num_gpus = node.runInfo.gpuCount if node.runInfo else 0
            if num_nodes_str and num_nodes_str[-5].isdigit() and num_nodes_str.endswith("Node"):
                num_nodes = int(num_nodes_str[-5])
            else:
                num_nodes = 0
        return num_nodes * num_gpus


class SyntheticGestaltGpuCounter(TeamGpuCounter):
    """SyntheticGestalt用のGPUカウント計算クラス"""
    
    def _calculate_gpu_count_impl(self, node: EasyDict, config_dict: Dict[str, Any], 
                                 run_path: str, created_at: datetime, updated_at: datetime) -> int:
        default_gpu_count = node.runInfo.gpuCount if node.runInfo else 0
        
        if default_gpu_count < 8 or created_at < datetime.fromisoformat("2025-02-17"):
            gpu_count = default_gpu_count
        else:
            num_nodes_str = node.description if node else "0Nodes"
            if num_nodes_str[-6].isdigit() and num_nodes_str.endswith("Nodes"):
                num_nodes = int(num_nodes_str[-6])
            else:
                num_nodes = 3
            gpu_count = num_nodes * 8
        return gpu_count


class DatagridGpuCounter(TeamGpuCounter):
    """Datagrid用のGPUカウント計算クラス"""
    
    def _calculate_gpu_count_impl(self, node: EasyDict, config_dict: Dict[str, Any], 
                                 run_path: str, created_at: datetime, updated_at: datetime) -> int:
        if node.runInfo and not node.runInfo.gpu:
            return 8
        else:
            return node.runInfo.gpuCount if node.runInfo else 0


class FutureGpuCounter(TeamGpuCounter):
    """Future用のGPUカウント計算クラス"""
    
    def _calculate_gpu_count_impl(self, node: EasyDict, config_dict: Dict[str, Any], 
                                 run_path: str, created_at: datetime, updated_at: datetime) -> int:
        # world_sizeがあればそれをそのままGPU数として使用
        world_size = self.get_config_value(config_dict, "world_size")
        
        if world_size > 0:
            return world_size
        else:
            # world_sizeがなければデフォルトのGPU数を使用
            return node.runInfo.gpuCount if node.runInfo else 0

class GpuCounterFactory:
    """チーム名に基づいてGPUカウンタを生成するファクトリクラス"""
    
    @staticmethod
    def create_counter(team: str) -> TeamGpuCounter:
        """チーム名に基づいて適切なGPUカウンタを生成する"""
        if team == "abeja-geniac":
            return AbejaGpuCounter(team)
        elif team == "aidealab-geniac":
            return AidealabGpuCounter(team)
        elif team == "aihub-geniac":
            return AihubGpuCounter(team)
        elif team == "aiinside-geniac":
            return SimpleNodeGpuCounter(team, "nnodes")
        elif team == "alt-geniac":
            return SimpleNodeGpuCounter(team, "NNODES")
        elif team == "datagrid-geniac":
            return DatagridGpuCounter(team)
        elif team == "future-geniac":
            return FutureGpuCounter(team)
        elif team == "kotoba-geniac":
            return NodeGpuMultiplierCounter(team, "num_nodes", "num_gpus")
        elif team == "nablas-geniac":
            return NodeGpuMultiplierCounter(team, "num_nodes", "num_gpus_per_node")
        elif team == "ricoh-geniac":
            return RicohGpuCounter(team)
        elif team == "syntheticgestalt-geniac":
            return SyntheticGestaltGpuCounter(team)
        else:
            # eques-geniac, humanome-geniac, jamstec-geniac, stockmark-geniac, ubitus-geniac
            return DefaultTeamGpuCounter(team)


def set_gpucount(node: EasyDict, team: str, run_path: str, created_at: datetime, updated_at: datetime) -> int:
    """チームごとのGPUカウントを設定する"""
    counter = GpuCounterFactory.create_counter(team)
    gpu_count = counter.calculate_gpu_count(node, run_path, created_at, updated_at)
    print(f"Calculated GPU count for {team} ({node.name}): {gpu_count}")
    return gpu_count