import json
from dataclasses import dataclass
from typing import List

from ..tracker.run_manager import RunManager
from ..uploader.artifact_handler import ArtifactHandler
from ..utils.config import CONFIG

@dataclass
class BlacklistRow:
    run_path: str
    tags: List[str]

def update_blacklist(date_range) -> None:
    blacklist = create_blacklist(date_range)
    ArtifactHandler.upload_blacklist([b.__dict__ for b in blacklist])

def create_blacklist(date_range) -> List[BlacklistRow]:
    run_manager = RunManager(date_range)
    all_runs = run_manager.fetch_runs()

    print("Update blacklist of runs ...")
    blacklist = []
    for run in all_runs.iter_rows(named=True):
        tags = json.loads(run['tags'])  # JSON文字列をリストに戻す
        if not set(CONFIG.ignore_tags).isdisjoint([t.lower() for t in run['tags']]):
            if 'tags' in run and not set(CONFIG.ignore_tags).isdisjoint([t.lower() for t in run['tags']]):
                blacklist.append(BlacklistRow(run_path=f"{run['company_name']}/{run['project']}/{run['run_id']}", tags=run['tags']))

    return blacklist

if __name__ == "__main__":
    update_blacklist()