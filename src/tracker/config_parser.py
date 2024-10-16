import yaml
import datetime as dt
from dataclasses import dataclass
from typing import List, Optional
from easydict import EasyDict

CONFIG_PATH = "config.yaml"

@dataclass
class TeamConfig:
    team: str
    start_date: dt.date
    end_date: dt.date
    distributed_learning: bool
    ignore_project_pattern: Optional[str] = None
    projects: Optional[List] = None

def parse_configs(config) -> dict:
    team_configs = []

    for company in config.companies:
        for team in company.teams:
            team_config = TeamConfig(
                team=team,
                start_date=__get_start_date(company.schedule),
                end_date=__get_end_date(company.schedule),
                ignore_project_pattern=company.get("ignore_project_pattern", None),
                distributed_learning=company.get("distributed_learning", False),
            )
            team_configs.append(team_config)
    return team_configs

def __get_start_date(company_schedule: List[EasyDict]) -> dt.date:
    dates = [dt.datetime.strptime(item.date, "%Y-%m-%d").date() for item in company_schedule]
    return min(dates)

def __get_end_date(company_schedule: List[EasyDict]) -> dt.date:
    sorted_schedule = sorted(company_schedule, key=lambda x: dt.datetime.strptime(x.date, "%Y-%m-%d").date())
    last_item = sorted_schedule[-1]
    last_date = dt.datetime.strptime(last_item.date, "%Y-%m-%d").date()
    
    if last_item.assigned_gpu_node == 0:
        return last_date
    else:
        return dt.date(2100, 1, 1)
