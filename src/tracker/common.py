import pytz
import datetime as dt
import polars as pl
from dataclasses import dataclass, field

JAPAN_TIMEZONE = pytz.timezone("Asia/Tokyo")
LOGGED_AT = dt.datetime.now(JAPAN_TIMEZONE).replace(tzinfo=None)
JAPAN_UTC_OFFSET = 9

GQL_QUERY = """
query GetGpuInfoForProject($project: String!, $entity: String!, $first: Int!, $cursor: String!) {
    project(name: $project, entityName: $entity) {
        name
        runs(first: $first, after: $cursor) {
            edges {
                cursor
                node {
                    name
                    createdAt
                    updatedAt
                    heartbeatAt
                    state
                    tags
                    host
                    description
                    runInfo {
                        gpuCount
                        cpuCount
                        gpu
                    }
                    config
                    summaryMetrics
                }
            }
        }
    }
}
"""

@dataclass
class Run:
    run_path: str
    created_at: dt.datetime
    updated_at: dt.datetime
    state: str
    tags: list[str]
    host_name: str
    gpu_name: str
    gpu_count: int
    cpu_count: int
    metrics_df: pl.DataFrame = pl.DataFrame()

@dataclass
class Project:
    project: str
    runs: list[Run] = field(default_factory=list)
