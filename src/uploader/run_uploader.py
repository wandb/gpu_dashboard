import datetime as dt
import polars as pl
from typing import List
from .artifact_handler import ArtifactHandler
from .data_processor import DataProcessor

class RunUploader:
    def __init__(self, new_runs_df, date_range: List):
        self.start_date = dt.datetime.strptime(date_range[0], "%Y-%m-%d").date()
        self.end_date = dt.datetime.strptime(date_range[1], "%Y-%m-%d").date()
        self.new_runs_df = new_runs_df
        self.date_range = date_range

    def process_and_upload_runs(self):
        old_runs_df = ArtifactHandler.read_dataset()
        all_runs_df = DataProcessor.combine_df(new_runs_df=self.new_runs_df, old_runs_df=old_runs_df)
        # .pipe(DataProcessor.apply_blacklist)
        ArtifactHandler.update_dataset(all_runs_df=all_runs_df, date_range=self.date_range)
        return all_runs_df