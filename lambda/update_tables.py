import datetime as dt

import polars as pl
import wandb

from blank_table import BlankTable
from config import CONFIG

def update_tables(df: pl.DataFrame, target_date: dt.date) -> None:
    return