import os
import sys
import datetime as dt
import json

import polars as pl


def handler(event: dict[str, str], context: object) -> None:
    return


if __name__ == "__main__":
    if len(sys.argv) == 2:
        event = {"WANDB_API_KEY": sys.argv[1]}
    elif len(sys.argv) == 3:
        event = {"WANDB_API_KEY": sys.argv[1], "target_date": sys.argv[2]}
    else:
        print("Invalid numbers of args.")
        exit()
    handler(event=event, context=None)
