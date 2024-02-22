import polars as pl

def agg_company_daily(df: pl.DataFrame) -> pl.DataFrame:
    """企業ごと、日ごとに集計する"""
    new_df = (
        # metricsのavgにdurationを掛け算
        df.with_columns(
            (pl.col("duration_hours") * pl.col("gpu_count")).alias("total_gpu_hours"),
            (pl.col("average_gpu_memory") * pl.col("duration_hours")).alias(
                "sum_gpu_memory"
            ),
            (pl.col("average_gpu_gpu") * pl.col("duration_hours")).alias("sum_gpu_gpu"),
        )
        .group_by("date", "company_name")
        .agg(
            pl.col("total_gpu_hours").sum(),
            pl.col("duration_hours").sum(),
            pl.col("assigned_gpu_node").max(),
            pl.col("max_gpu_gpu").max().alias("max_gpu_utilization"),
            pl.col("max_gpu_memory").max(),
            # 加重平均
            pl.col("sum_gpu_gpu")
            .sum()
            .truediv(
                df.filter(pl.col("max_gpu_memory").is_not_null())[
                    "duration_hours"
                ].sum()
            )
            .alias("average_gpu_utilization"),
            pl.col("sum_gpu_memory")
            .sum()
            .truediv(
                df.filter(pl.col("max_gpu_memory").is_not_null())[
                    "duration_hours"
                ].sum()
            )
            .alias("average_gpu_memory"),
        )
        .with_columns(
            (
                pl.col("total_gpu_hours")
                .truediv(pl.col("assigned_gpu_node").mul(8).mul(24))
                .mul(100)
            ).alias("utilization_rate"),
        )
        .select(
            "company_name",
            "date",
            "total_gpu_hours",
            "utilization_rate",
            "assigned_gpu_node",
            "average_gpu_utilization",
            "max_gpu_utilization",
            "average_gpu_memory",
            "max_gpu_memory",
        )
        .sort("date", descending=True)
    )
    return new_df



def monthly_overall(df: pl.DataFrame) -> pl.DataFrame:
    """"""
    new_df = df.select(
        "company_name",
        "year_month",
        "total_gpu_hours",
        "average_gpu_utilization",
        "max_gpu_utilization",
        "average_gpu_memory",
        "max_gpu_memory",
        "assigned_gpu_hours",
        "utilization_rate",
    )
    return new_df


def overall(df: pl.DataFrame) -> pl.DataFrame:
    """"""
    new_df = df.select(
        "company_name",
        "assigned_gpu_hours",
        "total_gpu_hours",
        "average_gpu_utilization",
        "max_gpu_utilization",
        "average_gpu_memory",
        "max_gpu_memory",
        "utilization_rate",
    )
    return new_df
