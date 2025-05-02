import polars as pl

def fillna_round(srs: pl.Series) -> pl.Series:
    return srs.fill_null(0).fill_nan(0).round(1)

BASE_SCHEMA = {
    "企業名": pl.Utf8,
    "合計GPU使用時間(h)": pl.Float64,
    "GPU稼働率(%)": pl.Float64,
    "平均GPUパフォーマンス率(%)": pl.Float64,
    "最大GPUパフォーマンス率(%)": pl.Float64,
    "平均GPUメモリ利用率(%)": pl.Float64,
    "最大GPUメモリ利用率(%)": pl.Float64,
    "n_runs": pl.Int64,
    "assigned_gpu_node": pl.Int64,
    "assigned_gpu_hour": pl.Float64,
    "_total_gpu_hour": pl.Float64,
    "total_metrics_hour": pl.Float64,
}

OVERALL_SCHEMA = BASE_SCHEMA.copy()
OVERALL_SCHEMA["日付"] = pl.Utf8

MONTHLY_SCHEMA = BASE_SCHEMA.copy()
MONTHLY_SCHEMA["年月"] = pl.Utf8

WEEKLY_SCHEMA = BASE_SCHEMA.copy()
WEEKLY_SCHEMA["週開始日"] = pl.Utf8

DAILY_SCHEMA = BASE_SCHEMA.copy()
DAILY_SCHEMA["日付"] = pl.Utf8

SUMMARY_SCHEMA = {
    "company_name": pl.Utf8,
    "project": pl.Utf8,
    "Total hours": pl.Float64,
    "Total runs": pl.Int64,
    "master_node_runs": pl.Int64,
    "overlap_runs": pl.Int64,
    "ignore_runs": pl.Int64,
}

GPU_PER_NODE = 8
HOURS_PER_DAY = 24
MAX_PERCENT = 100

TMP_COLS = (
    pl.when(pl.col("average_gpu_utilization").is_not_null())
    .then(pl.col("duration_hour"))
    .otherwise(None)
    .alias("metrics_hour"),
    (pl.col("average_gpu_utilization") * pl.col("duration_hour")).alias(
        "sum_gpu_utilization"
    ),
    (pl.col("average_gpu_memory") * pl.col("duration_hour")).alias("sum_gpu_memory"),
)

AGG_COLS = (
    pl.col("assigned_gpu_node")
    .first()
    .mul(GPU_PER_NODE * HOURS_PER_DAY)
    .alias("assigned_gpu_hour"),
    pl.col("metrics_hour").sum().alias("total_metrics_hour"),
    pl.col("sum_gpu_utilization").sum(),
    pl.col("max_gpu_utilization").max(),
    pl.col("sum_gpu_memory").sum(),
    pl.col("max_gpu_memory").max(),
    pl.coalesce(
        pl.col("run_id").filter(pl.col("run_id").is_not_null()).n_unique(),
        pl.lit(0)
    ).cast(pl.Int64).alias("n_runs"),
    pl.col("assigned_gpu_node").first(),
)

METRICS_COLS = (
    pl.when(pl.col("total_gpu_hour") > pl.col("assigned_gpu_hour"))
    .then(MAX_PERCENT)
    .otherwise(
        (pl.col("total_gpu_hour") / pl.col("assigned_gpu_hour")).mul(MAX_PERCENT)
    )
    .alias("utilization_rate"),
    (pl.col("sum_gpu_utilization") / pl.col("total_metrics_hour")).alias(
        "average_gpu_utilization"
    ),
    (pl.col("sum_gpu_memory") / pl.col("total_metrics_hour")).alias(
        "average_gpu_memory"
    ),
)

SELECT_COLS = (
    pl.col("total_gpu_hour").pipe(fillna_round).alias("合計GPU使用時間(h)"),
    pl.col("utilization_rate").pipe(fillna_round).alias("GPU稼働率(%)"),
    pl.col("average_gpu_utilization")
    .pipe(fillna_round)
    .alias("平均GPUパフォーマンス率(%)"),
    pl.col("max_gpu_utilization")
    .pipe(fillna_round)
    .alias("最大GPUパフォーマンス率(%)"),
    pl.col("average_gpu_memory").pipe(fillna_round).alias("平均GPUメモリ利用率(%)"),
    pl.col("max_gpu_memory").pipe(fillna_round).alias("最大GPUメモリ利用率(%)"),
    pl.col("n_runs"),
    pl.col("assigned_gpu_node"),
    pl.col("assigned_gpu_hour"),
    pl.col("_total_gpu_hour"),
    pl.col("total_metrics_hour"),
)