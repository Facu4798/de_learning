"""DataFrame comparison utilities for MDFE replacement.

Provides functions to normalize and compare two PySpark DataFrames with
multiset (duplicate-aware) comparison, rounding normalization for floats,
schema alignment, and diagnostic sampling for differing rows.

Usage example:
    from df_compare import compare_dataframes

    report = compare_dataframes(
        spark, df_mdfe, df_my, drop_columns=['processing_ts'],
        float_precision=6, sample_limit=100, write_diagnostics_to='wasbs://container@account.blob.core.windows.net/diag/')

The function returns a dictionary `report` with keys:
  - pass: bool
  - expected_count: int
  - actual_count: int
  - distinct_mismatch_count: int (number of distinct rows with count diffs)
  - total_count_diff: int (sum absolute count differences)
  - sample: list of dicts (sample of mismatching rows and counts)
  - diagnostics_path: optional path where a CSV of mismatches was written

This file is intentionally dependency-light and uses pyspark.sql APIs.
"""

from typing import List, Optional, Dict, Any
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger("df_compare")
logger.addHandler(logging.NullHandler())


def _normalize_dataframe(df: DataFrame, columns: List[str], float_precision: int,
                         drop_columns: Optional[List[str]] = None) -> DataFrame:
    """Return a normalized DataFrame selecting `columns` in deterministic order.

    - Drops columns in `drop_columns` if provided.
    - Orders columns alphabetically to ensure deterministic grouping/joins.
    - Applies basic normalization per-column.
    """
    if drop_columns:
        columns = [c for c in columns if c not in set(drop_columns)]

    columns_sorted = sorted(columns)

    exprs = []
    # For normalization decisions we inspect the df dtypes mapping
    dtypes = dict(df.dtypes)
    for col in columns_sorted:
        if col not in dtypes:
            # if missing, create null column
            exprs.append(F.lit(None).alias(col))
            continue
        dtype_str = dtypes[col]
        if dtype_str in ('float', 'double') or dtype_str.startswith('decimal'):
            exprs.append(F.round(F.col(col).cast('double'), float_precision).alias(col))
        elif dtype_str in ('int', 'bigint', 'smallint', 'tinyint'):
            exprs.append(F.col(col).cast('long').alias(col))
        elif dtype_str in ('timestamp', 'date'):
            exprs.append(F.col(col).cast('string').alias(col))
        else:
            exprs.append(F.trim(F.col(col).cast('string')).alias(col))

    return df.select(*exprs)


def compare_dataframes(
    spark: SparkSession,
    expected: DataFrame,
    actual: DataFrame,
    drop_columns: Optional[List[str]] = None,
    float_precision: int = 6,
    sample_limit: int = 100,
    write_diagnostics_to: Optional[str] = None,
) -> Dict[str, Any]:
    """Compare two DataFrames with normalization and produce a report.

    The comparison is multiset-aware: duplicate rows and their counts are considered.

    Parameters
    - spark: active SparkSession
    - expected: DataFrame produced by MDFE (ground truth)
    - actual: DataFrame produced by candidate notebook
    - drop_columns: columns to ignore (processing timestamps, metadata)
    - float_precision: number of decimal places to round numeric floats
    - sample_limit: number of mismatched distinct rows to collect for diagnostics
    - write_diagnostics_to: optional path (e.g., wasbs://...) to write CSV diagnostics

    Returns a report dictionary.
    """
    # Determine common columns to compare
    exp_cols = set(expected.columns)
    act_cols = set(actual.columns)
    common = list(exp_cols.intersection(act_cols))
    if not common:
        raise ValueError("No common columns between expected and actual DataFrames")

    # Normalize both DataFrames
    norm_exp = _normalize_dataframe(expected, common, float_precision, drop_columns)
    norm_act = _normalize_dataframe(actual, common, float_precision, drop_columns)

    # Counts
    expected_count = norm_exp.count()
    actual_count = norm_act.count()

    # Compute grouped counts (multiset representation)
    group_cols = sorted(common)
    exp_grouped = norm_exp.groupBy(*group_cols).agg(F.count(F.lit(1)).alias('cnt_exp'))
    act_grouped = norm_act.groupBy(*group_cols).agg(F.count(F.lit(1)).alias('cnt_act'))

    # Full outer join on all columns
    joined = exp_grouped.join(act_grouped, on=group_cols, how='fullouter')
    joined = joined.fillna({'cnt_exp': 0, 'cnt_act': 0})

    # mismatches: where counts differ
    mismatches = joined.where(F.col('cnt_exp') != F.col('cnt_act'))

    # total multiset difference (sum abs diff)
    diff_total_row = mismatches.select(F.sum(F.abs(F.col('cnt_exp') - F.col('cnt_act'))).alias('total_diff')).collect()
    total_diff = int(diff_total_row[0]['total_diff']) if diff_total_row and diff_total_row[0]['total_diff'] is not None else 0

    distinct_mismatch_count = mismatches.count()

    pass_check = (total_diff == 0)

    # Collect a small diagnostic sample to driver
    sample = []
    if distinct_mismatch_count > 0:
        sample_rows = mismatches.limit(sample_limit).collect()
        for r in sample_rows:
            row_dict = {c: r[c] for c in group_cols}
            row_dict['cnt_expected'] = int(r['cnt_exp'])
            row_dict['cnt_actual'] = int(r['cnt_act'])
            sample.append(row_dict)

    diagnostics_path = None
    if write_diagnostics_to and distinct_mismatch_count > 0:
        try:
            # write mismatches to provided path as CSV
            out_df = mismatches.select(*group_cols, 'cnt_exp', 'cnt_act')
            out_df = out_df.withColumn('_diff_abs', F.abs(F.col('cnt_exp') - F.col('cnt_act')))
            out_df.write.mode('overwrite').option('header', True).csv(write_diagnostics_to)
            diagnostics_path = write_diagnostics_to
        except Exception as e:
            logger = logging.getLogger('df_compare')
            logger.warning('Failed to write diagnostics to %s: %s', write_diagnostics_to, e)

    report = {
        'pass': pass_check,
        'expected_count': expected_count,
        'actual_count': actual_count,
        'distinct_mismatch_count': distinct_mismatch_count,
        'total_count_diff': total_diff,
        'sample': sample,
        'diagnostics_path': diagnostics_path
    }

    return report


if __name__ == '__main__':
    # simple self-test scaffold (won't run in CI without Spark)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master('local[2]').appName('df_compare_selftest').getOrCreate()
    df1 = spark.createDataFrame([(1, 'a', 1.234567), (2, 'b', 2.5), (2, 'b', 2.5)], ['id', 'val', 'v'])
    df2 = spark.createDataFrame([(1, 'a', 1.2345671), (2, 'b', 2.5)], ['id', 'val', 'v'])
    rpt = compare_dataframes(spark, df1, df2, float_precision=5)
    print(rpt)
