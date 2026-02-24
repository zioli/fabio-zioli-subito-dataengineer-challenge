from logging import Logger
from pyspark.sql import SparkSession, DataFrame
import os
from src.common import utils

def _build_stack_expression(cols):
    aux = ", ".join([f"'{c.replace('_', ' ')}', {c}" for c in cols])
    return f"stack({len(cols)}, {aux})"

def melt(logger:Logger, df:DataFrame, columns_to_keep:list, dimension_column_name:str, value_column_name:str) -> DataFrame:
    logger.info(f"unpivot: columns_to_keep       : [{columns_to_keep}]")
    logger.info(f"unpivot: dimension_column_name : [{dimension_column_name}]")
    logger.info(f"unpivot: value_column_name     : [{value_column_name}]")
    columns = utils.exclude_columns(df.columns, columns_to_keep)

    logger.info(f"unpivot: columns               : [{columns}]")

    stack = _build_stack_expression(columns)

    logger.info(f"unpivot: stack                 : [{stack}]")
    logger.info(f"unpivot: full stack            : [{stack} as ({dimension_column_name}, {value_column_name}]")

    unpivot_df = df.selectExpr(
        *columns_to_keep,
        f"{stack} as ({dimension_column_name}, {value_column_name})"
    )
    return unpivot_df

def get_dataframe(logger:Logger, spark:SparkSession, file_path:str) -> DataFrame:
    logger.info(f"load: Loading file [{file_path}]")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found : {file_path}")

    logger.info(f"load: Getting the normalized columns names ")
    columns = utils.get_columns_names(file_path)

    df = spark.read \
        .option("header", "true") \
        .option("skipRows", 1) \
        .csv(file_path) \
        .toDF(*columns)

    return df



# def get_unpivot_dataframe(logger:Logger, spark:SparkSession, file_path, columns_to_keep, dimension_column_name, value_column_name) -> DataFrame:
#     df_raw = get_dataframe(logger, spark, file_path)
#     df_unpivoted = melt(
#         logger,
#         df_raw,
#         columns_to_keep,
#         dimension_column_name,
#         value_column_name
#     )
#
#     return df_unpivoted

def get_spark_context(app_name=__name__) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()