from logging import Logger

from pyspark.sql import SparkSession, DataFrame
import os
import argparse
import json
import logging
import csv

def get_columns_names(file_path):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        columns = next(reader)

    return [c.replace(" ", "_") for c in columns]

def get_stack(cols):
    aux = ", ".join([f"'{c.replace('_', ' ')}', {c}" for c in cols])
    return f"stack({len(cols)}, {aux})"

def exclude_columns(columns:list, columns_to_remove:list) -> list:
    return [c for c in columns if c not in columns_to_remove]

def unpivot(logger:Logger, df:DataFrame, columns_to_keep:list, dimension_column_name:str, value_column_name:str) -> DataFrame:
    logger.info(f"unpivot: columns_to_keep       : [{columns_to_keep}]")
    logger.info(f"unpivot: dimension_column_name : [{dimension_column_name}]")
    logger.info(f"unpivot: value_column_name     : [{value_column_name}]")
    columns = exclude_columns(df.columns, columns_to_keep)

    logger.info(f"unpivot: columns               : [{columns}]")

    stack = get_stack(columns)

    logger.info(f"unpivot: stack                 : [{stack}]")
    logger.info(f"unpivot: full stack            : [{stack} as ({dimension_column_name}, {value_column_name}]")

    unpivoted_df = df.selectExpr(
        *columns_to_keep,
        f"{stack} as ({dimension_column_name}, {value_column_name})"
    )
    return unpivoted_df

def get_logger(name=__name__, level=logging.INFO):
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(name)

def get_spark_context(logger, app_name=__name__):
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def get_parameter(logger):
    parser = argparse.ArgumentParser(description="Subito Spark Job Parameters")
    defaul_input = {
        "city_attributes": "data/raw/city_attributes.csv",
        "humidity": "data/raw/humidity.csv",
        "pressure": "data/raw/pressure.csv",
        "temperature": "data/raw/temperature.csv",
        "weather_description": "data/raw/weather_description.csv",
    }
    parser.add_argument(
        '--config',
        type=str,
        default=json.dumps(defaul_input),
        help='JSON string for job configuration'
    )
    args = parser.parse_args()
    try:
        j = json.loads(args.config)

        if j.get("city_attributes", None) is None:
            raise ValueError("Missing city_attributes parameter")

        if j.get("humidity", None) is None:
            raise ValueError("Missing humidity parameter")

        if j.get("pressure", None) is None:
            raise ValueError("Missing pressure parameter")

        if j.get("temperature", None) is None:
            raise ValueError("Missing temperature parameter")

        if j.get("weather_description", None) is None:
            raise ValueError("Missing weather_description parameter")

        return j
    except json.decoder.JSONDecodeError as e:
        logger.error(f"Invalid JSON format for --config: {args.config}")
        raise

def get_dataframe(logger, spark, file_path):
    logger.info(f"load: Loading file [{file_path}]")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found : {file_path}")

    logger.info(f"load: Getting the normalized columns names ")
    columns = get_columns_names(file_path)

    df = spark.read \
        .option("header", "true") \
        .option("skipRows", 1) \
        .csv(file_path) \
        .toDF(*columns)

    return df

def normalize_columns_name(logger:Logger, df:DataFrame) -> DataFrame:
    columns = df.columns
    new_columns = [ c.replace("", "_") for c in columns ]
    return df.toDF(*new_columns)


def get_unpivot_dataframe(logger:Logger, spark:SparkSession, file_path, columns_to_keep, dimension_column_name, value_column_name) -> DataFrame:
    df_raw = get_dataframe(logger, spark, file_path)
    df_unpivoted = unpivot(
        logger,
        df_raw,
        columns_to_keep,
        dimension_column_name,
        value_column_name
    )

    return df_unpivoted


def main(logger):
    parameters = get_parameter(logger)

    spark = get_spark_context(logger, app_name="subito challenge")

    df_city_attributes = get_dataframe(logger, spark, parameters["city_attributes"])

    df_humidity = get_unpivot_dataframe(
        logger,
        spark,
        parameters["humidity"],
        ["datetime"],
        "country",
        "humidity"
    )

    df_pressure = get_unpivot_dataframe(
        logger,
        spark,
        parameters["pressure"],
        ["datetime"],
        "country",
        "pressure"
    )

    df_temperature = get_unpivot_dataframe(
        logger,
        spark,
        parameters["temperature"],
        ["datetime"],
        "country",
        "temperature"
    )

    df_weather_description = get_unpivot_dataframe(
        logger,
        spark,
        parameters["weather_description"],
        ["datetime"],
        "country",
        "weather_description"
    )

    df_weather_description.show(100)

if __name__ == "__main__":
    logger = get_logger()
    main(logger)