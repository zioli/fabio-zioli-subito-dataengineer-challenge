from pyspark.sql import functions as F, DataFrame


def nomalize_datafram(df:DataFrame, datetime_column:str="datetime") -> DataFrame:
    """
    Standardizes the dataset by adding specific time-based columns.

    Instead of recalculating months or dates in every query, this function
    prepares the DataFrame upfront. It takes the raw timestamp and breaks it
    down into the specific pieces we need for our weather analysis.

    Columns Created:
        - date: The full date extracted from the timestamp (YYYY-MM-DD).
        - month: The month as an integer (1 to 12), used for seasonal filtering.
        - month_id: The first day of the month extracted from the timestamp (YYYY-MM-DD).

    :param df:
    :param datetime_column:
    :return: DataFrame
    """
    df = df \
        .withColumn("date", F.col(datetime_column).cast("date")) \
        .withColumn("month", F.month("date")) \
        .withColumn("month_id", F.date_trunc("month", F.col("date")).cast("date"))

    return df
