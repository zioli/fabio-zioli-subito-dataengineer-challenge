from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from src.metadata import WeatherDescription, Granularity
from functools import reduce

# "sky is clear"


def filter_by_weather_description(df:DataFrame, description:str, weather_description_column_name:str="weather_description") -> DataFrame:
    filtered_df = df.filter(F.col("weather_description") == weather_description_column_name)

    return filtered_df


def filter_season(df:DataFrame, season_months: list[int]) -> DataFrame:
    """
    Narrows down the dataset to focus exclusively on a specific season.

    :param df: **required** The dataset must include the `date`.
    :param season_months: **required**  an array of integers. see src.metadata.Season:
    :return: DataFrame
    """
    filtered_df = df.filter(
        F.month("date").isin(season_months)
    )
    return filtered_df


def calculate_stats(df: DataFrame, dimensions:list[str], metric:str, granularity:str, temporal_dimension_name:str="date" ) -> DataFrame:
    """
    It calculates the aggregated statistics for the metric for the dimensions and granularity (`src.metadata.constants.Granularity`) passed as parameter.

    - standard deviation
    - min
    - max

     - it also creates the granularity column : month, year, day, etc as dimension

    :param df: **required** .
    :param dimensions: **required** A list of strings indicating which dimensions to aggregate (It has to include the temporal dimension here as well).
    :param metric:
    :param granularity: see *src.metadata.constants.Granularity*
    :param temporal_dimension: Temporal dimension name, as default: date
    :return: DataFrame
    """
    agg_df = df.withColumn(granularity, F.date_trunc(f"{granularity}", F.col(temporal_dimension_name)).cast("date"))
    agg_df = agg_df.groupby(*dimensions).agg(
        F.stddev(metric).alias(f"stddev_{metric}"),
        F.min(metric).alias(f"min_{metric}"),
        F.max(metric).alias(f"max_{metric}"),
    )

    return agg_df

def get_clear_weather_cities_by_season(df:DataFrame, season_months:list[int], threshold:int=15) -> DataFrame:
    """
        Filters for locations that surpassed the monthly threshold for "clear weather" (`see src.metadata.WeatherDescription`) at the season passed as parameter.
        This process identifies the specific city-month combinations that maintained a 'clear weather' status for at least
        X (`threshold`) days,

    :param df: **required** Expected the following columns `date`, `country` and `weather_description`
    :param season_months: **required** `The seasons can be found at src.metadata.Season`:
    :param threshold: Defaults to 15
    :return: DataFrame
    """

    required_columns = ["date", "country", "weather_description"]
    df_columns = df.columns
    missing = [c for c in required_columns if c not in df_columns]
    if missing:
        raise Exception("Missing required columns: {}".format(missing))

    # Narrow down the dataframe to keep only rows from the season
    agg_df = filter_season(df, season_months)

    # Summing the number of clear hours and not clear hours per day and per country
    agg_df = agg_df.groupBy("date", "country").agg(
                F.sum(
                    F.when(F.col("weather_description").isin( WeatherDescription.get_clear_weather()), 1).otherwise(0)
                ).alias("clear_weather_hours")
                ,
                F.sum(
                    F.when(~F.col("weather_description").isin(WeatherDescription.get_clear_weather()), 1).otherwise(0)
                ).alias("not_clear_weather_hours")
            )

    # Creating the flag `is_clear_day`. If the date has more `clear_weather_hours` than `not_clear_weather_hours`, it is set as true, else false
    agg_df = agg_df.withColumn("is_clear_day",
            F.col("clear_weather_hours") > F.col("not_clear_weather_hours")
        )

    # adding the month_id based on the date column
    agg_df = agg_df.withColumn("month_id", F.date_trunc("month", F.col("date")).cast("date"))

    # now summarizing the number of days considered clear weather per month_id and country
    agg_df = (agg_df.groupBy("month_id", "country").agg(
        F.sum(
            F.when(F.col("is_clear_day") == True, 1).otherwise(0)
        ).alias("clear_days")
    )

    # Finally keeping the countries and months were the number of `clear_days` > 15
    .filter(
        F.col("clear_days") > 15
    ))

    return agg_df


def join_dataframe(dfa:DataFrame, dfb:DataFrame, join_keys:dict, columns:dict, how:str="left"):
    """
        Joins two DataFrames with dynamic key matching and column aliases.

        It simplifies complex joins where one or more primary/foreign keys have different
        names and allows for a clean output schema by specifying which columns
        to keep and how to rename them.

        Args:
            dfa (DataFrame): The 'left' side DataFrame.
            dfb (DataFrame): The 'right' side DataFrame.
            join_keys (dict): A mapping of join columns where the key is the column in dfa
                and the value is the matching column in dfb.
                Example: {"City_ID": "Location_Code"}
            columns (dict): A nested dictionary defining columns to select and alias.
                Format: {"a": {"col_name": "alias_or_None"}, "b": {"col_name": "alias_or_None"}}
                If the alias is None, the original column name is preserved.
            how (str): Type of join to perform (e.g., 'inner', 'left', 'right', 'outer'). **Default**: **left**.

        Returns:
            DataFrame: A new DataFrame containing only the aliased columns defined in the
                columns dictionary, joined according to the provided keys.

        Example:
            >>> join_keys = {"id": "city_id"}
            >>> cols = {"a": {"name": "city"}, "b": {"temp": "avg_temp"}}
            >>> result = join_dataframe(df_cities, df_weather, join_keys, cols, "inner")
    """
    conditions = [dfa[left] == dfb[right] for left, right in join_keys.items()]
    final_condition = reduce(lambda x, y: x & y, conditions)

    selected_columns = []
    acol = columns.get("a", [])
    bcol = columns.get("b", [])

    for c in acol:
        col_name = c
        col_alias =  acol[c] or c
        selected_columns.append(dfa[col_name].alias(col_alias))

    for c in bcol:
        col_name = c
        col_alias =  bcol[c] or c
        selected_columns.append(dfb[col_name].alias(col_alias))

    df =  dfa.join(
                dfb,
                final_condition,
                how=how) \
            .select(*selected_columns)

    return df