from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from src.metadata import WeatherDescription

# "sky is clear"


def filter_by_weather_description(df:DataFrame, description:str, weather_description_column_name:str="weather_description") -> DataFrame:
    filtered_df = df.filter(F.col("weather_description") == weather_description_column_name)

    return filtered_df


def filter_season(df:DataFrame, season_months: list[int], month_column_name:str="month") -> DataFrame:
    """
    Narrows down the dataset to focus exclusively on a specific season.

    :param df: **required** The dataset must include a month column, represented as an integer (e.g., 1 for January, 12 for December).
    :param season_months: **required**  an array of integers. see src.metadata.Season:
    :param month_column_name: It takes the "month" name as a default name
    :return: DataFrame
    """
    filtered_df = df.filter(
        (F.col(month_column_name).isin(season_months))
    )
    return filtered_df



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

