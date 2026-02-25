from src.common import utils
from src.common import spark_utils as sutils
from src.metadata import Season, Granularity
from src.transformations import nomalizer, weather_aggregator as weather

def main(logger):
    parameters = utils.get_parameter(logger)

    spark = sutils.get_spark_context(app_name="subito challenge")

    ### LOAD DATAFRAMES
    # ------------------------------------------------------------------------
    # CITY ATTRIBUTES
    df_city_attributes = sutils.get_dataframe(logger, spark, parameters["city_attributes"])
    # ------------------------------------------------------------------------
    # HUMIDITY
    df_humidity = sutils.melt(
        logger,
        sutils.get_dataframe(logger, spark, parameters["humidity"]),
        ["datetime"],
        "country",
        "humidity"
    )
    df_humidity = nomalizer.nomalize_datafram(df_humidity)

    ## Getting country information for the humidity dataframe
    df_humidity = weather.join_dataframe(
                    df_humidity,
                    df_city_attributes,
            {"country": "city"},
            {"a" : {"datetime":None, "date":None, "country":"city", "humidity": None } , "b" : {"country" : None}})
    # ------------------------------------------------------------------------
    # PRESSURE
    df_pressure = sutils.melt(
        logger,
        sutils.get_dataframe(logger, spark, parameters["pressure"]),
        ["datetime"],
        "country",
        "pressure"
    )
    df_pressure = nomalizer.nomalize_datafram(df_pressure)
    ## Getting country information for the pressure dataframe
    df_pressure = weather.join_dataframe(
                    df_pressure,
                    df_city_attributes,
            {"country": "city"},
            {"a" : {"datetime":None, "date":None, "country":"city", "pressure": None } , "b" : {"country" : None}})
    # ------------------------------------------------------------------------
    # TEMPERATURE
    df_temperature = sutils.melt(
        logger,
        sutils.get_dataframe(logger, spark, parameters["temperature"]),
        ["datetime"],
        "country",
        "temperature"
    )
    df_temperature = nomalizer.nomalize_datafram(df_temperature)
    ## Getting country information for the temperature dataframe
    df_temperature = weather.join_dataframe(
                    df_temperature,
                    df_city_attributes,
            {"country": "city"},
            {"a" : {"datetime":None, "date":None, "country":"city", "temperature": None } , "b" : {"country" : None}})
    # ------------------------------------------------------------------------
    # WEATHER DESCRIPTION
    df_weather_description = sutils.melt(
        logger,
        sutils.get_dataframe(logger, spark, parameters["weather_description"]),
        ["datetime"],
        "country",
        "weather_description"
    )
    df_weather_description = nomalizer.nomalize_datafram(df_weather_description)
    # ------------------------------------------------------------------------

    # ------------------------------------------------------------------------
    # TASK 01
    # ------------------------------------------------------------------------
    logger.info("Task 1 - Clear Weather Spring Analysis ")

    df_agg_clear_weather_df = weather.get_clear_weather_cities_by_season(df_weather_description, Season.SPRING_MONTHS.value)
    # df_agg_clear_weather_df.show(10)
    logger.info(f"\nDataFrame Output:\n{df_agg_clear_weather_df._jdf.showString(10, 20, False)}")
    # ------------------------------------------------------------------------

    # ------------------------------------------------------------------------
    # TASK 02 : MONTHLY: Global Weather Statistics
    # ------------------------------------------------------------------------
    logger.info("Task 2 - Global Weather Statistics [MONTHLY AGGREGATION]")

    df_agg_monthly_humidity_stats =  weather.calculate_stats(df_humidity, [Granularity.MONTHLY.value, "country"], "humidity", Granularity.MONTHLY.value)

    df_agg_monthly_temperature_stats =  weather.calculate_stats(df_temperature, [Granularity.MONTHLY.value, "country"], "temperature", Granularity.MONTHLY.value)

    df_agg_monthly_pressure_stats =  weather.calculate_stats(df_pressure, [Granularity.MONTHLY.value, "country"], "pressure", Granularity.MONTHLY.value)

    df_agg_monthly_stats = (df_agg_monthly_humidity_stats
                            .join(df_agg_monthly_temperature_stats, on=["country", "month"], how="left")
                            .join(df_agg_monthly_pressure_stats, on=["country", "month"], how="left"))

    logger.info(f"\nDataFrame Output:\n{df_agg_monthly_stats._jdf.showString(10, 20, False)}")

    # ------------------------------------------------------------------------
    # TASK 02 : YEARLY: Global Weather Statistics
    # ------------------------------------------------------------------------
    logger.info("Task 2 - Global Weather Statistics [YEARLY AGGREGATION] ")


    df_agg_yearly_humidity_stats =  weather.calculate_stats(df_humidity, [Granularity.YEARLY.value, "country"], "humidity", Granularity.YEARLY.value)

    df_agg_yearly_temperature_stats =  weather.calculate_stats(df_temperature, [Granularity.YEARLY.value, "country"], "temperature", Granularity.YEARLY.value)

    df_agg_yearly_pressure_stats =  weather.calculate_stats(df_pressure, [Granularity.YEARLY.value, "country"], "pressure", Granularity.YEARLY.value)

    df_agg_yearly_stats = (df_agg_yearly_humidity_stats
                            .join(df_agg_yearly_temperature_stats, on=["country", "month"], how="left")
                            .join(df_agg_yearly_pressure_stats, on=["country", "month"], how="left"))

    logger.info(f"\nDataFrame Output:\n{df_agg_yearly_stats._jdf.showString(10, 20, False)}")

    # ------------------------------------------------------------------------
    # TASK 03 : Seasonal Temperature Range (Top 3 Cities)
    # ------------------------------------------------------------------------
    # ## TODO: TASK 03 : Will be working on this today!! After delivering the task, sorry for that.

if __name__ == "__main__":
    logger = utils.get_logger()
    main(logger)