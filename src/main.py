from src.common import utils
from src.common import spark_utils as sutils
from src.metadata import  Season
from src.transformations import nomalizer, weather_aggregator as weather


def main(logger):
    parameters = utils.get_parameter(logger)

    spark = sutils.get_spark_context(app_name="subito challenge")

    # df_city_attributes = sutils.get_dataframe(logger, spark, parameters["city_attributes"])

    # df_humidity = sutils.get_unpivot_dataframe(
    #     logger,
    #     spark,
    #     parameters["humidity"],
    #     ["datetime"],
    #     "country",
    #     "humidity"
    # )
    #
    # df_pressure = sutils.get_unpivot_dataframe(
    #     logger,
    #     spark,
    #     parameters["pressure"],
    #     ["datetime"],
    #     "country",
    #     "pressure"
    # )
    #
    # df_temperature = sutils.get_unpivot_dataframe(
    #     logger,
    #     spark,
    #     parameters["temperature"],
    #     ["datetime"],
    #     "country",
    #     "temperature"
    # )

    logger.info("Task 1 - Clear Weather Spring Analysis ")
    df_weather_description = sutils.melt(
        logger,
        sutils.get_dataframe(logger, spark, parameters["weather_description"]),
        ["datetime"],
        "country",
        "weather_description"
    )

    df_weather_description = nomalizer.nomalize_datafram(df_weather_description)

    agg_clear_weather_df = weather.get_clear_weather_cities_by_season(df_weather_description, Season.SPRING_MONTHS.value)

    agg_clear_weather_df.show(100)


if __name__ == "__main__":
    logger = utils.get_logger()
    main(logger)