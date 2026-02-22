from src import spark_utils as sutils, utils

def main(logger):
    parameters = utils.get_parameter(logger)

    spark = sutils.get_spark_context(app_name="subito challenge")

    df_city_attributes = sutils.get_dataframe(logger, spark, parameters["city_attributes"])

    df_humidity = sutils.get_unpivot_dataframe(
        logger,
        spark,
        parameters["humidity"],
        ["datetime"],
        "country",
        "humidity"
    )

    df_pressure = sutils.get_unpivot_dataframe(
        logger,
        spark,
        parameters["pressure"],
        ["datetime"],
        "country",
        "pressure"
    )

    df_temperature = sutils.get_unpivot_dataframe(
        logger,
        spark,
        parameters["temperature"],
        ["datetime"],
        "country",
        "temperature"
    )

    df_weather_description = sutils.get_unpivot_dataframe(
        logger,
        spark,
        parameters["weather_description"],
        ["datetime"],
        "country",
        "weather_description"
    )

    df_weather_description.show(4)

if __name__ == "__main__":
    logger = utils.get_logger()
    main(logger)