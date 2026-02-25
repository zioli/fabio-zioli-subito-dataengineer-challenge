from src.transformations import nomalizer
from pyspark.sql import SparkSession

def test_normilize_columns_names_should_strip_leading_and_trailing_spaces_and_lowercase():

    # Defining data with mixed case and specific temperatures
    data = [
        ("Milan", 28.5, 5.2),
        ("Biella", 24.0, 2.1),
        ("Rome", 31.2, 8.5)
    ]
    # Defining columns with SPACES to test your backtick/stack logic
    columns = ["City Name", "Hot Period", "Cold Period"]

    spark = SparkSession.builder.appName("WeatherTest").getOrCreate()
    df = spark.createDataFrame(data, schema=columns)

    df = nomalizer.normalize_columns(df)

    return_columns = df.columns
    expected_header = ["city_name", "hot_period", "cold_period"]
    assert return_columns == expected_header