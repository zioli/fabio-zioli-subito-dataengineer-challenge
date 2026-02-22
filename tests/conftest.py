import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("subito") \
        .appName("subito-data-challenge") \
        .getOrCreate()