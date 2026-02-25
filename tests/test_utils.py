import pytest
from src.common import utils


def test_exclude_columns_test_if_one_column_is_removed_as_expected():
    columns = ["datetime", "city_name", "humidity_value"]
    columns_to_remove  = ["datetime"]

    expected_response = ["city_name", "humidity_value"]

    response = utils.exclude_columns(columns, columns_to_remove)
    assert response == expected_response

def test_exclude_columns_test_if_multiple_columns_are_removed_as_expected():
    columns = ["datetime", "city_name", "some_other_column", "humidity_value"]
    columns_to_remove  = ["datetime", "some_other_column"]

    expected_response = ["city_name", "humidity_value"]

    response = utils.exclude_columns(columns, columns_to_remove)
    assert response == expected_response