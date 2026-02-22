import pytest
from src import utils
from unittest.mock import patch, mock_open, MagicMock

def test_get_columns_names_should_strip_leading_and_trailing_spaces():
    # Adding leading and trailing spaces to the columns
    mock_content = " datetime , city name ,   humidity value "
    mock_header = mock_content.split(",")

    # Expected columns names with underscore (_) instead of spaces on compound columns names
    expected_header = ["datetime", "city_name", "humidity_value"]

    with patch("builtins.open", mock_open(read_data=mock_content)) as mocked_file:
        with patch("src.utils.csv.reader") as mocked_csv_reader:

            mock_iter = MagicMock()
            mock_iter.__next__.return_value = mock_header
            mocked_csv_reader.return_value = mock_iter

            result = utils.get_columns_names('/file/mock/path.csv')

            assert result == expected_header

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