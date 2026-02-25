from src.common import spark_utils as sutils


def test_build_stack_expression():
    expected = "stack(3, 'col_1', `col_1`, 'col 2', `col 2`, 'Col 3', `Col 3`)"
    assert expected == sutils._build_stack_expression(["col_1", "col 2", "Col 3"])
