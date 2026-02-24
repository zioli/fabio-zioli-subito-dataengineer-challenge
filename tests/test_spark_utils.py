from src.common import spark_utils as sutils


def test_build_stack_expression():
    expected = "stack(2, 'col 1', col_1, 'col 2', col_2)"
    assert expected == sutils.build_stack_expression(["col_1", "col_2"])