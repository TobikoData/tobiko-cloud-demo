import pytest
from macros.gen_surrogate_key import gen_surrogate_key
from sqlmesh.core.macros import MacroEvaluator


def test_gen_surrogate_key_valid():
    field_list = ["field1", "field2"]
    expected_sql = "SHA256(CONCAT(COALESCE(CAST(field1 AS STRING), '_null_'), '-', COALESCE(CAST(field2 AS STRING), '_null_')))"

    result = gen_surrogate_key(evaluator=MacroEvaluator, field_list=field_list).sql(
        "bigquery"
    )

    assert result == expected_sql


def test_gen_surrogate_key_multiple_fields():
    field_list = ["field1", "field2", "field3"]
    expected_sql = "SHA256(CONCAT(COALESCE(CAST(field1 AS STRING), '_null_'), '-', COALESCE(CAST(field2 AS STRING), '_null_'), '-', COALESCE(CAST(field3 AS STRING), '_null_')))"

    result = gen_surrogate_key(evaluator=MacroEvaluator, field_list=field_list).sql(
        "bigquery"
    )

    assert result == expected_sql


def test_gen_surrogate_key_error_single_field():
    with pytest.raises(ValueError) as excinfo:
        gen_surrogate_key(evaluator=MacroEvaluator, field_list=["field1"])

    assert "List should have at least 2 items after validation, not 1" in str(
        excinfo.value
    )


def test_gen_surrogate_key_error_empty_list():
    with pytest.raises(ValueError) as excinfo:
        gen_surrogate_key(evaluator=MacroEvaluator, field_list=[])

    assert "List should have at least 2 items after validation, not 0" in str(
        excinfo.value
    )
