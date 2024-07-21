import pytest
from sqlglot import exp
from macros.gen_surrogate_key import gen_surrogate_key


def test_gen_surrogate_key_valid():
    field_list = [exp.Column(this="field1"), exp.Column(this="field2")]
    expected_sql = "SHA256(CONCAT(COALESCE(CAST(field1 AS STRING), '_null_'), '-', COALESCE(CAST(field2 AS STRING), '_null_')))"

    result = gen_surrogate_key(evaluator=None, field_list=field_list).sql("bigquery")

    assert result == expected_sql


def test_gen_surrogate_key_multiple_fields():
    field_list = [
        exp.Column(this="field1"),
        exp.Column(this="field2"),
        exp.Column(this="field3"),
    ]
    expected_sql = "SHA256(CONCAT(COALESCE(CAST(field1 AS STRING), '_null_'), '-', COALESCE(CAST(field2 AS STRING), '_null_'), '-', COALESCE(CAST(field3 AS STRING), '_null_')))"

    result = gen_surrogate_key(evaluator=None, field_list=field_list).sql("bigquery")

    assert result == expected_sql


def test_gen_surrogate_key_error_single_field():
    with pytest.raises(ValueError) as excinfo:
        gen_surrogate_key(evaluator=None, field_list=[exp.Column(this="field1")])

    assert "At least two fields are required to generate a surrogate key." in str(
        excinfo.value
    )


def test_gen_surrogate_key_error_empty_list():
    with pytest.raises(ValueError) as excinfo:
        gen_surrogate_key(evaluator=None, field_list=[])

    assert "At least two fields are required to generate a surrogate key." in str(
        excinfo.value
    )


def test_gen_surrogate_key_error_non_column_fields():
    with pytest.raises(ValueError) as excinfo:
        gen_surrogate_key(
            evaluator=None,
            field_list=[exp.Column(this="field1"), 2, exp.Column(this="field3")],
        )

    assert "All fields must be column objects." in str(excinfo.value)
