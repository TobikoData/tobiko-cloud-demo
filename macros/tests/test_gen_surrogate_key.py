import pytest
from macros.gen_surrogate_key import gen_surrogate_key
from sqlmesh.core.macros import MacroEvaluator


# Test the macro
def test_gen_surrogate_key():
    field_list = ["field1", "field2"]
    expected_sql = "SHA256(CONCAT(COALESCE(CAST(field1 AS STRING), '_sqlmesh_surrogate_key_null_default_'), '-', COALESCE(CAST(field2 AS STRING), '_sqlmesh_surrogate_key_null_default_')))"
    
    result = gen_surrogate_key(evaluator=MacroEvaluator,field_list=field_list).sql('bigquery')
    
    assert result == expected_sql