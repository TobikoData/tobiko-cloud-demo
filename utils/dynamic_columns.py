from sqlglot import exp
from sqlmesh import macro

https://deepwiki.com/search/for-sqlmesh-macros-built-in-py_aa50390d-82a7-4f0d-9209-2fe71b98cf39

@macro("dynamic_columns")
def dynamic_columns(evaluator) -> exp.Select:
    """
    Generates a dynamic SELECT statement with a 50% chance of having either one or two columns.
    
    Args:
        evaluator: The SQLMesh evaluator
    
    Returns:
        A SELECT statement with either "select 1 as column_1" or "select 1 as column_1, 2 as column_2"
    """
    import random
    
    # 50% chance of having one or two columns
    if random.random() < 0.5:
        select_expr = exp.Select(
            expressions=[
                exp.Alias(
                    this=exp.Literal.number(1),
                    alias="column_1"
                )
            ]
        )
    else:
        select_expr = exp.Select(
            expressions=[
                exp.Alias(
                    this=exp.Literal.number(1),
                    alias="column_1"
                ),
                exp.Alias(
                    this=exp.Literal.number(2),
                    alias="column_2"
                )
            ]
        )
    
    return select_expr
