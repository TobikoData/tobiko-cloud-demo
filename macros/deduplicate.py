"""a SQLMesh python macro that generates a query to deduplicate rows within a table"""

from sqlglot import exp
from sqlmesh import macro
from sqlglot import parse_one
from sqlmesh.core.macros import MacroEvaluator

@macro()
def deduplicate(evaluator, relation: exp.Table,partition_by: list[exp.Expression], order_by: list[str]) -> exp.Query:
    """Returns a QUERY to deduplicate rows within a table

    Args:
        relation: table or CTE name to deduplicate
        partition_by: column names, expressions, or cast expressions to use to identify a set/window of rows out of which to select one as the deduplicated row
        order_by: A list of strings representing the ORDER BY clause

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlglot.schema import MappingSchema
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@deduplicate(demo.table, [user_id, cast(timestamp as date)], ['timestamp desc', 'status asc'])"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT * FROM demo.table QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, CAST(timestamp AS date) ORDER BY timestamp DESC, status ASC) = 1'
    """
    # Construct the PARTITION BY clause
    partition = exp.Tuple(expressions=[
        exp.cast(col, "date") if isinstance(col, exp.Cast) else col
        for col in partition_by
    ])

    # Construct the ORDER BY clause
    order_expressions = []
    for order_item in order_by:
        parts = order_item.split()
        if len(parts) == 2 and parts[1].upper() in ('ASC', 'DESC'):
            column, direction = parts
            expr = exp.Column(this=column)
            if direction.upper() == 'DESC':
                expr = exp.Ordered(this=expr, desc=True)
            else:
                expr = exp.Ordered(this=expr, desc=False)
            order_expressions.append(expr)
        else:
            order_expressions.append(exp.Column(this=order_item))

    order = exp.Order(expressions=order_expressions)

    # Construct the window function
    window_function = exp.Window(
        this=exp.RowNumber(),
        partition_by=partition,
        order=order
    )

    # Construct the QUALIFY clause
    qualify_clause = exp.EQ(
        this=window_function,
        expression=exp.Literal.number(1)
    )

    # Construct the final query
    query = (
        exp.Select(expressions=[exp.Star()])
        .from_(relation)
        .qualify(qualify_clause)
    )

    return query

# Test the macro
sql = "@deduplicate(test_table, [user_id, cast(timestamp as date)], ['timestamp desc', 'status asc'])"
print(MacroEvaluator().transform(parse_one(sql)).sql())