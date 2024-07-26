"""a SQLMesh python macro that generates a query to deduplicate rows within a table"""

from sqlglot import exp
from sqlmesh import macro

@macro()
def deduplicate(evaluator, relation: exp.Table, partition_by: list[exp.Column], order_by: list[exp.Ordered]) -> exp.Query:
    """Returns a QUERY to deduplicate rows within a table

    Args:
        relation: table or CTE name to deduplicate
        partition_by: column names (or expressions) to use to identify a set/window of rows out of which to select one as the deduplicated row
        order_by: column names (or expressions) that determine the priority order of which row should be chosen if there are duplicates

    Example:
        >>> from sqlglot import parse_one
        >>> from sqlglot.schema import MappingSchema
        >>> from sqlmesh.core.macros import MacroEvaluator
        >>> sql = "@deduplicate(demo.table, [user_id, cast(timestamp as date)], [exp.desc('timestamp'), exp.desc('effective_sequence')])"
        >>> MacroEvaluator().transform(parse_one(sql)).sql()
        'SELECT * FROM demo.table QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id, CAST(timestamp AS date) ORDER BY timestamp DESC, effective_sequence DESC) = 1'
    """
    # Construct the PARTITION BY clause
    partition = exp.Tuple(expressions=partition_by)

    # Construct the ORDER BY clause
    order = exp.Tuple(expressions=order_by)

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