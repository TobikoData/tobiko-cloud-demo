"""a SQLMesh python macro that generates a query to deduplicate rows within a table"""

from sqlglot import exp
from sqlmesh import macro
from sqlglot import parse_one
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.utils.errors import MacroEvalError, SQLMeshError


@macro()
def deduplicate(
    evaluator,
    relation: exp.Expression,
    partition_by: list[exp.Expression],
    order_by: list[str],
) -> exp.Query:
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
    # Debugging: Print the type and content of the relation
    # print(f"Relation type: {type(relation)}")
    # print(f"Relation content: {relation}")

    # # Validate the relation is a table or CTE
    # if not isinstance(relation, (exp.Table, exp.CTE)):
    #     raise SQLMeshError("The relation must be a table or CTE.")

    # Construct the PARTITION BY clause
    partition = exp.Tuple(
        expressions=[
            col
            if not isinstance(col, exp.Cast)
            else exp.Cast(this=col.this, to=col.args.get("to"))
            for col in partition_by
        ]
    )

    # Construct the ORDER BY clause with validation
    order_expressions = []
    for order_item in order_by:
        parts = order_item.split()
        if len(parts) == 2 and parts[1].upper() in ("ASC", "DESC"):
            column, direction = parts
            expr = exp.Column(this=column)
            if direction.upper() == "DESC":
                expr = exp.Ordered(this=expr, desc=True)
            else:
                expr = exp.Ordered(this=expr, desc=False)
            order_expressions.append(expr)
        elif len(parts) == 1:
            order_expressions.append(exp.Column(this=order_item))
        else:
            raise SQLMeshError(
                f"Invalid order_by clause: {order_item}. Only 'asc' and 'desc' are allowed."
            )

    order = exp.Order(expressions=order_expressions)

    # Construct the window function
    window_function = exp.Window(
        this=exp.RowNumber(), partition_by=partition, order=order
    )

    # get the first unique row
    first_unique_row = exp.EQ(this=window_function, expression=exp.Literal.number(1))

    # Construct the final query
    query = (
        exp.Select(expressions=[exp.Star()]).from_(relation).qualify(first_unique_row)
    )

    return query


# Test the macro
# sql = "@deduplicate(test_table, [user_id, cast(timestamp as date)], ['timestamp desc', 'status asc'])"
# print(MacroEvaluator().transform(parse_one(sql)).sql())