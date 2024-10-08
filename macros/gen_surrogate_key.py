"""a SQLMesh python macro that generates a surrogate key based on the fields provided"""

from sqlglot import exp
from sqlmesh import macro


@macro("gen_surrogate_key")
def gen_surrogate_key(evaluator, field_list: list) -> exp.SHA2:
    """
    Generates a surrogate key by concatenating provided fields,
    treating null values with a specific placeholder,
    then applying a hash function to the result using the database's native SHA256 algorithm.

    Args:
    - field_list: List of field names to be included in the surrogate key.

    Example:
    - gen_surrogate_key(["field1", "field2"])
    - In a SQL model: select @gen_surrogate_key([orders.order_id, orders.customer_id]) as surrogate_key from orders

    Returns: An expression (SQLGlot) representing the SQL for the generated surrogate key.
    """

    if len(field_list) < 2:
        raise ValueError(
            "At least two fields are required to generate a surrogate key."
        )

    # Assuming columns are instances of a specific class, e.g., Column
    if not all(isinstance(field, exp.Column) for field in field_list):
        raise ValueError("All fields must be column objects.")

    default_null_value = "_null_"

    expressions = []
    for i, field in enumerate(field_list):
        coalesce_expression = exp.Coalesce(
            this=exp.cast(
                expression=field, to="TEXT"
            ),  # Adjusted to use the field directly
            expressions=exp.Literal.string(default_null_value),
        )
        expressions.append(coalesce_expression)
        if i < len(field_list) - 1:  # Add separator except for the last element
            expressions.append(exp.Literal.string("-"))

    concat_exp = exp.Concat(expressions=expressions)
    hash_exp = exp.SHA2(this=concat_exp, length=exp.Literal.number(256))

    return hash_exp
