"""a SQLMesh python macro that generates a surrogate key based on the fields provided"""

from sqlglot import exp
from sqlmesh import macro
from pydantic import BaseModel, Field


class SurrogateKeyInput(BaseModel):
    field_list: list[str] = Field(..., min_items=2)


@macro("gen_surrogate_key")
def gen_surrogate_key(evaluator, field_list: list[str]) -> exp.SHA2:
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

    # Validate input using Pydantic
    SurrogateKeyInput(field_list=field_list)

    default_null_value = "_null_"
    separator = exp.Literal.string("-")

    expressions = []
    for field in field_list:
        if expressions:  # Add separator only between fields
            expressions.append(separator)
        expressions.append(
            exp.Coalesce(
                this=exp.cast(exp.Column(this=field), to=exp.DataType.build("STRING")),
                expressions=[exp.Literal.string(default_null_value)],
            )
        )

    concat_exp = exp.Concat(expressions=expressions)
    hash_exp = exp.SHA2(this=concat_exp, length=exp.Literal.number(256))

    return hash_exp
