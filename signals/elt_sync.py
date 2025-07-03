import typing as t

from sqlmesh import signal, DatetimeRanges, ExecutionContext
from sqlglot import exp

# add the context argument to your function
@signal()
def elt_sync(batch: DatetimeRanges, context: ExecutionContext, upstream_ref: exp.Table, downstream_ref: exp.Table, date_column: exp.Column) -> bool:

    upstream_max_date = context.engine_adapter.fetchdf(f"SELECT max({date_column}) from {upstream_ref}").iloc[0, 0]
    current_max_date_this_model = context.engine_adapter.fetchdf(f"SELECT max({date_column}) from {downstream_ref}").iloc[0, 0]

    if upstream_max_date > current_max_date_this_model:
        print(f'Upstream ref has more date intervals than this model. Triggering run for: {downstream_ref}')
        return True
    else:
        print(f'Upstream ref does not have more date intervals than this model. Not triggering run for: {downstream_ref}')
        return False