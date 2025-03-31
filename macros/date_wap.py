"""a SQLMesh python macro that generates a surrogate key based on the fields provided"""

from sqlglot import exp
from sqlmesh import macro
import os

@macro("start_date_wap")
def start_date_wap(evaluator):
    if os.getenv("START_DATE_WAP") is None:
        return exp.TimestampSub(
            this=exp.CurrentTimestamp(),
            expression=exp.Literal.number(1),
            unit="DAY")
    else:
        return os.getenv("START_DATE_WAP")

@macro("end_date_wap")
def end_date_wap(evaluator):
    if os.getenv('END_DATE_WAP') is None:
        return exp.CurrentTimestamp()
    else:
        return os.getenv('END_DATE_WAP')
