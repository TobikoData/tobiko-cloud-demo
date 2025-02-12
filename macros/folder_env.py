from sqlmesh import macro
from sqlglot import exp

@macro()
def folder_env(evaluator, model_suffix: str) -> exp.Table:
    folder_env_value = evaluator._path.as_uri().split("/")[-2]
    return folder_env_value + model_suffix
