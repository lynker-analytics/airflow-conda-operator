from typing import TYPE_CHECKING, Callable

from airflow.decorators.base import task_decorator_factory
from airflow.decorators.python import _PythonDecoratedOperator

from .conda_operator import CondaPythonOperator

if TYPE_CHECKING:
    from airflow.decorators.base import TaskDecorator


class _CondaDecoratedOperator(_PythonDecoratedOperator, CondaPythonOperator):
    # the extra heavy lifting is done by _PythonDecoratedOperator
    custom_operator_name = "@task.conda"


def conda_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> "TaskDecorator":
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_CondaDecoratedOperator,
        **kwargs,
    )


def get_provider_info():
    return {
        "package-name": "airflow-conda-operator",
        "name": "Conda",
        "description": "Conda/Mamba environment integration",
        "integrations": [
            {"integration-name": "Conda"},
        ],
        "task-decorators": [
            {
                "name": "conda",
                "class-name": "airflow_conda_operator.conda_task.conda_task",
            }
        ],
        "operators": [
            {
                "integration-name": "Conda",
                "python-modules": ["airflow_conda_operator.conda_operator"],
            }
        ],
    }
