from typing import TYPE_CHECKING, Callable

from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.bases.decorator import task_decorator_factory
    from airflow.providers.standard.decorators.external_python import (
        _PythonExternalDecoratedOperator,
    )
else:
    from airflow.decorators.base import task_decorator_factory  # type: ignore[no-redef]
    from airflow.decorators.external_python import _PythonExternalDecoratedOperator  # type: ignore[no-redef]

from .conda_operator import CondaPythonOperator

if TYPE_CHECKING:
    from airflow.sdk.bases.decorator import TaskDecorator


class _CondaPythonDecoratedOperator(
    _PythonExternalDecoratedOperator, CondaPythonOperator
):
    """Wraps a Python callable and captures args/kwargs when called for execution."""

    template_fields = CondaPythonOperator.template_fields
    custom_operator_name: str = "@task.conda"


def conda_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> "TaskDecorator":
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_CondaPythonDecoratedOperator,
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
