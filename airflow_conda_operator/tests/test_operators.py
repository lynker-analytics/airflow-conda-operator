# https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#unit-tests
import pendulum

from airflow.sdk import DAG
from airflow.utils.state import TaskInstanceState

from airflow_conda_operator.conda_operator import CondaPythonOperator

# todo: mock instead of using airflow installation:
# clear DB of all test DAG runs (?)


def python_prog():
    return 1


def test_conda_operator_execute_w_dependencies():
    TEST_TASK_ID = "conda_operator_test_task"

    with DAG(
        dag_id="conda_operator_test_dag",
        schedule="@daily",
        start_date=pendulum.now(tz="UTC"),
    ) as dag:
        CondaPythonOperator(
            conda_env={"dependencies": ["python"]},
            python_callable=python_prog,
            task_id=TEST_TASK_ID,
            expect_airflow=True,
        )
    dagrun = dag.test()
    ti = dagrun.get_task_instance(task_id=TEST_TASK_ID)

    assert ti.state == TaskInstanceState.SUCCESS

    # Assert something related to tasks results.
    assert ti.xcom_pull() == 1


# run another operator test


def test_conda_operator_w_name():
    TEST_TASK_ID1 = "conda_operator_test_task1"

    with DAG(
        dag_id="conda_operator_test_dag1",
        schedule="@daily",
        start_date=pendulum.now(tz="UTC"),
    ) as dag1:
        CondaPythonOperator(
            conda_env="base",
            python_callable=python_prog,
            task_id=TEST_TASK_ID1,
            expect_airflow=True,
        )
    dagrun = dag1.test()
    ti = dagrun.get_task_instance(task_id=TEST_TASK_ID1)

    assert ti.state == TaskInstanceState.SUCCESS

    # Assert something related to tasks results.
    assert ti.xcom_pull() == 1


def test_failed_env_creation_is_not_hidden_by_version_checks(monkeypatch):
    # the optional pendulum/airflow checks create the environment first; a failure
    # there must fail the task instead of continuing with a broken environment
    import pytest

    import airflow_conda_operator.conda_operator as conda_operator
    from airflow_conda_operator.utils.conda_envs import CondaEnvCreationError

    def failing_ensure_conda_env(env_def, conda):
        raise CondaEnvCreationError("download failed")

    monkeypatch.setattr(conda_operator, "ensure_conda_env", failing_ensure_conda_env)

    operator = CondaPythonOperator(
        conda_env={"dependencies": ["python"]},
        python_callable=python_prog,
        task_id="conda_operator_failing_env",
        expect_airflow=False,
        expect_pendulum=False,
    )
    with pytest.raises(CondaEnvCreationError):
        operator._is_pendulum_installed_in_target_env()
    with pytest.raises(CondaEnvCreationError):
        operator._get_airflow_version_from_target_env()


def test_pkg_version_lookup_with_mamba2_list_format(monkeypatch):
    import airflow_conda_operator.conda_operator as conda_operator
    from airflow_conda_operator.utils.conda_envs import CondaSystem

    monkeypatch.setattr(conda_operator, "ensure_conda_env", lambda env_def, conda: "env")
    monkeypatch.setattr(CondaSystem, "find_env_prefix", lambda self, env_name: env_name)
    monkeypatch.setattr(
        CondaSystem,
        "execute_with_conda_cli",
        lambda self, arguments: {
            "log_history": [],
            "packages": [{"name": "pendulum", "version": "3.1.0"}],
        },
    )
    operator = CondaPythonOperator(
        conda_env={"dependencies": ["python", "pendulum"]},
        python_callable=python_prog,
        task_id="conda_operator_mamba2_list",
        expect_airflow=False,
    )
    assert operator._get_pkg_version_from_target_env("pendulum") == "3.1.0"
    assert operator._is_pendulum_installed_in_target_env()
