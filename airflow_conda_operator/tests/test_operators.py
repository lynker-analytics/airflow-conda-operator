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
