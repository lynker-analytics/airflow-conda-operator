# https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#unit-tests
import datetime

import pendulum
import pytest

# from airflow.sdk import DAG
from airflow import DAG
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from airflow_conda_operator.conda_operator import CondaPythonOperator

# todo: mock instead of using airflow installation:
# clear DB of all test DAG runs
DATA_INTERVAL_START = pendulum.now(tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)

TEST_DAG_ID = "conda_operator_test_dag"
TEST_TASK_ID = "conda_operator_test_task"
TEST_RUN_ID = f"conda_operator_test_dag_run_{DATA_INTERVAL_START.isoformat()}"

def python_prog():
    return 1

@pytest.fixture()
def dag():
    with DAG(
        dag_id=TEST_DAG_ID,
        schedule="@daily",
        start_date=DATA_INTERVAL_START,
    ) as dag:
        CondaPythonOperator(
            conda_env={"dependencies": ["python"]},
            python_callable=python_prog,
            task_id=TEST_TASK_ID,
            expect_airflow=True,
        )
    return dag


def test_conda_operator_execute_w_dependencies(dag):
    dagrun = dag.create_dagrun(
        run_id=TEST_RUN_ID,
        logical_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        run_type=DagRunType.MANUAL,
        run_after=datetime.datetime.now(),
        triggered_by=DagRunTriggeredByType.TIMETABLE,
        state=DagRunState.RUNNING,
        start_date=DATA_INTERVAL_END,
    )
    ti = dagrun.get_task_instance(task_id=TEST_TASK_ID)
    ti.task = dag.get_task(task_id=TEST_TASK_ID)
    ti.run(ignore_ti_state=True)

    assert ti.state == TaskInstanceState.SUCCESS

    # Assert something related to tasks results.
    assert ti.xcom_pull() == 1

# run another operator test
# todo: understand how to avoid copying/renaming

DATA_INTERVAL_START1 = pendulum.now(tz="UTC")
DATA_INTERVAL_END1 = DATA_INTERVAL_START1 + datetime.timedelta(days=1)

TEST_DAG_ID1 = "conda_operator_test_dag1"
TEST_TASK_ID1 = "conda_operator_test_task1"
TEST_RUN_ID1 = f"conda_operator_test_dag_run1_{DATA_INTERVAL_START1.isoformat()}"

@pytest.fixture()
def dag1():
    with DAG(
        dag_id=TEST_DAG_ID1,
        schedule="@daily",
        start_date=DATA_INTERVAL_START1,
    ) as dag:
        CondaPythonOperator(
            conda_env="base",
            python_callable=python_prog,
            task_id=TEST_TASK_ID1,
            expect_airflow=True,
        )
    return dag


def test_conda_operator_w_name(dag1):
    dagrun = dag1.create_dagrun(
        run_id=TEST_RUN_ID1,
        logical_date=DATA_INTERVAL_START1,
        data_interval=(DATA_INTERVAL_START1, DATA_INTERVAL_END1),
        run_type=DagRunType.MANUAL,
        run_after=datetime.datetime.now(),
        triggered_by=DagRunTriggeredByType.TIMETABLE,
        state=DagRunState.RUNNING,
        start_date=DATA_INTERVAL_END1,
    )
    ti = dagrun.get_task_instance(task_id=TEST_TASK_ID1)
    ti.task = dag1.get_task(task_id=TEST_TASK_ID1)
    ti.run(ignore_ti_state=True)

    assert ti.state == TaskInstanceState.SUCCESS

    # Assert something related to tasks results.
    assert ti.xcom_pull() == 1
