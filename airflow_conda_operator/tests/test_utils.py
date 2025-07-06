import subprocess

import pytest

import airflow_conda_operator.utils.conda_envs
from airflow_conda_operator.utils.conda_envs import CondaSystem, ensure_conda_env

# all right - how to call the tests?


def test_basic_creation():

    conda = CondaSystem()

    env_name = "airflow-env-test"

    env_prefix = conda.create_env(
        {"name": env_name, "dependencies": ["python"]},
    )

    assert env_prefix.name == env_name

    assert conda.find_env_prefix(env_name)

    conda.delete_env(env_name)

    assert not env_prefix.exists()


def test_cached_env():

    conda = CondaSystem()

    new_env = ensure_conda_env({"dependencies": ["python=3.13", "pandas=2.3"]}, conda)

    python_exe = new_env / "bin" / "python"

    python_version = subprocess.check_output([python_exe, "--version"], text=True)

    assert python_version.startswith("Python 3.13.")

    pandas_version = subprocess.check_output(
        [python_exe, "-c", "import pandas; print(pandas.__version__)"], text=True
    )

    assert pandas_version.startswith("2.3.")


def test_cache_eviction(monkeypatch):

    conda = CondaSystem()

    # temporarily reduce cache size
    temp_cache_size = 2
    monkeypatch.setattr(
        airflow_conda_operator.utils.conda_envs,
        "AIRFLOW_CONDA_OPERATOR_ENV_CACHE_MAXSIZE",
        2,
    )
    example_env = {"name": "test-max-cache", "dependencies": ["python=3.13"]}

    # evict first after 2 others were created
    first_env = ensure_conda_env(example_env, conda)
    for repeat in range(temp_cache_size):
        ensure_conda_env(
            example_env
            | {"dependencies": example_env["dependencies"] + ["pip"] * (repeat + 1)},
            conda,
        )
    assert not first_env.exists()

    # don't evict when requested repeatedly
    first_env = ensure_conda_env(example_env, conda)
    for repeat in range(temp_cache_size):
        ensure_conda_env(example_env, conda)
        ensure_conda_env(
            example_env
            | {"dependencies": example_env["dependencies"] + ["pip"] * (repeat + 1)},
            conda,
        )
    assert first_env.exists()


def test_failed_env_creation():
    conda = CondaSystem()

    with pytest.raises(subprocess.CalledProcessError):
        conda.create_env({"dependencies": ["python<1"]})
