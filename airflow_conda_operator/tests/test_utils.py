import pathlib
import shutil
import subprocess

import pytest

import airflow_conda_operator.utils.conda_envs
from airflow_conda_operator.utils.conda_envs import (
    ENV_COMPLETE_MARKER,
    CondaEnvCreationError,
    CondaSystem,
    ensure_conda_env,
)

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

    assert any(
        pkg["name"] == "python" and pkg["version"].startswith("3.13.")
        for pkg in conda.list_packages(new_env)
    )

    pandas_version = subprocess.check_output(
        [python_exe, "-c", "import pandas; print(pandas.__version__)"], text=True
    )

    assert pandas_version.startswith("2.3.")

    assert any(
        pkg["name"] == "pandas" and pkg["version"].startswith("2.3.")
        for pkg in conda.list_packages(new_env)
    )


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

    with pytest.raises(CondaEnvCreationError) as error:
        conda.create_env({"dependencies": ["python<1"]})
    # the reason is reported by conda/mamba on stdout (--json), not stderr
    assert "python" in str(error.value) and "no error message" not in str(error.value)
    assert isinstance(error.value.__cause__, subprocess.CalledProcessError)


@pytest.fixture
def env_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(
        airflow_conda_operator.utils.conda_envs,
        "AIRFLOW_CONDA_OPERATOR_ENV_CACHE",
        tmp_path,
    )
    return tmp_path


def test_failed_cached_env_creation_leaves_nothing_behind(env_cache, monkeypatch):
    conda = CondaSystem()
    env_def = {"name": "test-failing", "dependencies": ["python"]}

    def fail_after_creating_prefix(arguments):
        # conda/mamba create the prefix directory before failing (e.g. download error)
        prefix = pathlib.Path(arguments[arguments.index("--prefix") + 1])
        (prefix / "conda-meta").mkdir(parents=True)
        raise subprocess.CalledProcessError(
            1,
            arguments,
            output='{"success": false, "log_history": [{"level": "error", '
            '"message": "Download error: connection reset"}]}',
            stderr="",
        )

    monkeypatch.setattr(conda, "execute_with_conda_cli", fail_after_creating_prefix)

    with pytest.raises(CondaEnvCreationError, match="connection reset"):
        ensure_conda_env(env_def, conda)
    assert not any(path.is_dir() for path in env_cache.iterdir())

    # a later attempt must not pick up a broken environment either
    with pytest.raises(CondaEnvCreationError):
        ensure_conda_env(env_def, conda)


def test_incomplete_cached_env_is_recreated(env_cache):
    conda = CondaSystem()
    env_def = {"name": "test-incomplete", "dependencies": ["python"]}

    env_path = ensure_conda_env(env_def, conda)
    assert (env_path / ENV_COMPLETE_MARKER).is_file()

    # simulate an interrupted creation: directory without python and marker
    shutil.rmtree(env_path)
    (env_path / "conda-meta").mkdir(parents=True)

    assert ensure_conda_env(env_def, conda) == env_path
    assert (env_path / "bin" / "python").is_file()
    assert (env_path / ENV_COMPLETE_MARKER).is_file()


@pytest.mark.parametrize(
    "cli_output",
    [
        # conda
        [{"name": "python", "version": "3.13.1"}],
        # mamba >= 2
        {"log_history": [], "packages": [{"name": "python", "version": "3.13.1"}]},
    ],
)
def test_list_packages_output_formats(cli_output, monkeypatch, tmp_path):
    conda = CondaSystem()
    monkeypatch.setattr(conda, "find_env_prefix", lambda env_name: tmp_path)
    monkeypatch.setattr(conda, "execute_with_conda_cli", lambda arguments: cli_output)

    assert conda.list_packages("some-env") == [{"name": "python", "version": "3.13.1"}]
