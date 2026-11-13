import json
import pathlib
import shutil
import subprocess

import pytest

import airflow_conda_operator.utils.conda_envs
from airflow_conda_operator.utils.conda_envs import (
    ENV_COMPLETE_MARKER,
    CondaEnvCreationError,
    CondaSystem,
    CondaVersionError,
    check_cli_version,
    is_supported_version,
    ensure_conda_env,
    parse_cli_json,
    parse_cli_versions,
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


@pytest.mark.parametrize(
    "output, expected",
    [
        ("conda 26.7.1\n", {"conda": (26, 7, 1)}),
        ("mamba 1.5.8\nconda 24.7.1\n", {"mamba": (1, 5, 8), "conda": (24, 7, 1)}),
        ("2.9.0\n", {"mamba": (2, 9, 0)}),
        ("conda 25.1.0.post12+gabcdef\n", {"conda": (25, 1, 0)}),
        ("something else\n", {}),
    ],
)
def test_parse_cli_versions(output, expected):
    assert parse_cli_versions(output) == expected


def _fake_executable(tmp_path, name, version_output):
    executable = tmp_path / name
    executable.write_text(f"#!/bin/sh\nprintf '{version_output}'\n")
    executable.chmod(0o755)
    return executable


@pytest.mark.parametrize(
    "tool, version, supported",
    [
        ("conda", (24, 7, 1), False),
        ("conda", (24, 9, 0), True),
        ("conda", (26, 7, 1), True),
        ("mamba", (1, 5, 8), False),
        ("mamba", (1, 5, 9), True),
        ("mamba", (2, 0, 0), False),
        ("mamba", (2, 0, 2), False),
        ("mamba", (2, 0, 3), True),
        ("mamba", (2, 9, 0), True),
    ],
)
def test_supported_versions(tool, version, supported):
    assert is_supported_version(tool, version) == supported


def test_check_cli_version_too_old(tmp_path):
    old_conda = _fake_executable(tmp_path, "conda", "conda 24.7.1\\n")
    with pytest.raises(CondaVersionError, match=r"conda 24\.7\.1 \(supported: >=24\.9\)"):
        check_cli_version(old_conda)
    with pytest.raises(CondaVersionError, match=r"mamba 2\.0\.0 .*>=2\.0\.3"):
        check_cli_version(_fake_executable(tmp_path, "mamba", "2.0.0\\n"))


def test_check_cli_version_mamba1_checks_bundled_conda(tmp_path):
    mamba = _fake_executable(tmp_path, "mamba", "mamba 1.5.8\\nconda 24.7.1\\n")
    with pytest.raises(CondaVersionError, match="mamba 1.5.8.*conda 24.7.1"):
        check_cli_version(mamba)
    ok = tmp_path / "ok"
    ok.mkdir()
    assert check_cli_version(
        _fake_executable(ok, "mamba", "mamba 1.5.12\\nconda 24.11.3\\n")
    ) == {"mamba": (1, 5, 12), "conda": (24, 11, 3)}


def test_check_cli_version_unknown(tmp_path):
    with pytest.raises(CondaVersionError, match="cannot determine"):
        check_cli_version(_fake_executable(tmp_path, "mamba", "who knows\\n"))


def test_installed_version_is_supported():
    versions = CondaSystem().versions
    assert all(is_supported_version(tool, version) for tool, version in versions.items())


def test_parse_cli_json_with_leading_text():
    # `conda env create --json` with pip dependencies prints pip's output first
    stdout = (
        "Warning: you have pip-installed dependencies in your environment file\n"
        "Installing pip dependencies: ...working... Ran pip subprocess with arguments:\n"
        "['python', '-m', 'pip', 'install', '-U', '-r', 'condaenv.txt']\n"
        "Pip subprocess output:\nSuccessfully installed six-1.17.0\n\ndone\n"
        '{\n  "prefix": "/envs/x",\n  "success": true\n}\n'
    )
    assert parse_cli_json(stdout) == {"prefix": "/envs/x", "success": True}
    assert parse_cli_json('[{"name": "python"}]') == [{"name": "python"}]
    with pytest.raises(json.JSONDecodeError):
        parse_cli_json("no json here\n")


def test_conda_env_with_pip_dependencies(env_cache):
    conda = CondaSystem()
    env_path = ensure_conda_env(
        {"name": "test-pip", "dependencies": ["python", "pip", {"pip": ["six"]}]}, conda
    )
    assert any(pkg["name"] == "six" for pkg in conda.list_packages(str(env_path)))
