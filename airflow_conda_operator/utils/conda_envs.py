import fcntl
import hashlib
import json
import logging
import os
import pathlib
import socket
import subprocess
import tempfile
import typing

import yaml

logger = logging.getLogger(__name__)


class CondaSystem:
    """
    Find/Create/Delete conda environments utilizing the conda/mamba CLI.
    """

    # todo: find a better interface, this might be the most compatible approach at the moment

    @staticmethod
    def find_conda_root_prefix() -> pathlib.Path | None:
        """
        Determine the root prefix of the conda/mamba installation.
        """

        conda_root_prefix = None

        if "CONDA_EXE" in os.environ:
            conda_root_prefix = pathlib.Path(os.environ["CONDA_EXE"]).parents[1]
        elif "CONDA_ROOT" in os.environ:
            conda_root_prefix = pathlib.Path(os.environ["CONDA_ROOT"])
        elif "MAMBA_ROOT_PREFIX" in os.environ:
            conda_root_prefix = pathlib.Path(os.environ["MAMBA_ROOT_PREFIX"])

        if not conda_root_prefix:
            return

        assert conda_root_prefix.is_dir()
        assert (conda_root_prefix / "bin" / "activate").exists()
        return conda_root_prefix

    @staticmethod
    def find_conda_executable(
        conda_root_prefix: typing.Optional[pathlib.Path],
    ) -> pathlib.Path:
        if conda_root_prefix:
            conda_root_prefix = pathlib.Path(conda_root_prefix)
        else:
            conda_root_prefix = CondaSystem.find_conda_root_prefix()

        # or conda?
        mamba_or_conda = (
            "mamba" if (conda_root_prefix / "bin" / "mamba").is_file() else "conda"
        )

        conda_executable = conda_root_prefix / "bin" / mamba_or_conda
        assert conda_executable.is_file()

        return conda_executable

    def __init__(self, conda_root: typing.Optional[pathlib.Path] = None):
        if conda_root:
            self.conda_root = pathlib.Path(conda_root)
        else:
            self.conda_root = self.find_conda_root_prefix()

        if self.conda_root is None:
            raise ValueError("conda/mamba environment prefix not located")

        if not (self.conda_root / "bin" / "activate").is_file():
            raise ValueError(f"`activate` script not found in {self.conda_root}")

        self.conda_exe = self.find_conda_executable(self.conda_root)

    def execute_with_conda_cli(self, arguments: list[str]) -> dict:
        # maybe set conda variables to environment?
        all_arguments = [str(self.conda_exe), *arguments, "--json"]

        logging.debug("executing %s", str(all_arguments))
        cli_result = subprocess.run(
            all_arguments,
            text=True,
            capture_output=True,
        )
        # make sure the error message is captured in exception
        cli_result.check_returncode()

        if cli_result.stderr:
            logger.warning(
                "captured output on stderr while executing %s:\n%s",
                all_arguments,
                cli_result.stderr,
            )

        return json.loads(cli_result.stdout)

    def create_env(
        self,
        env_def: dict,
        conda_env_dir: typing.Optional[pathlib.Path] = None,
    ) -> pathlib.Path:
        """
        creates the environment (will overwrite existing)

        returns the environment prefix path

        env_def: dictionary with name, channels, dependencies
        conda_env_dir: optional location to create the environment directory in
        """

        with tempfile.NamedTemporaryFile(
            prefix="conda-operator-env-",
            suffix=".yaml",
            mode="wt",
            delete_on_close=False,
        ) as f:
            yaml.dump(env_def, f)
            f.close()

            env_location_args = []
            if conda_env_dir:
                assert (
                    "prefix" not in env_def
                ), "only one of prefix or conda_env_dir allowed"
                env_name = env_def.get("name")
                env_location_args = ["--prefix", str(conda_env_dir / env_name)]

            try:
                install_result = self.execute_with_conda_cli(
                    [
                        "env",
                        "create",
                        "--file",
                        f.name,
                        *env_location_args,
                        "--yes",  # automatically accept/override anything
                    ],
                )
            except subprocess.CalledProcessError as e:
                logger.error(
                    "failed to create environment %s:\n%s", str(env_def), e.stderr
                )
                raise

        if logger.getEffectiveLevel() <= logging.DEBUG:
            logger.debug(
                "environment creation results:\n%s",
                json.dumps(install_result, indent=2),
            )

        if not install_result["success"]:
            logger.error(
                "failed to create environment %s:\n%s",
                str(env_def),
                json.dumps(install_result, indent=2),
            )
            # todo: more suitable exception
            assert install_result["success"], "no success"

        env_prefix = pathlib.Path(install_result["prefix"])
        assert env_prefix.is_dir()

        logger.info("created environment %s at %s", str(env_def), str(env_prefix))
        # todo: maybe less (only package names/versions)
        logger.debug("environment details:\n%s ", json.dumps(install_result, indent=2))

        return env_prefix

    def delete_env(
        self,
        env_name: str,
        conda_env_dir: typing.Optional[pathlib.Path] = None,
    ):

        if conda_env_dir:
            env_location_args = ["--prefix", str(conda_env_dir / env_name)]
        else:
            env_location_args = ["--name", env_name]

        try:
            remove_result = self.execute_with_conda_cli(
                [
                    "env",
                    "remove",
                    "--yes",
                    *env_location_args,
                ],
            )
        except subprocess.CalledProcessError as e:
            logger.error(
                "failed to remove environment %s:\n%s", str(env_name), e.stderr
            )
            raise

        if logger.getEffectiveLevel() <= logging.DEBUG:
            logger.debug(
                "environment removal results:\n%s", json.dumps(remove_result, indent=2)
            )

        if not remove_result["success"]:
            logger.error(
                "failed to remove environment %s:\n%s",
                str(env_name),
                json.dumps(remove_result, indent=2),
            )
            # todo: more suitable exception
            assert remove_result["success"], "no success"

        removed_prefix = pathlib.Path(remove_result["prefix"])

        # todo: this might not always be an error
        assert (
            not removed_prefix.exists()
        ), f"env {removed_prefix} not (completely) removed"

        return removed_prefix

    def find_env_prefix(self, env_name: str) -> pathlib.Path:
        """
        Find the environment prefix by environment name and returns the first
        prefix path matching. Warns if multiple matching environment
        directories are found.

        If the env_name is an absolute path

        The `base` environment name will return the base (conda/mamba installation)
        environment.
        """

        # special name `base`
        if env_name == "base":
            return self.conda_root

        # either it is a prefix (abs path) or find on default path/s
        if pathlib.Path(env_name).is_absolute():
            env_prefix = pathlib.Path(env_name)
        else:
            all_known_envs = self.execute_with_conda_cli(["env", "list"])["envs"]
            env_prefix, *other_prefixes = (
                p for p in map(pathlib.Path, all_known_envs) if p.name == env_name
            )
            # warn if more than one
            if other_prefixes:
                logger.warning(
                    "more than one prefix found for environment %s", env_name
                )

        assert (env_prefix / "conda-meta").is_dir()

        return env_prefix

    def list_packages(self, env_name: str, regex=None, full_name=False) -> list[dict]:
        """
        List installed packages of an environment.

        A regular expression can be specified to narrow the results.
        The option `full-name` will limit filtering to a full match.
        
        Pip-installed packages appear with `channel` set to `pypi`.
        """
        filter_args = []
        if full_name:
            filter_args.append("--full-name")
        if regex is not None:
            filter_args.append(regex)

        try:
            return self.execute_with_conda_cli(
                [
                    "list",
                    "--prefix",
                    self.find_env_prefix(env_name),
                    *filter_args,
                ],
            )
        except subprocess.CalledProcessError as e:
            logger.error("failed to list packages %s:\n%s", str(env_name), e.stderr)
            raise


def hash_env_def(env_def: dict) -> str:
    return hashlib.md5(json.dumps(env_def).encode()).hexdigest()


AIRFLOW_CONDA_OPERATOR_ENV_CACHE = (
    pathlib.Path.home() / ".cache" / "airflow-conda-operator-envs"
)
AIRFLOW_CONDA_OPERATOR_ENV_CACHE.mkdir(parents=True, exist_ok=True)
# max size per env name
AIRFLOW_CONDA_OPERATOR_ENV_CACHE_MAXSIZE = 5


def ensure_conda_env(env_def: dict | str, conda: CondaSystem) -> pathlib.Path:
    # access and maintain cache
    # LRU cache with max 5 older versions per environment name

    if isinstance(env_def, str):
        # assume this is a pre-existing environment
        return conda.find_env_prefix(env_def)

    assert "prefix" not in env_def, "don't expect a prefix"

    # from here on, allocate environment in the airflow specific env
    env_hash = hash_env_def(env_def)
    env_name = env_def.get("name", "conda-operator")
    env_name = f"{env_name}-{env_hash[:8]}"
    env_def = env_def | {"name": env_name}

    assert os.pathsep not in env_name
    cached_env_path = AIRFLOW_CONDA_OPERATOR_ENV_CACHE / env_name
    # lock files are used for preventing parallel creation/deletion
    # and to implement a LRU cache
    cached_env_path_lock = cached_env_path.with_suffix(".lock")

    # list all lock files from same name and sort by age
    existing_envs = [
        env_dir
        for env_dir in AIRFLOW_CONDA_OPERATOR_ENV_CACHE.iterdir()
        if env_dir.is_dir() and env_dir.name[:-8] == (env_name[:-8])
    ]

    if cached_env_path in existing_envs:
        # keep requested environment alive
        existing_envs.remove(cached_env_path)

    def last_env_usage(env_dir):
        return env_dir.with_suffix(".lock").stat().st_mtime

    existing_envs = sorted(existing_envs, key=last_env_usage)

    while (
        existing_envs and len(existing_envs) >= AIRFLOW_CONDA_OPERATOR_ENV_CACHE_MAXSIZE
    ):
        # this could/should be done in parallel with the env creation
        env_to_evict = existing_envs.pop(0)  # evict oldest
        logger.info("removing env %s from cache", env_to_evict.name)
        if not env_to_evict.is_dir():
            # maybe in creation? Though unlikely
            continue
        env_lock = env_to_evict.with_suffix(".lock")
        with open(env_lock, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            # todo: handle error
            conda.delete_env(env_to_evict.name, env_to_evict.parent)
        env_lock.unlink()

    with open(cached_env_path_lock, "w") as f:
        # Ensure that cache is not build by parallel workers
        # taken from PythonVirtualEnv operator
        fcntl.flock(f, fcntl.LOCK_EX)

        # might be there now?
        if not cached_env_path.is_dir():
            logger.info(
                "creating env %s on %s with definition: %s",
                cached_env_path,
                socket.gethostname(),
                str(env_def),
            )
            new_env_path = conda.create_env(
                env_def,
                conda_env_dir=AIRFLOW_CONDA_OPERATOR_ENV_CACHE,
            )
            assert new_env_path == cached_env_path
        return cached_env_path
