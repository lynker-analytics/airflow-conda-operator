import copy
import logging
import os
import re
import socket
import tempfile
from typing import Any, Sequence

from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.providers.standard.operators.python import \
        ExternalPythonOperator
else:
    from airflow.operators.python import ExternalPythonOperator  # type: ignore[no-redef]

from jinja2 import Environment, PackageLoader, select_autoescape

from .utils.conda_envs import CondaSystem, ensure_conda_env

logger = logging.getLogger(__name__)


class CondaPythonOperator(ExternalPythonOperator):
    """
    Run a function in a mamba/conda environment `conda_env`.

    The environment can be named (i.e. a manually created environment), as a
    list (or tuple) of dependencies or specified as a dictionary.
    This dictionary can contain the keys "dependencies", "channels", and "name".

    Environments specified by their dependencies (i.e. dictionary) are cached.
    At max 5 versions per "name" are kept in a LRU cache.
    """

    template_fields: Sequence[str] = tuple(
        {"conda_env", "conda_root_prefix"}
        | set(ExternalPythonOperator.template_fields) - {"python"}
    )

    def __init__(
        self,
        *,
        conda_env: str | dict = "base",
        conda_root_prefix: str | None = None,
        **kwargs,
    ):
        self.conda_root_prefix = conda_root_prefix
        if isinstance(conda_env, str):
            self.conda_env = conda_env
        elif isinstance(conda_env, dict):
            self.conda_env = copy.deepcopy(conda_env)
        elif isinstance(conda_env, (list, tuple)):
            self.conda_env = {
                "dependencies": [copy.deepcopy(dependency) for dependency in conda_env]
            }
        self.interpreter_name = "python"

        super().__init__(
            python=self.interpreter_name,
            **kwargs,
        )

    def _create_python_stub(self) -> str:
        conda = CondaSystem(self.conda_root_prefix)

        # todo: if creation fails, raise exception stopping all task instances?
        conda_env_prefix = ensure_conda_env(self.conda_env, conda)

        logger.info(
            "creating python stub for prefix %s on %s",
            conda_env_prefix,
            socket.gethostname(),
        )

        templating_env = Environment(
            loader=PackageLoader("airflow_conda_operator"),
            autoescape=select_autoescape(),  # todo: check this
        )
        python_stub_script = templating_env.get_template("conda_python_env.sh").render(
            {
                "conda_root_prefix": conda.conda_root,
                "interpreter_name": self.interpreter_name,
                "conda_env": str(conda_env_prefix.absolute()),
            }
        )
        python_stub_prefix = f"conda-python-{conda_env_prefix.name}-"
        python_stub = tempfile.NamedTemporaryFile(
            prefix=python_stub_prefix, suffix=".sh", delete=False
        )
        python_stub.write(python_stub_script.encode())
        python_stub.close()
        try:
            os.chmod(python_stub.name, 0o755)
        except PermissionError:
            # might go wrong, some OS/distributions do not allow executables in /tmp
            # Add error hint: consider setting TMPDIR environment variable
            raise
        return python_stub.name

    def execute_callable(self) -> Any | None:
        # bypass all checks, as the environment is created and
        # and checked in _create_python_stub
        # todo: python_version: check whether python 2 or 3?
        return self._execute_python_callable_in_subprocess(None)

    def _execute_python_callable_in_subprocess(self, _) -> Any | None:
        python_stub = self._create_python_stub()
        try:
            return super()._execute_python_callable_in_subprocess(python_stub)
        finally:
            os.unlink(python_stub)

    def _get_pkg_version_from_target_env(self, pkg_name: str) -> str | None:
        conda = CondaSystem()
        return next(
            (
                pkg["version"]
                for pkg in conda.list_packages(
                    ensure_conda_env(self.conda_env, conda),
                    regex=re.escape(pkg_name),
                    full_name=True,
                )
                if pkg["name"] == pkg_name
            ),
            None,
        )

    def _is_pendulum_installed_in_target_env(self) -> bool:
        # edited copy from ExternalPythonOperator
        # assuming the pendulum package is installed via conda or pip
        try:
            return self._get_pkg_version_from_target_env("pendulum") is not None
        except Exception as e:
            if self.expect_pendulum:
                self.log.warning(
                    "When checking for Pendulum installed in conda environment got %s",
                    e,
                )
                self.log.warning(
                    "Pendulum is not properly installed in the conda environment %s"
                    "Pendulum context keys will not be available. "
                    "Please Install Pendulum or Airflow in your conda environment to access them.",
                    self.conda_env,
                )
            return False

    def _get_airflow_version_from_target_env(self) -> str | None:
        # edited copy from ExternalPythonOperator
        # assuming the airflow package is installed via conda or pip

        from airflow import __version__ as airflow_version
        from airflow.exceptions import AirflowConfigException

        try:
            target_airflow_version = self._get_pkg_version_from_target_env(
                "apache-airflow"
            )
            if target_airflow_version != airflow_version:
                raise AirflowConfigException(
                    f"The version of Airflow installed for the {self.python} "
                    f"({target_airflow_version}) is different than the runtime Airflow version: "
                    f"{airflow_version}. Make sure your environment has the same Airflow version "
                    f"installed as the Airflow runtime."
                )
            return target_airflow_version
        except Exception as e:
            if self.expect_airflow:
                self.log.warning(
                    "When checking for Airflow installed in virtual environment got %s",
                    e,
                )
                self.log.warning(
                    "This means that Airflow is not properly installed by %s. "
                    "Airflow context keys will not be available. "
                    "Please Install Airflow %s in your environment to access them.",
                    self.python,
                    airflow_version,
                )
            return None
