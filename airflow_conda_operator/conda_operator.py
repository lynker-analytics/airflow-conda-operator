import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Sequence

from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.providers.standard.operators.python import ExternalPythonOperator
else:
    from airflow.operators.python import ExternalPythonOperator  # type: ignore[no-redef]

from jinja2 import Environment, PackageLoader, select_autoescape


class CondaPythonOperator(ExternalPythonOperator):
    """
    Run a function in a mamba/conda env.
    """

    template_fields: Sequence[str] = tuple(
        {"conda_env", "conda_root_prefix"}
        | set(ExternalPythonOperator.template_fields) - {"python"}
    )

    def __init__(
        self,
        *,
        conda_env: str = "base",
        conda_root_prefix: str | None = None,
        **kwargs,
    ):
        # todo: alternatively, get this from the output of `conda --json`` field "conda_prefix"
        if conda_root_prefix is None:
            conda_root_prefix = Path(os.environ["CONDA_EXE"]).parents[1]
        else:
            conda_root_prefix = Path(conda_root_prefix)
        if not (conda_root_prefix / "bin" / "activate").is_file():
            raise ValueError(f"`activate` script not found in {conda_root_prefix}")
        self.conda_root_prefix = str(conda_root_prefix)
        self.conda_env = conda_env
        self.interpreter_name = "python"

        super().__init__(
            python=self._get_python_path(),
            **kwargs,
        )

    def _create_python_stub(self) -> str:
        templating_env = Environment(
            loader=PackageLoader("airflow_conda_operator"),
            autoescape=select_autoescape(),  # todo: check this
        )
        python_stub_script = templating_env.get_template("conda_python_env.sh").render(
            {
                "conda_root_prefix": self.conda_root_prefix,
                "interpreter_name": self.interpreter_name,
                "conda_env": self.conda_env,
            }
        )
        python_stub_prefix = f"conda-python-{self.conda_env.replace('/', '-')}-"
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

    def _get_python_path(self) -> str:
        """
        get the full executable path of the python interpreter
        """
        python_stub = self._create_python_stub()
        try:
            return subprocess.check_output(
                [python_stub, "-c", "import sys; print(sys.executable)"],
                text=True,
            ).strip()
        finally:
            os.unlink(python_stub)

    def _execute_python_callable_in_subprocess(self, _) -> Any | None:
        python_stub = self._create_python_stub()
        try:
            return super()._execute_python_callable_in_subprocess(python_stub)
        finally:
            os.unlink(python_stub)
