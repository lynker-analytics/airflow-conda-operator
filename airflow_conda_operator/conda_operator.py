import os
import tempfile
from pathlib import Path
from typing import Sequence

from airflow.operators.python import ExternalPythonOperator
from jinja2 import Environment, PackageLoader, select_autoescape


class CondaPythonOperator(ExternalPythonOperator):
    """
    Run a function in a mamba/conda env.
    """

    template_fields: Sequence[str] = tuple({"conda_env", "conda_root_prefix"} | set(ExternalPythonOperator.template_fields))

    def __init__(
        self,
        *,
        python="python",
        conda_env: str = "base",
        conda_root_prefix: str | None = None,
        **kwargs,
    ):
        if conda_root_prefix is None:
            conda_root_prefix = Path(os.environ["CONDA_EXE"]).parents[1]
        else:
            conda_root_prefix = Path(conda_root_prefix)
        if not (conda_root_prefix / "bin" /"activate").is_file():
            raise ValueError(f"`activate` script not found in {conda_root_prefix}")
        self.conda_root_prefix = str(conda_root_prefix)
        self.conda_env = conda_env
        self.interpreter_name = python
        super().__init__(
            python=python,
            **kwargs,
        )

    def execute_callable(self):
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
        try:
            python_stub.write(python_stub_script.encode())
            python_stub.close()
            # might go wrong, some OS do not allow executables in /tmp
            os.chmod(python_stub.name, 0o755)
            self.python = python_stub.name
            try:
                return super().execute_callable()
            finally:
                self.python = self.interpreter_name
        finally:
            os.unlink(python_stub.name)
