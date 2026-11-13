# Airflow Task and Operator for Conda/Mamba

Runs a Python function inside a fully initialized conda/mamba environment.

Compatible with airflow V2 and V3.

## Background

The Conda/Mamba execution environment manager is popular with python
extensions, which have more dependencies than common system libraries.
Some of the complex binary packages require additional environment variables
set up, not only the path to the python executable and a custom `PYTHONPATH`.

The operator/task provided by this project uses the `conda activate` [scripts
supplied by the conda packages](https://conda-forge.org/docs/maintainer/adding_pkgs/#activate-scripts)
(see `<env-prefix>/etc/conda/activate.d/`).
Thus it will ensure the correct setup of the essential environment variables
like `GDAL_DATA`, `GDAL_DRIVER_PATH`, `PROJ_DATA`, `PROJ_NETWORK`, for e.g.
spatial python packages (`rasterio`, `geopandas`).

## Install

Please install from source/github:

```bash
pip install git+ssh://git@github.com/lynker-analytics/airflow-conda-operator.git
```

(Packaging is on the ToDo list...)

## Task Example

Use the operator in your Airflow DAG file:

```python3
from airflow.decorators import task

@task.conda(conda_env=["rasterio=1.4"], expect_airflow=False)
def load_geotiffs(data_location):
    # IMPORTANT: all imports inside the function
    import rasterio
    with rasterio.open(data_location) as img:
        # do something with it
        pass
```

## Operator Example

Use the operator in your Airflow DAG file:

```python3
from airflow_conda_operator.conda_operator import CondaPythonOperator

# to be executed in the environment satellite-data
def load_geotiffs(data_location):
    # IMPORTANT: all imports inside the function
    import rasterio
    with rasterio.open(data_location) as img:
        # do something with it
        pass

# the operator will define the task in the airflow DAG
CondaPythonOperator(
    task_id="load_geotiffs",
    op_args=["s3://bucket/data.tif"],
    conda_env=["rasterio=1.4"],
    expect_airflow=False,
    python_callable=load_geotiffs
)
```

### Environment Specification

If `conda_env` contains a dictionary or list, it will create (and cache)
the environment when and where (e.g. on a worker node) required.

The above examples work with:

```python
conda_env={
    "name": "satellite-data", 
    "dependencies": [
        "rasterio=1.4",
        ],
    }
```

A list can be used to specify the dependencies only, the name defaults to `airflow-conda`.

A dictionary requires "dependencies", optionally allows "name"
and "channels", just as a conda environment definition file. If no name is specified,
it will default to `airflow-conda`.

Pip dependencies can be specified by adding `{"pip": ["pkg1", ....]}` to the dependency
list.

An already created environment can be specified by name or absolute path
e.g. `conda_env="satellite-data"` or `conda_env="/home/user/miniconda/envs/satellite-data"`.

### Environment Caching


The environments are cached on the workers with an LRU cache located in
`$HOME/.cache/airflow-conda-operator-envs`.
A maximum of 5 environments are kept separately for each environment name.

### Requirements

Requires Python >= 3.12 and `bash`, and one of these conda/mamba installations
(`mamba` is used if present in the installation's root prefix):

| tool | supported versions | reason |
|------|--------------------|--------|
| conda | >= 24.9 | older `conda env remove --json` produces no output, 24.1/24.3 ignore `--json` for `env create` with pip dependencies |
| mamba | >= 2.0.3 | 2.0.0 lacks `list --full-name`, 2.0.1/2.0.2 omit pip-installed packages from `list` |

The versions are checked when the operator accesses the installation; an unsupported
version raises a `CondaVersionError` asking to update it, e.g. with
`mamba update -n base conda mamba`.
The root prefix needs the `activate` script of the conda package (`<root>/bin/activate`),
as in Miniforge installations. mamba 1 is not supported.

Some OS distributions do not allow executable files in `/tmp`.
If you experience `PermissionDenied` errors using the operator,
specify another directory to create the temporary stub bash scripts:

Set the environment variable `TMPDIR` when starting the scheduler and worker
processes (see [mkstemp](https://docs.python.org/3/library/tempfile.html#tempfile.mkstemp)).

### Implementation

This implementation is "bolted-on" onto the `ExternalPython` operator
in the most straight forward way, but it does the trick.

### Development

The tests are based on `pytest`.

```shell
pytest
```
