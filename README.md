# Airflow Conda Operator

Fully initialize a conda environment and start a python script.

## Background

Conda/Mamba is popular for python extensions, which need a bit more than one
or the other system library. More complex binary packages require additional
environment variables set up than "just" the PYTHONPATH - often implied in the interpeter path.

Using the `activate` scripts supplied by the conda packages
(see `<env-prefix>/etc/conda/activate.d/`) will also set up essential environment
variables like `GDAL_DATA`, `GDAL_DRIVER_PATH`, `PROJ_DATA`, `PROJ_NETWORK`, for e.g.
`gdal` based packages (`rasterio`, `geopandas`).

## Usage

### Install

Please install from github:

```bash
pip install git+ssh://git@github.com/lynker-analytics/airflow-conda-operator.git
```

(Packaging is on the ToDo list...)

### Example

Create an environment with all requirements for the data processing:

```bash
mamba create -n satellite-data python rasterio
```

Use the operator in your airflow DAG file:

```python3
from airflow_conda_operator import CondaPythonOperator

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
    conda_env="satellite-data",
    expect_airflow=False,
    python_callable=load_geotiffs
)
```

### Requirements

Works with conda/mamba, requires `bash`.

### Implementation

This implementation is "bolted-on" onto the `ExternalPython` operator
in the most straight forward way, but it does the trick.
