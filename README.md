# Airflow Conda Operator

Fully initialize a conda environment and start a python script.

## Background

Conda/Mamba is popular for python extensions, which need a bit more than one
or the other system library. More complex binary packages require additional
environment variables set up than "just" the PYTHONPATH - often implied in the interpeter path.

Using the `activate` scripts supplied by the conda packages will also set
up the correct library paths, essential e.g for gdal (rasterio, geopandas).

## Usage

### Example

```python
from airflow_conda_operator import CondaPythonOperator

def load_geotiffs(data_location):
    import rasterio
    with rasterio.open(data_location) as img:
        # do something with it
        pass

CondaPythonOperator(
            task_id="load_geotiffs",
            op_args="s3://bucket/data.tif",
            conda_env="satellite-data",
            expect_airflow=False,
            python_callable=load_geotiffs
        )
```

### Requirements

works with conda/mamba, requires `bash`.

### Implementation

Yes - admittedly this implementation is "bolted-on" onto the `ExternalPython`
operator in the most straight forward way, but it does the trick.
