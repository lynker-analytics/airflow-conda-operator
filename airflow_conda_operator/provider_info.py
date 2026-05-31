def get_provider_info():
    return {
        "package-name": "airflow-conda-operator",
        "name": "Conda",
        "description": "Conda/Mamba environment integration",
        "integrations": [
            {"integration-name": "Conda"},
        ],
        "task-decorators": [
            {
                "name": "conda",
                "class-name": "airflow_conda_operator.conda_task.conda_task",
            }
        ],
        "operators": [
            {
                "integration-name": "Conda",
                "python-modules": ["airflow_conda_operator.conda_operator.conda_operator"],
            }
        ],
    }
