### Configuração do .env


```
AIRFLOW_UID=1000
_PIP_ADDITIONAL_REQUIREMENTS='requests pandas ipdb pyarrow'

AIRFLOW_CONN_MY_GIT_CONN='{
    "conn_type": "git",
    "host": "https://github.com/fmarcon/dags-airflow3.git",
    "password": "<token>"
}'

AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST='[
    {
        "name": "BundleGit",
        "classpath": "airflow.providers.git.bundles.git.GitDagBundle",
        "kwargs": {
            "git_conn_id": "my_git_conn",
            "subdir": "dags",  
            "tracking_ref": "main"  
        }
    }
]'
```