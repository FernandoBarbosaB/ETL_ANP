from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime
import pandas as pd
from functions import func_ler_xls_petroleo, func_enviar_blob, func_ler_xls_diesel

default_args = {
    'owner': 'Fernando Barbosa',
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
    'description': 'estudos_airflow: etl dados anp'
}



with DAG(
    "fernando_barbosa_anp",
    default_args = default_args,
    schedule_interval=None,
) as dag:
    

    ler_dados_petroleo = PythonOperator(
            task_id = 'ler_dados_petroleo',
            python_callable = func_ler_xls_petroleo.ler_xls
        )

    ler_dados_diesel = PythonOperator(
        task_id = 'ler_dados_diesel',
        python_callable = func_ler_xls_diesel.ler_xls
    )

    enviar_blob = PythonOperator(
        task_id = 'enviar_blob',
        python_callable = func_enviar_blob.salvar_blob
    )

    notebook_databricks = DatabricksSubmitRunOperator(
        task_id='notebook_databricks',
        databricks_conn_id='azure_databricks',
        existing_cluster_id='0303-131524-ssey0mj1',
        notebook_task={
            'notebook_path': '<caminho_notebook_databricks>'
        },
        dag=dag
    )



ler_dados_petroleo >> ler_dados_diesel >> enviar_blob >> notebook_databricks

