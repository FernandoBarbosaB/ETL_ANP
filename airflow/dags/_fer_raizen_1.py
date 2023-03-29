from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

from functions import func_dados_stage,  func_dados_final




with DAG('raizen_fer_derivados_petroleo', schedule_interval = None,
         start_date = datetime.now(), catchup = False,
         template_searchpath = '/opt/airflow/sql') as dag:
    
    create_table_stage = PostgresOperator(
        task_id = 'create_table_stage',
        postgres_conn_id = 'postgres-airflow',
        sql = 'create_table.sql'
    )

    create_table_final = PostgresOperator(
        task_id = 'create_table_final',
        postgres_conn_id = 'postgres-airflow',
        sql = 'final_create_table.sql'
    )
 

    dados_stage_op = PythonOperator(
        task_id = 'dados_stage_op',
        python_callable = func_dados_stage.dados_stage
    )

    dados_final_op = PythonOperator(
        task_id = 'dados_final_op',
        python_callable = func_dados_final.dados_final
    )
    


create_table_stage >> create_table_final >> dados_stage_op >> dados_final_op

