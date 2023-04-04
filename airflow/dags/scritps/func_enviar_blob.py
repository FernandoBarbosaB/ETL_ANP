from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

def salvar_blob():
    
    dados_petroleo_path = r'./data/petroleo.parquet'
    dados_diesel_path = r'./data/diesel.parquet'

    azurehook = WasbHook(wasb_conn_id="azure_blob_storage")
    azurehook.load_file(file_path=dados_petroleo_path, container_name="fernando-barbosa", blob_name='estudos_airflow/petroleo.parquet')
    azurehook.load_file(file_path=dados_diesel_path, container_name="fernando-barbosa", blob_name='estudos_airflow/diesel.parquet')
