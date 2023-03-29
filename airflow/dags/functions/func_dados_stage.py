import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

def dados_stage():

  
    caminho = r'./data/vendas-combustiveis-m3.xlsx'
    planilha = pd.read_excel(caminho, skiprows=2, sheet_name=None)

    df_stage = pd.DataFrame()
    for i in planilha:
        if i != 'Plan1':
            df_stage = pd.concat([df_stage, planilha[i]])
   
    print('SHAPE:')
    print(df_stage.shape)

    
    pg_hook = PostgresHook(postgres_conn_id='postgres-airflow')
    for _, row in df_stage.iterrows():

        pg_hook.run(f" INSERT INTO stage_derivados_petroleo \
                    VALUES ('{str(row.COMBUSTÍVEL)}', '{str(row.ANO)}', '{str(row.REGIÃO)}', '{str(row.ESTADO)}', '{str(row.UNIDADE)}', '{str(row.Jan)}', '{str(row.Fev)}', '{str(row.Mar)}', '{str(row.Abr)}', '{str(row.Mai)}', '{str(row.Jun)}', '{str(row.Jul)}', '{str(row.Ago)}', '{str(row.Set)}', '{str(row.Out)}', '{str(row.Nov)}', '{str(row.Dez)}', '{str(row.TOTAL)}')")


