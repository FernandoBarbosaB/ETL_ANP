import pandas as pd
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook




def monta_df_final_mes(index, var_data, mes, df):
    var_data = var_data
    var_value = df.loc[index, mes]
    var_uf = df.loc[index, 'ESTADO']
    var_product = df.loc[index, "COMBUSTÍVEL"]
    d = {'year_month':var_data,'uf': var_uf,'product': var_product,'unit': 'm3','volume': var_value,'created_at': datetime.now()}
    linha = pd.DataFrame(data=d, index=[0])
    df_teste = pd.DataFrame({})
    df_teste = pd.concat([df_teste, linha], ignore_index=True)
    return df_teste

def dados_final():


    

    meses = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez' ]


    anos = range(2000, 2023)
    for ano in anos:
        globals()[f'data_{ano}'] = [f'{ano}-{mes}-01' for mes in range(1, 13)]

    anos_dict = {ano: globals().get(f"data_{ano}") for ano in anos}

    pg_hook = PostgresHook(postgres_conn_id='postgres-airflow')
    caminho = r'./data/vendas-combustiveis-m3.xlsx'
    planilha = pd.read_excel(caminho, skiprows=2, sheet_name=None)


    df_aux = pd.DataFrame()
    for i in planilha:
        if i != 'Plan1':
            df = planilha[i]
            print('SHAPE:')
            print(df.shape)

            for index, row in df.iterrows():

                for ano in anos:
                    if df.loc[index, 'ANO'] == int(ano):
                        for i, mes in enumerate(meses):
                            df_aux = pd.concat([df_aux, monta_df_final_mes(index ,anos_dict[(ano)][i], mes, df)], ignore_index=True)
                        

    print('SHAPE:')
    print(df_aux.shape)
    print(df_aux)

    pg_hook = PostgresHook(postgres_conn_id='postgres-airflow')
    for _, row in df_aux.iterrows():

	
        pg_hook.run(f" INSERT INTO final_derivados_petroleo (year_month, uf, product, unit, volume, created_at ) \
                    VALUES ('{str(row.year_month)}', '{str(row.uf)}', '{str(row.product)}', '{str(row.unit)}', '{str(row.volume)}', '{str(row.created_at)}')")

