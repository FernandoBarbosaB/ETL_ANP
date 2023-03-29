import pandas as pd

def monta_df_final_mes(index, var_data, mes, df):
    var_data = var_data
    var_value = df.loc[index, mes]
    var_uf = df.loc[index, 'ESTADO']
    var_product = df.loc[index, 'COMBUSTÍVEL']
    d = {'year_month':var_data,'uf': var_uf,'product': var_product,'unit': 'm3','volume': var_value,'created_at': datetime.now()}
    linha = pd.DataFrame(data=d, index=[0])
    df_teste = pd.DataFrame({})
    df_teste = pd.concat([df_teste, linha], ignore_index=True)
    return df_teste
