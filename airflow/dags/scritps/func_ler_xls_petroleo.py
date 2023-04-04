import pandas as pd

def ler_xls():
    
    caminho = r'./data/vendas-combustiveis-m3-anp-ate-jan-2023.xlsx'
    planilha = pd.read_excel(caminho, skiprows=2, sheet_name=None)
    df_stage = pd.DataFrame()
    for i in planilha:
        if i != 'Plan1':
            df_stage = pd.concat([df_stage, planilha[i]])
   
    print('SHAPE:')
    print(df_stage.shape)

    df_stage.to_parquet(r'./data/petroleo.parquet', index=False)