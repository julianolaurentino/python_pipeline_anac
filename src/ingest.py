import sqlalchemy
import pandas as pd
import glob
import os   

# -- Constantes --

INPUT_PATH = '/home/obzen/Documentos/workspace/dataset/anac_combinada/parquet'
GLOB_PATH = os.path.join(INPUT_PATH, '*.parquet')
PARQUET_FILES = glob.glob(GLOB_PATH)
print(f"Arquivos parquet encontrados: {len(PARQUET_FILES)}")

# -- Funções --

# Função para carregar os arquivos Parquet e concatená-los em um único DataFrame
def carregar_parquet_files(parquet_files):
    dataframes = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dataframes.append(df)
            print(f"Arquivo '{file}' carregado com sucesso.")
        except Exception as e:
            print(f"Erro ao carregar o arquivo '{file}': {e}")
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        print(f"DataFrames combinados com sucesso. Total de registros: {len(combined_df)}")
        return combined_df
    else:
        print("Nenhum DataFrame foi carregado. Verifique os arquivos Parquet.")
        return pd.DataFrame()
if __name__ == "__main__":
    df_combinado = carregar_parquet_files(PARQUET_FILES)    
print('DataFrame carregado com sucesso!')



# %%
df_combinado.head()