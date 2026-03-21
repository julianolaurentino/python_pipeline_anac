import glob
import os
import pandas as pd
import sqlalchemy

# -- Constantes --

INPUT_PATH = "/home/obzen/Documentos/workspace/dataset/anac_combinada/parquet"
GLOB_PATH = os.path.join(INPUT_PATH, "*.parquet")
PARQUET_FILES = glob.glob(GLOB_PATH)
CONNECTION_STRING = "postgresql://username:password@localhost:5432/mydatabase"
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
        print(
            f"DataFrames combinados com sucesso. Total de registros: {len(combined_df)}"
        )
        return combined_df
    else:
        print("Nenhum DataFrame foi carregado. Verifique os arquivos Parquet.")
        return pd.DataFrame()


# funcão para salvar o DataFrame combinado em um banco de dados SQL
def salvar_em_banco(df, connection_string, table_name):
    try:
        engine = sqlalchemy.create_engine(connection_string)
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
        print(f"DataFrame salvo com sucesso na tabela '{table_name}'.")
    except Exception as e:
        print(f"Erro ao salvar o DataFrame no banco de dados: {e}")