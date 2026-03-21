import pandas as pd
import polars as pl
import glob
import csv
import os
import pathlib

# -- Constantes --

INPUT_PATH = pathlib.Path('/home/obzen/Documentos/workspace/dataset/anac_combinada')
OUTPUT_PATH = pathlib.Path('/home/obzen/Documentos/workspace/dataset/anac_combinada/parquet')
GLOB_PATH = str(INPUT_PATH / '*.txt')   
TXT_FILES = glob.glob(GLOB_PATH)
print(f"Arquivos encontrados: {TXT_FILES}")
# Tentar diferentes encodings comuns em arquivos brasileiros
ENCONDINGS = [# UTF-8 - Padrão moderno e mais comum
            'utf-8',
            'utf-8-sig',  # UTF-8 com BOM (Byte Order Mark)
                # ISO 8859-1 (Latin-1) - Muito comum em arquivos antigos brasileiros
            'iso-8859-1',
            'latin-1',
            'latin1',
            # Windows Code Pages - Usado por sistemas Windows brasileiros
            'cp1252',      # Windows-1252 (Western European)
            'windows-1252',
            'cp850',       # MS-DOS Latin-1
            'cp437',       # MS-DOS US
            # ISO 8859-15 - Variante do ISO-8859-1 com símbolo do Euro
            'iso-8859-15',
            'latin-9',
            # Outras variantes ISO para português
            'iso-8859-2',  # Central European
            'iso-8859-3',  # South European
            # MacRoman - Usado em Macs antigos
            'mac_roman',
            'macroman',
            # Encodings específicos do Brasil
            'cp860',       # MS-DOS Portuguese
            # UTF-16 e UTF-32 (menos comuns mas possíveis)
            'utf-16',
            'utf-16-le',
            'utf-16-be',
            'utf-32',
            # ASCII (subset de UTF-8, mas pode ajudar)
            'ascii',]


# -- Funções -- 

def ler_arquivos_txt(file_path):
    for encoding in ENCONDINGS:
        try:
            df = pd.read_csv(file_path, sep=';', encoding=encoding, low_memory=False)
            print(f"Arquivo '{file_path}' lido com sucesso usando encoding '{encoding}'")
            return df
        except UnicodeDecodeError:
            print(f"Erro de decodificação com encoding '{encoding}' para o arquivo '{file_path}'. Tentando próximo encoding...")
        except Exception as e:
            print(f"Erro ao ler o arquivo '{file_path}' com encoding '{encoding}': {e}")
    print(f"Não foi possível ler o arquivo '{file_path}' com nenhum dos encodings testados.")
    return None
print("Iniciando a leitura dos arquivos TXT...")
for file in TXT_FILES:
    df = ler_arquivos_txt(file)
    if df is not None:
        # Gerar o nome do arquivo Parquet com base no nome do arquivo TXT
        parquet_file_name = os.path.splitext(os.path.basename(file))[0] + '.parquet'
        parquet_file_path = OUTPUT_PATH / parquet_file_name
        # Salvar o DataFrame como Parquet usando Polars para melhor desempenho
        pl.from_pandas(df).write_parquet(parquet_file_path)
        print(f"Arquivo '{file}' convertido e salvo como '{parquet_file_path}'")
    else:
        print(f"Falha ao processar o arquivo '{file}'. Ele será ignorado.")
print("Processamento concluído para todos os arquivos TXT.")