import glob
import os
import pathlib

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv

load_dotenv()


# -- Constantes --

INPUT_PATH = pathlib.Path(os.getenv("DATA_PATH", "data/"))
GLOB_PATH = str(INPUT_PATH / "*.parquet")
PARQUET_FILES = glob.glob(GLOB_PATH)

CONNECTION_STRING = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'airflow')}"
    f":{os.getenv('POSTGRES_PASSWORD', 'airflow')}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}"
    f":{os.getenv('POSTGRES_PORT', '5432')}"
    f"/{os.getenv('POSTGRES_DB', 'anac_dw')}"
)

TABLE_NAME = "voos"
SCHEMA_NAME = "raw"
CHUNKSIZE = 50_000  # linhas por batch no INSERT — evita estourar memória


# -- Funções --


def carregar_parquet(file_path: pathlib.Path) -> pd.DataFrame | None:
    """Lê um único arquivo .parquet e retorna o DataFrame."""
    try:
        df = pd.read_parquet(file_path)
        print(f"  Lido: {file_path.name} ({len(df):,} linhas)")
        return df
    except Exception as e:
        print(f"  Erro ao ler '{file_path.name}': {e}")
        return None


def ja_carregado(
    engine: sqlalchemy.Engine, schema: str, table: str, arquivo: str
) -> bool:
    """
    Verifica se um arquivo já foi carregado anteriormente consultando
    a coluna de controle '_arquivo_origem'.
    Evita duplicatas em reprocessamentos.
    """
    query = sqlalchemy.text(
        f"SELECT 1 FROM {schema}.{table} WHERE _arquivo_origem = :arquivo LIMIT 1"
    )
    try:
        with engine.connect() as conn:
            resultado = conn.execute(query, {"arquivo": arquivo}).fetchone()
            return resultado is not None
    except Exception:
        # Tabela ainda não existe — primeira carga
        return False


def salvar_em_banco(
    df: pd.DataFrame,
    engine: sqlalchemy.Engine,
    table_name: str,
    schema_name: str,
    arquivo_origem: str,
) -> bool:
    """
    Salva o DataFrame no PostgreSQL usando append + chunksize.
    Adiciona coluna de controle '_arquivo_origem' para rastreabilidade
    e evitar recargas duplicadas.
    """
    df = df.copy()
    df["_arquivo_origem"] = arquivo_origem

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists="append",  # nunca apaga dados existentes
            index=False,
            chunksize=CHUNKSIZE,  # insere em batches de 50k linhas
            method="multi",  # INSERT com múltiplas linhas por query (mais rápido)
        )
        print(f"  Salvo em {schema_name}.{table_name} ({len(df):,} linhas)")
        return True
    except Exception as e:
        print(f"  Erro ao salvar '{arquivo_origem}': {e}")
        return False


def garantir_schema(engine: sqlalchemy.Engine, schema: str) -> None:
    """Cria o schema no PostgreSQL se ainda não existir."""
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))


def processar_carga(parquet_files: list[str]) -> None:
    """
    Orquestra a carga de todos os .parquet para o PostgreSQL.
    Arquivos já carregados anteriormente são ignorados (idempotente).
    """
    total = len(parquet_files)
    sucessos = 0
    pulados = 0
    falhas = []

    print(f"\n{'=' * 60}")
    print(f"Iniciando carga de {total} arquivo(s) → {SCHEMA_NAME}.{TABLE_NAME}")
    print(f"{'=' * 60}")

    engine = sqlalchemy.create_engine(CONNECTION_STRING)
    garantir_schema(engine, SCHEMA_NAME)

    for i, file in enumerate(parquet_files, start=1):
        file_path = pathlib.Path(file)
        print(f"\n[{i}/{total}] {file_path.name}")

        # Controle de idempotência — pula se já foi carregado
        if ja_carregado(engine, SCHEMA_NAME, TABLE_NAME, file_path.name):
            print("  Já carregado anteriormente — pulando.")
            pulados += 1
            continue

        df = carregar_parquet(file_path)
        if df is None:
            falhas.append(file_path.name)
            continue

        ok = salvar_em_banco(df, engine, TABLE_NAME, SCHEMA_NAME, file_path.name)
        if ok:
            sucessos += 1
        else:
            falhas.append(file_path.name)

    engine.dispose()

    print(f"\n{'=' * 60}")
    print("Carga concluída.")
    print(f"  Carregados : {sucessos}/{total}")
    print(f"  Pulados    : {pulados}/{total}")
    print(f"  Falhas     : {len(falhas)}/{total}")
    if falhas:
        print("  Arquivos com falha:")
        for nome in falhas:
            print(f"    - {nome}")
    print(f"{'=' * 60}\n")


# -- Ponto de entrada --

if __name__ == "__main__":
    print(f"Arquivos .parquet encontrados: {len(PARQUET_FILES)}")

    if not PARQUET_FILES:
        print(f"Nenhum arquivo x.parquet em: {INPUT_PATH}")
    else:
        processar_carga(PARQUET_FILES)
