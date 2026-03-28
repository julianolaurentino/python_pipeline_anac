import glob
import pathlib

import chardet
import pandas as pd
import polars as pl

# -- Constantes --

INPUT_PATH = pathlib.Path("/home/obzen/Documentos/workspace/dataset/anac_combinada")
OUTPUT_PATH = pathlib.Path(
    "/home/obzen/Documentos/workspace/python_pipeline_anac/data/raw"
)
GLOB_PATH = str(INPUT_PATH / "*.txt")
TXT_FILES = glob.glob(GLOB_PATH)

# Encodings testados em ordem de prioridade (do mais comum ao menos comum)
# Usado apenas como fallback se chardet não conseguir detectar com confiança
ENCODINGS_FALLBACK = [
    "utf-8",
    "utf-8-sig",  # UTF-8 com BOM — comum em exports do Excel
    "cp1252",  # Windows-1252 — padrão em sistemas Windows BR
    "iso-8859-1",  # Latin-1 — arquivos legados
    "cp860",  # MS-DOS Portuguese
    "latin-1",
    "utf-16",
]


# -- Funções --


def detectar_encoding(file_path: pathlib.Path, amostra_bytes: int = 50_000) -> str:
    """
    Detecta o encoding do arquivo usando chardet.
    Lê apenas os primeiros `amostra_bytes` bytes para ser eficiente em arquivos grandes.
    Retorna o encoding detectado ou 'utf-8' como padrão seguro.
    """
    with open(file_path, "rb") as f:
        amostra = f.read(amostra_bytes)

    resultado = chardet.detect(amostra)
    encoding = resultado.get("encoding") or "utf-8"
    confianca = resultado.get("confidence", 0)

    print(f"  Encoding detectado: '{encoding}' (confiança: {confianca:.0%})")

    # Se confiança baixa, usa cp1252 como padrão seguro para arquivos BR
    if confianca < 0.7:
        print("Confiança baixa — usando 'cp1252' como fallback seguro")
        return "cp1252"

    return encoding


def ler_arquivo_txt(file_path: pathlib.Path) -> pd.DataFrame | None:
    """
    Lê um arquivo .txt da ANAC (CSV separado por ';').
    Tenta primeiro com chardet, depois percorre ENCODINGS_FALLBACK.
    Retorna o DataFrame ou None em caso de falha total.
    """
    file_path = pathlib.Path(file_path)
    print(f"\nLendo: {file_path.name}")

    # Tentativa 1: encoding detectado automaticamente
    encoding_detectado = detectar_encoding(file_path)
    try:
        df = pd.read_csv(
            file_path, sep=";", encoding=encoding_detectado, low_memory=False
        )
        print(f"  Lido com sucesso ({len(df):,} linhas, {len(df.columns)} colunas)")
        return df
    except UnicodeDecodeError:
        print(f"  Falha com '{encoding_detectado}' — percorrendo fallbacks...")

    # Tentativa 2: percorrer lista de fallback
    for encoding in ENCODINGS_FALLBACK:
        if encoding == encoding_detectado:
            continue  # já tentou
        try:
            df = pd.read_csv(file_path, sep=";", encoding=encoding, low_memory=False)
            print(
                f"  Lido com sucesso usando fallback '{encoding}' ({len(df):,} linhas)"
            )
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"  Erro inesperado com '{encoding}': {e}")
            continue

    # Tentativa 3: último recurso — substitui caracteres inválidos
    print(
        "Todos os encodings falharam — usando errors='replace' (pode haver caracteres corrompidos)"
    )
    try:
        df = pd.read_csv(
            file_path, sep=";", encoding="utf-8", errors="replace", low_memory=False
        )
        print(f"  Lido com substituição de caracteres ({len(df):,} linhas)")
        return df
    except Exception as e:
        print(f"  Falha total ao ler '{file_path.name}': {e}")
        return None


def processar_arquivos_txt(txt_files: list[str]) -> None:
    """
    Orquestra a leitura de todos os .txt e a conversão para .parquet.
    Arquivos com falha são registrados e ignorados sem interromper o processo.
    """
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    total = len(txt_files)
    sucessos = 0
    falhas = []

    print(f"\n{'=' * 60}")
    print(f"Iniciando processamento de {total} arquivo(s)...")
    print(f"{'=' * 60}")

    for i, file in enumerate(txt_files, start=1):
        file_path = pathlib.Path(file)
        print(f"\n[{i}/{total}]", end=" ")

        df = ler_arquivo_txt(file_path)

        if df is None:
            falhas.append(file_path.name)
            print(f"  IGNORADO: '{file_path.name}'")
            continue

        # Salvar como Parquet via Polars (mais eficiente que pandas.to_parquet)
        parquet_name = file_path.stem + ".parquet"
        parquet_path = OUTPUT_PATH / parquet_name

        try:
            pl.from_pandas(df).write_parquet(parquet_path)
            print(f"  Salvo em: {parquet_path}")
            sucessos += 1
        except Exception as e:
            print(f"  Erro ao salvar parquet '{parquet_name}': {e}")
            falhas.append(file_path.name)

    # Resumo final
    print(f"\n{'=' * 60}")
    print("Processamento concluído.")
    print(f"  Sucessos : {sucessos}/{total}")
    print(f"  Falhas   : {len(falhas)}/{total}")
    if falhas:
        print("Arquivos com falha:")
        for nome in falhas:
            print(f"    - {nome}")
    print(f"{'=' * 60}\n")


# -- Ponto de entrada --

if __name__ == "__main__":
    print(f"Arquivos .txt encontrados: {len(TXT_FILES)}")

    if not TXT_FILES:
        print(f"Nenhum arquivo .txt encontrado em: {INPUT_PATH}")
    else:
        processar_arquivos_txt(TXT_FILES)
