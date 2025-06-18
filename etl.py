import pandas as pd
import os
import glob

# Uma função de Extract que lê e consolida os JSON

def extrair_dados_e_consolidar(pasta: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index = True)
    return df_total

# Uma função que transforma
def calcular_kpi_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df

# Uma função que da load em csv

def carregar_dados(df: pd.DataFrame, format_saida: list):
    """
    Parâmetro que vai ser ou CSV ou Parquet ou Os Dois
    """
    for formato in format_saida:
        if formato == 'csv':
            df.to_csv("dados.csv")
        if formato == 'parquet':
            df.to_csv("dados.parquet")

def pipeline_calcular_kpi_de_vendas_consolidado():
    data_frame = extrair_dados_e_consolidar(pasta)
    data_frame_calculado = calcular_kpi_total_de_vendas(data_frame)
    carregar_dados(data_frame_calculado)
