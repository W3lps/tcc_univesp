import pandas as pd
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

def get_spark():
    return SparkSession.builder \
        .appName('TCC - Extract Data') \
        .getOrCreate()

### Funções ###
def extract_veiculos(file_path: str, ano: str):
    print('Puxando o Excel para Pandas')
    df_pd = pd.read_excel(
        file_path,
        sheet_name=f'VEICULOS_{ano}',
        dtype=str
    )

    spark = get_spark()
    print('Convertendo Pandas -> Spark')
    df = spark.createDataFrame(df_pd)

    return df


def extract_censo(nivel: str):
    spark = SparkSession.builder.getOrCreate()

    paths = {
        "setor": {
            "basico": "./data/censo/basico/raw/setores/Agregados_por_setores_basico_BR.csv",
            "entorno": "./data/censo/caracteristicas_domicilio/raw/setores/Agregados_por_setores_entorno_domicílios_BR.csv",
            "rendimento": "./data/censo/rendimento_responsavel/raw/setores/Agregados_por_setores_renda_responsavel_BR.csv",
        },
        "distrito": {
            "basico": "./data/censo/basico/raw/distritos/Agregados_por_distritos_basico_BR.csv",
            "entorno": "./data/censo/caracteristicas_domicilio/raw/distritos/Agregados_por_distritos_entorno_domicílios_BR.csv",
            "rendimento": "./data/censo/rendimento_responsavel/raw/distritos/Agregados_por_distritos_renda_responsavel_BR.csv",
        }
    }

    if nivel not in paths:
        raise ValueError("nivel deve ser 'setor' ou 'distrito'")

    df_basico = spark.read.csv(
        paths[nivel]["basico"],
        sep=";",
        header=True,
        encoding="ISO-8859-1"
    )

    df_entorno = spark.read.csv(
        paths[nivel]["entorno"],
        sep=";",
        header=True,
        encoding="ISO-8859-1"
    )

    df_rendimento = spark.read.csv(
        paths[nivel]["rendimento"],
        sep=";",
        header=True,
        encoding="ISO-8859-1"
    )

    return df_basico, df_entorno, df_rendimento