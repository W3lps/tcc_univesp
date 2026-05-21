from pyspark.sql import DataFrame
import traceback
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit 

def get_spark():
    return SparkSession.builder \
        .appName('TCC - Load Data') \
        .getOrCreate()


def save_dataframe(df: DataFrame, output_path: str, format_type: str = "parquet", mode: str = "overwrite") -> None:
    try:
        writer = df.write.mode(mode)

        if format_type == "parquet":
            writer.parquet(output_path)

        elif format_type == "csv":
            (
                df.coalesce(1)
                .write
                .mode(mode)
                .option("header", True)
                .option("encoding", "ISO-8859-1")
                .csv(output_path)
            )

        elif format_type == "json":
            writer.json(output_path)

        else:
            raise ValueError(f"Formato não suportado: {format_type}")

        print(f"DataFrame salvo com sucesso em: {output_path}")

    except Exception as e:
        print("Erro ao salvar o DataFrame.")
        print(f"Tipo do erro: {type(e).__name__}")
        print(f"Mensagem do erro: {str(e)}")

        print("Stack trace completo:")
        traceback.print_exc()


def save_veiculos_setor_partitioned_parquet(input_dir: str, output_path: str, years: list[str]) -> None:
    try:
        spark = get_spark()

        dfs = []
        for year in years:
            for quarter in range(1, 5):
                file_path = f"{input_dir}/veiculos_{year}_{quarter}_setor.csv"

                if not os.path.exists(file_path):
                    print(f"Arquivo não encontrado: {file_path}")
                    continue

                print(f"Lendo: {file_path}")

                df = (
                    spark.read
                    .option("header", True)
                    .option("encoding", "ISO-8859-1")
                    .csv(file_path)
                    .withColumn("ano_particao", lit(year))
                    .withColumn("trimestre_particao", lit(quarter))
                    .withColumn("ano_trimestre", lit(f"{year}_{quarter}"))
                )

                dfs.append(df)

        if not dfs:
            raise ValueError("Nenhum arquivo encontrado.")

        df_final = dfs[0]

        for df in dfs[1:]:
            df_final = df_final.unionByName(df, allowMissingColumns=True)

        (
            df_final.write
            .mode("overwrite")
            .partitionBy("ano_trimestre")
            .parquet(output_path)
        )

        print(f"Parquet particionado salvo em: {output_path}")

    except Exception as e:
        print("Erro ao salvar parquet particionado.")
        print(f"Tipo do erro: {type(e).__name__}")
        print(f"Mensagem: {str(e)}")
        traceback.print_exc()