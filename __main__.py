from src.pyspark.extract import ( extract_veiculos, extract_censo )
from src.pyspark.transform import ( transform_veiculos, split_veiculos_by_quarter, transform_censo )
from src.pyspark.load import ( save_dataframe, save_veiculos_setor_partitioned_parquet )

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def process_veiculos(year: str) -> None:
    print(f"\n{'=' * 30}")
    print(f"Gravando {year}")
    print(f"{'=' * 30}")

    file_path = f"./data/criminalidade/raw/VeiculosSubtraidos_{year}.xlsx"

    print("Extração")
    df_raw = extract_veiculos(file_path, year)

    # df_raw = df_raw.filter(col("cidade") == "S. PAULO")

    print("Transformação")
    df = transform_veiculos(df=df_raw, year=year)

    df.printSchema()

    veiculos_outputpath = (
        f"data/criminalidade/processed/veiculos/veiculos_{year}"
    )

    print("Iniciando Gravação")
    print(file_path, " - ", veiculos_outputpath)

    save_dataframe(
        df,
        output_path=veiculos_outputpath,
        format_type="csv",
        mode="overwrite"
    )

    print(f"DataFrame {year} salvo com sucesso")


def validate_null_data_ocorrencia(years: list[str]) -> None:
    spark = (
        SparkSession.builder
        .appName("Validate Null data_ocorrencia_bo")
        .getOrCreate()
    )

    for year in years:
        input_path = (
            f"data/criminalidade/processed/veiculos/"
            f"veiculos_{year}/"
        )

        print(f"\n{'=' * 40}")
        print(f"Validando {year}")
        print(f"{'=' * 40}")

        df = (
            spark.read
            .option("header", True)
            .option("encoding", "ISO-8859-1")
            .csv(input_path)
        )

        total = df.count()

        total_null = (
            df.filter(col("data_ocorrencia_bo").isNull())
            .count()
        )

        total_empty = (
            df.filter(col("data_ocorrencia_bo") == "")
            .count()
        )

        print(f"Total linhas: {total}")
        print(f"Null: {total_null}")
        print(f"Empty string: {total_empty}")


def process_censo(niveis: list[str]) -> None:
    for nivel in niveis:
        print(f"\n{'=' * 40}")
        print(f"Processando Censo - {nivel}")
        print(f"{'=' * 40}")

        try:
            print("Extração")
            df_basico, df_entorno, df_rendimento = extract_censo(nivel)

            print("Transformação")
            dfs_censo = transform_censo(
                df_basico=df_basico,
                df_entorno=df_entorno,
                df_rendimento=df_rendimento,
                nivel=nivel
            )

            print("Gravação")
            for nome_dataset, df in dfs_censo.items():
                output_path = f"data/censo/{nome_dataset}/processed/{nivel}"
                print('Out - ', output_path) 
                print(f"Salvando {nome_dataset} - {nivel}")
            #     df.printSchema()
            #     df.show(5, truncate=False)

                save_dataframe(
                    df,
                    output_path=output_path,
                    format_type="parquet",
                    mode="overwrite"
                )

                print(f"Salvo em: {output_path}")

        except Exception as e:
            print(f"Erro ao processar Censo - {nivel}")
            print(f"Tipo do erro: {type(e).__name__}")
            print(f"Mensagem: {str(e)}")


def main():
    years = ["2025", "2024", "2023", "2022", "2021", "2020", "2019", "2018", "2017"]
    years = ["2021","2020", "2019"]
    for year in years:    
        try:
            process_veiculos(year)

        except Exception as e:
            print(f"Erro ao processar {year}")
            print(f"Tipo do erro: {type(e).__name__}")
            print(f"Mensagem: {str(e)}")

    # Separação por trimestre
    validate_null_data_ocorrencia(years)
    split_veiculos_by_quarter(years)

    # Criação da Tabela Particionada
    save_veiculos_setor_partitioned_parquet(
        input_dir="data/criminalidade/processed/veiculos_setor",
        output_path="data/criminalidade/curated/veiculos_setor_parquet",
        years=years
    )

    process_censo(niveis=['setor', 'distrito'])

if __name__ == "__main__":
    main()