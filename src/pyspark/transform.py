from pyspark.sql import DataFrame 
from pyspark.sql.functions import ( 
    col, trim, upper, regexp_replace, to_date, to_timestamp, coalesce, when, quarter, hour, log1p
)
import unicodedata
import re

from pyspark.sql import SparkSession


### Limpeza e Normalização ###
def normalize_column_name(name: str) -> str: 
    name = name.lower().strip()

    name= unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))

    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")

    return name


def normalize_columns(df: DataFrame) -> DataFrame:
    for old_name in df.columns:
        df = df.withColumnRenamed(old_name, normalize_column_name(old_name))

    return df


def trim_string_columns(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(field.name, trim(col(field.name)))

    return df


# def clean_null_strings(df):
#     null_values = ["", "nan", "NaN", "NAN", "null", "NULL", "None"]

#     for column_name in df.columns:
#         df = df.withColumn(
#             column_name,
#             when(col(column_name).isin(null_values), None)
#             .otherwise(col(column_name))
#         )

#     return df


def clean_null_strings(df: DataFrame) -> DataFrame:
    null_values = ["", "nan", "NaN", "NAN", "null", "NULL", "None"]

    for column_name in df.columns:
        cleaned_value = trim(col(column_name).cast("string"))

        df = df.withColumn(
            column_name,
            when(cleaned_value.isin(null_values), None)
            .otherwise(col(column_name))
        )

    return df


def drop_unnecessary_columns(df):
    columns_to_drop = [
        "tipo_intolerancia",
        "nome_departamento",
        "flag_ato_infracional",
        "logradouro_versao",
        #"placa_veiculo",
        "flag_intolerancia",
        "desdobramento",
        "circunstancia",
        "descr_periodo",
        "descricao_apresentacao",
        "desc_lei",
    ]

    return df.drop(*[c for c in columns_to_drop if c in df.columns])


# def cast_date_columns(df: DataFrame, columns: list[str]) -> DataFrame:

#     for column_name in columns:
#         if column_name in df.columns:
#             df = df.withColumn(
#                 column_name,
#                 coalesce(
#                     to_date(col(column_name), "yyyy-MM-dd HH:mm:ss"),
#                     to_date(col(column_name), "yyyy-MM-dd"),
#                     to_date(col(column_name), "dd/MM/yyyy")
#                 )
#             )

#     return df

def cast_date_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    for column_name in columns:
        if column_name in df.columns:
            cleaned_value = trim(col(column_name).cast("string"))

            df = df.withColumn(
                column_name,
                when(
                    cleaned_value.rlike(r"^20\d{2}-\d{2}-\d{2}"),
                    to_date(cleaned_value.substr(1, 10), "yyyy-MM-dd")
                )
                .when(
                    cleaned_value.rlike(r"^\d{2}/\d{2}/20\d{2}"),
                    to_date(cleaned_value.substr(1, 10), "dd/MM/yyyy")
                )
                .otherwise(None)
            )

    return df

def uppercase_string_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    for column_name in columns:
        if column_name in df.columns:
            df = df.withColumn(column_name, upper(col(column_name)))

    return df


def cast_timestamp_columns(df: DataFrame, columns: list[str]) -> DataFrame:

    for column_name in columns:
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                coalesce(
                    to_timestamp(col(column_name), "yyyy-MM-dd HH:mm:ss"),
                    to_timestamp(col(column_name), "dd/MM/yyyy HH:mm:ss"),
                    to_timestamp(col(column_name), "yyyy-MM-dd"),
                    to_timestamp(col(column_name), "dd/MM/yyyy")
                )
            )

    return df


def cast_integer_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    for column_name in columns:
        if column_name in df.columns:
            cleaned_value = trim(col(column_name).cast("string"))

            df = df.withColumn(
                column_name,
                when(
                    cleaned_value.rlike(r"^\d+$"),
                    cleaned_value.cast("int")
                ).otherwise(None)
            )

    return df


# def cast_time_columns(df: DataFrame, columns: list[str]) -> DataFrame:

#     for column_name in columns:
#         if column_name in df.columns:
#             df = df.withColumn(
#                 column_name,
#                 when(
#                     col(column_name).isNotNull(),
#                     date_format(
#                         to_timestamp(col(column_name), "HH:mm:ss"),
#                         "HH:mm:ss"
#                     )
#                 )
#             )

#     return df


def cast_time_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    for column_name in columns:
        if column_name in df.columns:
            cleaned_value = trim(col(column_name).cast("string"))

            df = df.withColumn(
                column_name,
                when(
                    cleaned_value.rlike(r"^\d{2}:\d{2}:\d{2}"),
                    regexp_replace(cleaned_value, r"^(\d{2}:\d{2}:\d{2}).*$", r"$1")
                ).otherwise(None)
            )

    return df


def cast_coordinate_columns(df: DataFrame, columns: list[str]) -> DataFrame:

    for column_name in columns:
        if column_name in df.columns:

            cleaned_value = trim(col(column_name).cast("string"))
            cleaned_value = regexp_replace(cleaned_value, ",", ".")

            df = df.withColumn(
                column_name,
                when(
                    cleaned_value.rlike(r"^-?\d+(\.\d+)?$"),
                    cleaned_value.cast("double")
                ).otherwise(None)
            )

    return df


def cast_coordinate_columns(df: DataFrame, columns: list[str]) -> DataFrame:

    for column_name in columns:
        if column_name in df.columns:

            cleaned_value = trim(col(column_name).cast("string"))
            cleaned_value = regexp_replace(cleaned_value, ",", ".")

            df = df.withColumn(
                column_name,
                when(
                    cleaned_value.rlike(r"^-?\d+(\.\d+)?$"),
                    cleaned_value.cast("double")
                ).otherwise(None)
            )

    return df

def create_time_features(df):
    if "hora_ocorrencia" in df.columns:
        df = df.withColumn(
            "hora_ocorrencia_num",
            hour(to_timestamp(col("hora_ocorrencia"), "HH:mm:ss"))
        )

    return df


def create_periodo_dia(df):
    if "hora_ocorrencia_num" in df.columns:
        df = df.withColumn(
            "periodo_dia",
            when(col("hora_ocorrencia_num").between(0, 5), "MADRUGADA")
            .when(col("hora_ocorrencia_num").between(6, 11), "MANHA")
            .when(col("hora_ocorrencia_num").between(12, 17), "TARDE")
            .when(col("hora_ocorrencia_num").between(18, 23), "NOITE")
            .otherwise(None)
        )

    return df


def create_categoria_conduta(df: DataFrame) -> DataFrame:
    if "descr_conduta" in df.columns:
        df = df.withColumn(
            "categoria_conduta",
            when(col("descr_conduta").isin("VEÍCULO", "INTERIOR DE VEÍCULO"), "VEICULO")
            .when(col("descr_conduta").isin("TRANSEUNTE", "PESSOA"), "PESSOA")
            .when(col("descr_conduta").isin("RESIDÊNCIA", "CONDOMÍNIO RESIDÊNCIAL"), "RESIDENCIAL")
            .when(
                col("descr_conduta").isin(
                    "ESTABELECIMENTO COMERCIAL",
                    "INTERIOR ESTABELECIMENTO",
                    "ESTABELECIMENTO-OUTROS"
                ),
                "COMERCIAL"
            )
            .when(col("descr_conduta") == "CARGA", "CARGA")
            .when(col("descr_conduta").isin("COLETIVO", "APLICATIVO DE MOBILIDADE URBANA"), "TRANSPORTE")
            .when(col("descr_conduta").isin("OUTROS", "FIOS E CABOS"), "OUTROS")
            .otherwise("NAO_INFORMADO")
        )

    return df


def create_categoria_rubrica(df: DataFrame) -> DataFrame:
    if "rubrica" in df.columns:
        df = df.withColumn(
            "categoria_rubrica",

            when(col("rubrica").rlike("(?i)^FURTO"), "FURTO")
            .when(col("rubrica").rlike("(?i)^ROUBO"), "ROUBO")
            .when(col("rubrica").rlike("(?i)LOCALIZAÇÃO|APREENSÃO|ENTREGA"), "RECUPERACAO")
            .when(col("rubrica").rlike("(?i)RECEPTAÇÃO"), "RECEPTACAO")
            .when(col("rubrica").rlike("(?i)ADULTERAÇÃO"), "ADULTERACAO")
            .when(col("rubrica").rlike("(?i)ESTELIONATO|APROPRIAÇÃO INDÉBITA"), "FRAUDE")
            .when(col("rubrica").rlike("(?i)COLISÃO|CHOQUE|ABALROAMENTO|TOMBAMENTO"), "ACIDENTE")
            .when(col("rubrica").rlike("(?i)HOMICÍDIO|LESÃO CORPORAL|AMEAÇA|VIOLÊNCIA"), "VIOLENCIA")
            .when(col("rubrica").rlike("(?i)TRÁFICO|DROGAS"), "DROGAS")
            .when(col("rubrica").rlike("(?i)ARMA DE FOGO|ARMA"), "ARMAS")
            .otherwise("OUTROS")
        )

    return df


def create_categoria_ocorrencia_veiculo(df: DataFrame) -> DataFrame:
    if "descr_ocorrencia_veiculo" in df.columns:
        df = df.withColumn(
            "categoria_ocorrencia_veiculo",
            when(col("descr_ocorrencia_veiculo") == "Localizado / Entregue", "RECUPERADO")
            .when(col("descr_ocorrencia_veiculo") == "Furtado", "FURTADO")
            .when(col("descr_ocorrencia_veiculo") == "Roubado", "ROUBADO")
            .otherwise("OUTROS")
        )

    return df


def create_categoria_tipo_veiculo(df: DataFrame) -> DataFrame:
    if "descr_tipo_veiculo" in df.columns:
        df = df.withColumn(
            "categoria_tipo_veiculo",
            when(col("descr_tipo_veiculo").isin(
                "AUTOMOVEL", "CAMINHONETE", "CAMIONETA", "UTILITÁRIO"
            ), "LEVE")
            .when(col("descr_tipo_veiculo").isin(
                "MOTOCICLO", "MOTONETA", "CICLOMOTO"
            ), "MOTOCICLETA")
            .when(col("descr_tipo_veiculo").isin(
                "CAMINHÃO", "CAMINHÃO TRATOR", "SEMI-REBOQUE", "REBOQUE"
            ), "PESADO")
            .when(col("descr_tipo_veiculo").isin(
                "ONIBUS", "MICRO-ONIBUS"
            ), "TRANSPORTE_PUBLICO")
            .when(col("descr_tipo_veiculo").rlike("(?i)TRATOR"), "MAQUINARIO")
            .when(col("descr_tipo_veiculo") == "BICICLETA", "BICICLETA")
            .otherwise("OUTROS")
        )

    return df


def create_categoria_tipolocal(df: DataFrame) -> DataFrame:
    if "descr_tipolocal" in df.columns:
        df = df.withColumn(
            "categoria_tipolocal",
            when(col("descr_tipolocal") == "Via Pública", "VIA_PUBLICA")
            .when(col("descr_tipolocal").isin(
                "Residência", "Condomínio Residencial", "Favela"
            ), "RESIDENCIAL")
            .when(col("descr_tipolocal").isin(
                "Comércio e Serviços", "Centro Comercial/Empresarial",
                "Shopping Center", "Escritório",
                "Estabelecimento Bancário", "Restaurante e Afins",
                "Condomínio Comercial"
            ), "COMERCIAL")
            .when(col("descr_tipolocal").isin(
                "Estacionamento/Garagem"
            ), "ESTACIONAMENTO")
            .when(col("descr_tipolocal").isin(
                "Rodovia/Estrada"
            ), "RODOVIA")
            .when(col("descr_tipolocal").isin(
                "Repartição Pública", "Serviços e Bens Públicos",
                "Saúde", "Terminal/Estação",
                "Estabelecimento de Ensino",
                "Entidade Assistencial"
            ), "PUBLICO")
            .when(col("descr_tipolocal").isin(
                "Area não Ocupada", "Unidade Rural"
            ), "AREA_ABERTA")
            .otherwise("OUTROS")
        )

    return df


def categorize_columns(df: DataFrame) -> DataFrame:
    df = create_categoria_conduta(df)
    df = create_categoria_rubrica(df)
    df = create_categoria_ocorrencia_veiculo(df)
    df = create_categoria_tipo_veiculo(df)
    df = create_categoria_tipolocal(df)
    
    return df


def create_is_recuperado(df: DataFrame) -> DataFrame:
    if "categoria_ocorrencia_veiculo" in df.columns:
        df = df.withColumn(
            "is_recuperado",
            when(col("categoria_ocorrencia_veiculo") == "RECUPERADO", 1).otherwise(0)
        )

    return df


def create_is_furto(df: DataFrame) -> DataFrame:
    if "categoria_rubrica" in df.columns:
        df = df.withColumn(
            "is_furto",
            when(col("categoria_rubrica") == "FURTO", 1).otherwise(0)
        )

    return df


def create_is_roubo(df: DataFrame) -> DataFrame:
    if "categoria_rubrica" in df.columns:
        df = df.withColumn(
            "is_roubo",
            when(col("categoria_rubrica") == "ROUBO", 1).otherwise(0)
        )

    return df


def create_is_violento(df: DataFrame) -> DataFrame:
    if "categoria_rubrica" in df.columns:
        df = df.withColumn(
            "is_violento",
            when(
                col("categoria_rubrica").isin(
                    "ROUBO",
                    "VIOLENCIA"
                ),
                1
            ).otherwise(0)
        )

    return df


def create_flag_columns(df: DataFrame) -> DataFrame:
    df = create_is_recuperado(df)
    df = create_is_furto(df)
    df = create_is_roubo(df)
    df = create_is_violento(df)

    return df



def split_veiculos_by_quarter(years: list[str]) -> None:
    spark = (
        SparkSession.builder
        .appName("Separar Veículos Limpo em Trimestres")
        .getOrCreate()
    )

    for year in years:
        input_path = (
            f"data/criminalidade/processed/veiculos/"
            f"veiculos_{year}/veiculos_{year}_limpo.csv"
        )

        output_dir = (
            f"data/criminalidade/processed/veiculos_trimestre/"
            f"veiculos_{year}"
        )

        print(f"\nSeparando {year} por trimestre")

        df = (
            spark.read
            .option("header", True)
            .option("encoding", "ISO-8859-1")
            .csv(input_path)
        )

        df = df.withColumn(
            "trimestre",
            quarter(col("data_ocorrencia_bo"))
        )

        for trimestre in [1, 2, 3, 4]:
            output_path = (
                f"{output_dir}/"
                f"veiculos_{year}_{trimestre}_limpo"
            )

            (
                df.filter(col("trimestre") == trimestre)
                .drop("trimestre")
                .coalesce(1)
                .write
                .mode("overwrite")
                .option("header", True)
                .option("encoding", "ISO-8859-1")
                .csv(output_path)
            )

            print(f"Trimestre {trimestre} salvo")


def clean_null_like(column_name: str):
    return (
        when(
            trim(col(column_name)).isin("X", "x", ".", "-", ""),
            None
        )
        .otherwise(trim(col(column_name)))
    )


def br_bigint(column_name: str):
    return clean_null_like(column_name).cast("bigint")


def br_double(column_name: str):
    return (
        regexp_replace(clean_null_like(column_name), ",", ".")
        .cast("double")
    )


def transform_censo_basico(df_basico: DataFrame, nivel: str) -> DataFrame:

    if nivel == "setor":
        return (
            df_basico
            .filter(col("nm_mun") == "São Paulo")
            .withColumn("cd_tipo_int", col("cd_tipo").cast("int"))
            .select(
                col("cd_setor"),
                col("situacao").alias("ds_situacao"),
                col("cd_sit").alias("cd_situacao"),
                col("cd_tipo"),
                when(col("cd_tipo_int") == 0, "NAO_ESPECIAL")
                .when(col("cd_tipo_int") == 1, "FAVELA_COMUNIDADE_URBANA")
                .when(col("cd_tipo_int") == 2, "QUARTEL_BASE_MILITAR")
                .when(col("cd_tipo_int") == 3, "ALOJAMENTO_ACAMPAMENTO")
                .when(col("cd_tipo_int") == 4, "SETOR_BAIXO_PATAMAR_DOMICILIAR")
                .when(col("cd_tipo_int") == 5, "AGRUPAMENTO_INDIGENA")
                .when(col("cd_tipo_int") == 6, "UNIDADE_PRISIONAL")
                .when(col("cd_tipo_int") == 7, "CONVENTO_HOSPITAL_ILPI_IACA")
                .when(col("cd_tipo_int") == 8, "AGROVILA_PA")
                .when(col("cd_tipo_int") == 9, "AGRUPAMENTO_QUILOMBOLA")
                .otherwise("OUTROS")
                .alias("categoria_tipo_setor"),
                br_double("area_km2").alias("area_km2"),
                col("nm_uf"),
                col("cd_mun"),
                col("nm_mun"),
                col("cd_dist"),
                col("nm_dist"),
                col("cd_subdist"),
                br_bigint("v0001").alias("qt_pessoas"),
                br_bigint("v0002").alias("qt_domicilios"),
                br_double("v0005").alias("avg_moradores_domiciliosparticularesocupados"),
                br_bigint("v0007").alias("qt_domicilios_particularesocupados"),
                br_bigint("v0008").alias("qt_domicilios_particulares_usococasional"),
                br_bigint("v0009").alias("qt_domicilios_particulares_vagos"),
            )
        )

    if nivel == "distrito":
        return (
            df_basico
            .filter(col("NM_MUN") == "São Paulo")
            # .withColumn("cd_tipo_int", col("cd_tipo").cast("int"))
            .select(
                col("CD_DIST").alias("cd_dist"),
                col("NM_DIST").alias("nm_dist"),
                col("CD_REGIAO").alias("cd_regiao"),
                col("NM_REGIAO").alias("nm_regiao"),
                col("CD_UF").alias("cd_uf"),
                col("NM_UF").alias("nm_uf"),
                col("CD_MUN").alias("cd_mun"),
                col("NM_MUN").alias("nm_mun"),
                br_double("AREA_KM2").alias("area_km2"),
                br_bigint("v0001").alias("qt_pessoas"),
                br_bigint("v0002").alias("qt_domicilios"),
                br_bigint("v0003").alias("qt_domicilios_particulares"),
                br_bigint("v0004").alias("qt_domicilios_coletivos"),
                br_double("v0005").alias("avg_moradores_domiciliosparticularesocupados"),
                br_double("v0006").alias("perc_domicilios_coletivos"),
                br_bigint("v0007").alias("qt_domicilios_particularesocupados"),
                br_bigint("v0008").alias("qt_domicilios_particulares_usococasional"),
                br_bigint("v0009").alias("qt_domicilios_particulares_vagos"),
            )
        )

    raise ValueError("nivel deve ser 'setor' ou 'distrito'")


def transform_censo_entorno(df_entorno: DataFrame, nivel: str) -> DataFrame:

    if nivel == "setor":
        chave_geo = "cd_setor"
    elif nivel == "distrito":
        chave_geo = "cd_dist"
    else:
        raise ValueError("nivel deve ser 'setor' ou 'distrito'")

    return (
        df_entorno
        .select(
            col(chave_geo),

            br_bigint("v05000").alias("qt_domicilios_entorno"),

            br_bigint("v05001").alias("qt_domicilios_face_circulacao_caminhao_onibus"),
            br_bigint("v05002").alias("qt_domicilios_face_circulacao_carro_van"),
            br_bigint("v05003").alias("qt_domicilios_face_circulacao_pedestre_bicicleta_motocicleta"),
            br_bigint("v05004").alias("qt_domicilios_face_circulacao_aquavia"),
            br_bigint("v05005").alias("qt_domicilios_face_circulacao_saltado"),

            br_bigint("v05006").alias("qt_domicilios_face_via_pavimentada_sim"),
            br_bigint("v05007").alias("qt_domicilios_face_via_pavimentada_nao"),
            br_bigint("v05008").alias("qt_domicilios_face_via_pavimentada_nao_declarado"),

            br_bigint("v05009").alias("qt_domicilios_face_bueiro_sim"),
            br_bigint("v05010").alias("qt_domicilios_face_bueiro_nao"),
            br_bigint("v05011").alias("qt_domicilios_face_bueiro_nao_declarado"),

            br_bigint("v05012").alias("qt_domicilios_face_iluminacao_publica_sim"),
            br_bigint("v05013").alias("qt_domicilios_face_iluminacao_publica_nao"),
            br_bigint("v05014").alias("qt_domicilios_face_iluminacao_publica_nao_declarado"),

            br_bigint("v05015").alias("qt_domicilios_face_ponto_onibus_sim"),
            br_bigint("v05016").alias("qt_domicilios_face_ponto_onibus_nao"),
            br_bigint("v05017").alias("qt_domicilios_face_ponto_onibus_nao_declarado"),

            br_bigint("v05018").alias("qt_domicilios_face_via_sinalizada_bicicleta_sim"),
            br_bigint("v05019").alias("qt_domicilios_face_via_sinalizada_bicicleta_nao"),
            br_bigint("v05020").alias("qt_domicilios_face_via_sinalizada_bicicleta_nao_declarado"),

            br_bigint("v05021").alias("qt_domicilios_face_calcada_sim"),
            br_bigint("v05022").alias("qt_domicilios_face_calcada_nao"),
            br_bigint("v05023").alias("qt_domicilios_face_calcada_nao_declarado"),

            br_bigint("v05024").alias("qt_domicilios_face_obstaculo_calcada_sim"),
            br_bigint("v05025").alias("qt_domicilios_face_obstaculo_calcada_nao"),
            br_bigint("v05026").alias("qt_domicilios_face_obstaculo_calcada_nao_declarado"),

            br_bigint("v05027").alias("qt_domicilios_face_rampa_cadeirante_sim"),
            br_bigint("v05028").alias("qt_domicilios_face_rampa_cadeirante_nao"),
            br_bigint("v05029").alias("qt_domicilios_face_rampa_cadeirante_nao_declarado"),

            br_bigint("v05030").alias("qt_domicilios_face_arborizacao_sem_arvores"),
            br_bigint("v05031").alias("qt_domicilios_face_arborizacao_1_2_arvores"),
            br_bigint("v05032").alias("qt_domicilios_face_arborizacao_3_4_arvores"),
            br_bigint("v05033").alias("qt_domicilios_face_arborizacao_5_mais_arvores"),
            br_bigint("v05034").alias("qt_domicilios_face_arborizacao_saltado")
        )
    )

def safe_divide(numerator, denominator):
    return (
        when(
            (denominator.isNull()) | (denominator == 0),
            None
        )
        .otherwise(numerator / denominator)
    )


def transform_censo_rendimento(df_rendimento: DataFrame, nivel: str) -> DataFrame:
    chave_geo = "cd_setor" if nivel == "setor" else "cd_dist"

    return (
        df_rendimento
        # .filter(col("nm_mun") == "São Paulo")
        .select(
            col(chave_geo),
            br_bigint("v06001").alias("qt_pessoas_responsaveis_domicilios"),
            br_bigint("v06002").alias("qt_moradores_domicilios"),
            br_double("v06003").alias("vl_variancia_qt_moradores_domicilios"),
            br_double("v06004").alias("vl_rendimento_nominal_medio_responsavel"),
            br_double("v06005").alias("vl_variancia_rendimento_nominal_responsavel"),
            br_double("v06006").alias("vl_rendimento_nominal_mediano_responsavel"),
        )
    )


def join_censo_datasets(
        df_basico_clean: DataFrame, 
        df_entorno_clean: DataFrame, 
        df_rendimento_clean: DataFrame, 
        nivel: str
    ) -> DataFrame:

    if nivel == "setor":
        chave_geo = "cd_setor"
    elif nivel == "distrito":
        chave_geo = "cd_dist"
    else:
        raise ValueError("nivel deve ser 'setor' ou 'distrito'")

    return (
        df_basico_clean
        .join(df_entorno_clean, on=chave_geo, how="left")
        .join(df_rendimento_clean, on=chave_geo, how="left")
    )


def add_censo_demographic_features(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "densidade_populacional",
            safe_divide(col("qt_pessoas"), col("area_km2"))
        )
        .withColumn(
            "densidade_domicilios",
            safe_divide(col("qt_domicilios"), col("area_km2"))
        )
        .withColumn(
            "tx_domicilios_ocupados",
            safe_divide(
                col("qt_domicilios_particularesocupados"),
                col("qt_domicilios")
            )
        )
        .withColumn(
            "tx_domicilios_vagos",
            safe_divide(
                col("qt_domicilios_particulares_vagos"),
                col("qt_domicilios")
            )
        )
    )


def add_censo_urban_infra_features(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "tx_via_pavimentada",
            safe_divide(
                col("qt_domicilios_face_via_pavimentada_sim"),
                col("qt_domicilios_face_via_pavimentada_sim")
                + col("qt_domicilios_face_via_pavimentada_nao")
            )
        )
        .withColumn(
            "tx_bueiro",
            safe_divide(
                col("qt_domicilios_face_bueiro_sim"),
                col("qt_domicilios_face_bueiro_sim")
                + col("qt_domicilios_face_bueiro_nao")
            )
        )
        .withColumn(
            "tx_iluminacao_publica",
            safe_divide(
                col("qt_domicilios_face_iluminacao_publica_sim"),
                col("qt_domicilios_face_iluminacao_publica_sim")
                + col("qt_domicilios_face_iluminacao_publica_nao")
            )
        )
        .withColumn(
            "tx_ponto_onibus",
            safe_divide(
                col("qt_domicilios_face_ponto_onibus_sim"),
                col("qt_domicilios_face_ponto_onibus_sim")
                + col("qt_domicilios_face_ponto_onibus_nao")
            )
        )
        .withColumn(
            "tx_ciclovia",
            safe_divide(
                col("qt_domicilios_face_via_sinalizada_bicicleta_sim"),
                col("qt_domicilios_face_via_sinalizada_bicicleta_sim")
                + col("qt_domicilios_face_via_sinalizada_bicicleta_nao")
            )
        )
        .withColumn(
            "tx_calcada",
            safe_divide(
                col("qt_domicilios_face_calcada_sim"),
                col("qt_domicilios_face_calcada_sim")
                + col("qt_domicilios_face_calcada_nao")
            )
        )
        .withColumn(
            "tx_obstaculo_calcada",
            safe_divide(
                col("qt_domicilios_face_obstaculo_calcada_sim"),
                col("qt_domicilios_face_obstaculo_calcada_sim")
                + col("qt_domicilios_face_obstaculo_calcada_nao")
            )
        )
        .withColumn(
            "tx_rampa_cadeirante",
            safe_divide(
                col("qt_domicilios_face_rampa_cadeirante_sim"),
                col("qt_domicilios_face_rampa_cadeirante_sim")
                + col("qt_domicilios_face_rampa_cadeirante_nao")
            )
        )
        .withColumn(
            "tx_arborizacao",
            safe_divide(
                col("qt_domicilios_face_arborizacao_1_2_arvores")
                + col("qt_domicilios_face_arborizacao_3_4_arvores")
                + col("qt_domicilios_face_arborizacao_5_mais_arvores"),
                col("qt_domicilios_face_arborizacao_sem_arvores")
                + col("qt_domicilios_face_arborizacao_1_2_arvores")
                + col("qt_domicilios_face_arborizacao_3_4_arvores")
                + col("qt_domicilios_face_arborizacao_5_mais_arvores")
            )
        )
        .withColumn(
            "indice_arborizacao_ponderado",
            safe_divide(
                col("qt_domicilios_face_arborizacao_1_2_arvores") * 1
                + col("qt_domicilios_face_arborizacao_3_4_arvores") * 2
                + col("qt_domicilios_face_arborizacao_5_mais_arvores") * 3,
                col("qt_domicilios_face_arborizacao_sem_arvores")
                + col("qt_domicilios_face_arborizacao_1_2_arvores")
                + col("qt_domicilios_face_arborizacao_3_4_arvores")
                + col("qt_domicilios_face_arborizacao_5_mais_arvores")
            )
        )
    )



def add_censo_socioeconomic_features(df: DataFrame) -> DataFrame:

    return (
        df
        # LOG RENDA MÉDIA
        .withColumn(
            "log_renda_media",
            log1p(col("vl_rendimento_nominal_medio_responsavel"))
        )
        # LOG RENDA MEDIANA
        .withColumn(
            "log_renda_mediana",
            log1p(col("vl_rendimento_nominal_mediano_responsavel"))
        )
        # RENDA MÉDIA POR MORADOR
        .withColumn(
            "vl_renda_media_por_morador",
            safe_divide(
                col("vl_rendimento_nominal_medio_responsavel"),
                col("avg_moradores_domiciliosparticularesocupados")
            )
        )
        # RENDA MEDIANA POR MORADOR
        .withColumn(
            "vl_renda_mediana_por_morador",
            safe_divide(
                col("vl_rendimento_nominal_mediano_responsavel"),
                col("avg_moradores_domiciliosparticularesocupados")
            )
        )
        # RENDA MÉDIA POR ÁREA
        .withColumn(
            "densidade_renda_media_km2",
            safe_divide(
                col("vl_rendimento_nominal_medio_responsavel"),
                col("area_km2")
            )
        )
        # RENDA MEDIANA POR ÁREA
        .withColumn(
            "densidade_renda_mediana_km2",
            safe_divide(
                col("vl_rendimento_nominal_mediano_responsavel"),
                col("area_km2")
            )
        )
    )


def add_censo_composite_indexes(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "indice_infraestrutura_urbana",
            (
                col("tx_via_pavimentada")
                + col("tx_bueiro")
                + col("tx_iluminacao_publica")
                + col("tx_calcada")
                + col("tx_rampa_cadeirante")
                + col("tx_arborizacao")
            ) / 6
        )
        .withColumn(
            "indice_mobilidade_urbana",
            (
                col("tx_ponto_onibus")
                + col("tx_ciclovia")
            ) / 2
        )
        .withColumn(
            "indice_caminhabilidade",
            (
                col("tx_calcada")
                + col("tx_rampa_cadeirante")
                + (1 - col("tx_obstaculo_calcada"))
            ) / 3
        )
        .withColumn(
            "indice_vulnerabilidade_urbana",
            (
                col("tx_domicilios_vagos")
                + (1 - col("tx_iluminacao_publica"))
                + (1 - col("tx_via_pavimentada"))
                + (1 - col("tx_calcada"))
                + (1 - col("tx_bueiro"))
            ) / 5
        )
    )


def build_censo_final(
    df_basico_clean: DataFrame,
    df_entorno_clean: DataFrame,
    df_rendimento_clean: DataFrame,
    nivel: str
) -> DataFrame:

    df = join_censo_datasets(
        df_basico_clean=df_basico_clean,
        df_entorno_clean=df_entorno_clean,
        df_rendimento_clean=df_rendimento_clean,
        nivel=nivel
    )

    df = add_censo_demographic_features(df)
    df = add_censo_urban_infra_features(df)
    df = add_censo_socioeconomic_features(df)
    df = add_censo_composite_indexes(df)

    return df


def select_censo_final_columns(df: DataFrame, nivel: str) -> DataFrame:

    colunas_base = [
        "cd_mun",
        "nm_mun",
        "area_km2",

        # Demografia
        "qt_pessoas",
        "qt_domicilios",
        "qt_domicilios_particularesocupados",
        "qt_domicilios_particulares_vagos",

        "densidade_populacional",
        "densidade_domicilios",
        "tx_domicilios_ocupados",
        "tx_domicilios_vagos",

        "avg_moradores_domiciliosparticularesocupados",

        # Infraestrutura urbana
        "tx_via_pavimentada",
        "tx_bueiro",
        "tx_iluminacao_publica",
        "tx_ponto_onibus",
        "tx_ciclovia",
        "tx_calcada",
        "tx_obstaculo_calcada",
        "tx_rampa_cadeirante",
        "tx_arborizacao",

        "indice_arborizacao_ponderado",

        # Socioeconômico
        "vl_rendimento_nominal_medio_responsavel",
        "vl_rendimento_nominal_mediano_responsavel",
        "vl_variancia_rendimento_nominal_responsavel",
        "vl_variancia_qt_moradores_domicilios",

        "log_renda_media",
        "log_renda_mediana",

        "vl_renda_media_por_morador",
        "vl_renda_mediana_por_morador",

        "densidade_renda_media_km2",
        "densidade_renda_mediana_km2",

        # Índices compostos
        "indice_infraestrutura_urbana",
        "indice_mobilidade_urbana",
        "indice_caminhabilidade",
        "indice_vulnerabilidade_urbana",
    ]

    if nivel == "setor":

        colunas_geo = [
            "cd_setor",
            "cd_dist",
            "nm_dist",
            "cd_subdist",
            "ds_situacao",
            "cd_situacao",
            "cd_tipo",
            "categoria_tipo_setor",
        ]

    elif nivel == "distrito":

        colunas_geo = [
            "cd_dist",
            "nm_dist",
            "cd_regiao",
            "nm_regiao",
        ]

    else:
        raise ValueError("nivel deve ser 'setor' ou 'distrito'")

    return df.select(*(colunas_geo + colunas_base))


def transform_veiculos(df: DataFrame, year: str) -> DataFrame:
    print('===========================')
    print('Transformação')

    print('Limpeza Inicial')
    ## Limpeza inicial
    df = normalize_columns(df)
    df = trim_string_columns(df)
    df = clean_null_strings(df)
    df = drop_unnecessary_columns(df)

    df = uppercase_string_columns(
        df,
        [
            "nome_seccional",
            "nome_delegacia",
            "nome_municipio",
            "nome_municipio_circ",
            "cidade",
            "bairro",
            "logradouro",
            "rubrica",
            "descr_conduta",
            "descr_tipo_veiculo",
            "descr_marca_veiculo",
            "desc_cor_veiculo",
        ],
    )

    print('Castando colunas e tipos de dados')
    ## Castar tipos de dados específicos
    df = cast_date_columns(df, ["data_ocorrencia_bo", "data_comunicacao_bo"])
    df = cast_timestamp_columns(df, ["datahora_registro_bo", "datahora_impressao_bo"])

    df = cast_integer_columns(
        df,
        [
            "id_delegacia",
            "ano_bo",
            "versao",
            "cont_veiculo",
            "ano_fabricacao",
            "ano_modelo",
            "mes_registro_bo",
            "ano_registro_bo",
        ]
    )

    df = cast_time_columns(df, ["hora_ocorrencia"])
    df = cast_coordinate_columns(df, ["latitude", "longitude"])

    print('Adicionando Features')
    ## Criar Features Temporais e Categóricas
    df = create_time_features(df)
    df = create_periodo_dia(df)
    df = categorize_columns(df)
    df = create_flag_columns(df)

    print('Removendo Duplicidade')
    ## Eliminar duplicidade de dados (reconhecida pela SSP)
    df = df.dropDuplicates()

    return df


def transform_censo(df_basico: DataFrame, df_entorno: DataFrame, df_rendimento: DataFrame, nivel: str) -> dict[str, DataFrame]:

    df_basico_clean = transform_censo_basico(
        df_basico=df_basico,
        nivel=nivel
    )

    df_entorno_clean = transform_censo_entorno(
        df_entorno=df_entorno,
        nivel=nivel
    )

    df_rendimento_clean = transform_censo_rendimento(
        df_rendimento=df_rendimento,
        nivel=nivel
    )

    df_censo_final = build_censo_final(
        df_basico_clean=df_basico_clean,
        df_entorno_clean=df_entorno_clean,
        df_rendimento_clean=df_rendimento_clean,
        nivel=nivel
    )

    df_censo_final = select_censo_final_columns(
        df=df_censo_final,
        nivel=nivel
    )

    return {
        "basico": df_basico_clean,
        "entorno": df_entorno_clean,
        "rendimento": df_rendimento_clean,
        "final": df_censo_final
    }