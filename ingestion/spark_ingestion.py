import pandas as pd

from utils.SparkConfig import SparkConfig



def ingestion_postgres(path_txt, path_parquet, path_csv, path_json, postgres_user, postgres_pass, postgres_url):

    spark = SparkConfig()
    mode = "overwrite"
    properties = {"user": postgres_user, "password": postgres_pass, "driver": "org.postgresql.Driver"}

    txt_base = spark.read.option("header", "true").option("delimiter", ",").csv(path_txt)
    txt_base = txt_base.withColumn("peso_unitario", txt_base["peso_unitario"].cast("double"))
    txt_base.write.jdbc(url=postgres_url, table="tb_peso_unitario", mode=mode, properties=properties)

    parquet_base = spark.read.parquet(path_parquet)
    parquet_base.write.jdbc(url=postgres_url, table="tb_faturamento", mode=mode, properties=properties)

    csv_base = spark.read.option("header", "true").option("delimiter", "|").csv(path_csv)
    csv_base = csv_base.withColumn("custo_frete", csv_base["custo_frete"].cast("double"))
    csv_base.write.jdbc(url=postgres_url, table="tb_frete", mode=mode, properties=properties)

    json_base = pd.read_json(path_json)
    json_base = spark.createDataFrame(json_base)
    json_base = json_base.withColumn("sku", json_base["sku"].cast('string'))
    json_base.write.jdbc(url=postgres_url, table="tb_familia_setor", mode=mode, properties=properties)


if __name__ == '__main__':

    path_txt = '/petlove/files/read/txt/peso_unitario_new.txt'
    path_json = '/petlove/files/read/json/familiasetor.json'
    path_csv = '/petlove/files/read/csv/frete.csv'
    path_parquet = '/petlove/files/read/parquet/faturamento.parquet.gzip'

    postgres_user = ''
    postgres_pass = ''
    postgres_url = ''

    ingestion_postgres(path_txt, path_parquet, path_csv, path_json, postgres_user, postgres_pass, postgres_url)
