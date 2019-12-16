import pandas as pd

from utils.SparkConfig import SparkConfig


def write_parquet(path_txt, path_parquet, path_csv, path_json):

    spark = SparkConfig()
    txt_base = spark.read.option("header", "true").option("delimiter", ",").csv(path_txt)
    txt_base = txt_base.withColumn("peso_unitario", txt_base["peso_unitario"].cast("double"))
    txt_base.createOrReplaceTempView('tb_peso_unitario')

    parquet_base = spark.read.parquet(path_parquet)
    parquet_base.createOrReplaceTempView('tb_faturamento')

    csv_base = spark.read.option("header", "true").option("delimiter", "|").csv(path_csv)
    csv_base = csv_base.withColumn("custo_frete", csv_base["custo_frete"].cast("double"))
    csv_base.createOrReplaceTempView('tb_frete')

    json_base = pd.read_json(path_json)
    json_spark = spark.createDataFrame(json_base)
    json = json_spark.withColumn("sku", json_spark["sku"].cast('string'))
    json.createOrReplaceTempView('tb_familia_setor')

    first = spark.sql("select extract(year from f.dia_emissao_nota) as ano, extract(month from f.dia_emissao_nota) as "
                      "mes, count(distinct f.sku) quantidade, ano_faturamento.quantidade_ano from tb_faturamento as f "
                      "inner join tb_familia_setor as s on f.sku = s.sku and s.familia = 'Cachorros' inner join (select "
                      "extract(year from faturamento.dia_emissao_nota) as ano, count(distinct faturamento.sku) as "
                      "quantidade_ano from tb_faturamento as faturamento inner join tb_familia_setor as s on "
                      "faturamento.sku = s.sku and s.familia = 'Cachorros' group by ano) ano_faturamento on "
                      "ano_faturamento.ano = extract(year from f.dia_emissao_nota) and f.sku = s.sku where extract(year "
                      "from f.dia_emissao_nota) = '2018' group by mes, extract(year from f.dia_emissao_nota), "
                      "ano_faturamento.quantidade_ano order by mes asc")
    first.write.parquet('files/write/parquet/script_1')

    second = spark.sql(
        "SELECT sum((frete.custo_frete/query_valor.valor_total) * peso.peso_unitario) total_valor_rateio "
        "FROM (SELECT ( peso.peso_unitario * faturamento.quantidade) valor_total, faturamento.uf_entrega, "
        "faturamento.sku, faturamento.dia_emissao_nota FROM tb_faturamento as faturamento INNER JOIN "
        "tb_familia_setor setor   ON faturamento.sku = setor.sku AND setor.setor    = 'Alimentos' INNER "
        "JOIN tb_peso_unitario as peso ON faturamento.sku = peso.sku) query_valor INNER JOIN tb_frete as "
        "frete  ON query_valor.uf_entrega = frete.uf_entrega AND cast(query_valor.dia_emissao_nota as "
        "date) = cast(frete.dia as date)  INNER JOIN tb_peso_unitario as peso     ON query_valor.sku = "
        "peso.sku")
    second.write.parquet('files/write/parquet/script_2')

    third = spark.sql("SELECT ano_2018.mes, round((ano_2019.receita/ano_2018.receita)::numeric *100, 2) || '%' as "
                      "percentual_2018_2019,round((ano_2018.receita/ano_2017.receita)::numeric *100, 2) || '%' as "
                      "percentual_2017_2018 FROM (SELECT extract(month from dia_emissao_nota) mes, sum(receita) receita "
                      "FROM tb_faturamento WHERE extract(year from dia_emissao_nota) = '2018' group by mes) ano_2018 LEFT "
                      "JOIN (SELECT extract(month from dia_emissao_nota) mes, sum(receita) receita FROM tb_faturamento "
                      "WHERE extract(year from dia_emissao_nota) > '2018' group by mes ) ano_2019 on ano_2018.mes = "
                      "ano_2019.mes INNER JOIN (SELECT extract(month from dia_emissao_nota) mes, sum(receita) receita "
                      "FROM tb_faturamento WHERE extract(year from dia_emissao_nota) = '2017'  group by mes) ano_2017 ON "
                      "ano_2018.mes = ano_2017.mes")
    third.write.parquet('files/write/parquet/script_3')


if __name__ == '__main__':
    path_txt = "/petlove/files/read/txt/peso_unitario_new.txt"
    path_parquet = "/petlove/files/read/parquet/faturamento.parquet.gzip"
    path_csv = "/petlove/files/read/csv/frete.csv"
    path_json = "/petlove/files/read/json/familiasetor.json"

    write_parquet(path_txt, path_parquet, path_csv, path_json)
