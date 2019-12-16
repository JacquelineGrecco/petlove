/*
Escreva uma única consulta que apresente o percentual de vendas das famílias Cachorros,
Gato e Peixes em relação ao total para todos os meses em 2018.

## INCOMPLETO DEVIDO A MUDANÇA DE UTILIZAÇÃO DO CROSSTAB (PIVOT)
*/
SELECT
    crosstab(setor.familia, 3),
    ano_2018.mes,
    round((total.receita/ano_2018.receita)::numeric * 100, 2) percentual
FROM (SELECT
            extract(month from dia_emissao_nota) mes,
            sku,
            sum(receita) receita
        FROM tb_faturamento
        WHERE extract(year from dia_emissao_nota) = '2018' group by mes, sku) ano_2018
INNER JOIN (SELECT
            extract(month from dia_emissao_nota) mes,
            sku,
            sum(receita) receita
        FROM tb_faturamento
        WHERE extract(year from dia_emissao_nota) <> '2018' group by mes, sku) total
        ON ano_2018.mes = total.mes
        AND ano_2018.sku = total.sku
INNER JOIN tb_familia_setor as setor
        on cast(ano_2018.sku as BIGINT) = setor.sku
        and cast(total.sku as BIGINT) = setor.sku;