/*
Escreva uma única consulta que apresente o crescimento percentual de vendas  em relação
ao último mês para todos os meses a partir de 2018.
Apresente também a variação em relação ao mesmo mês do ano anterior para todos os meses.
*/
SELECT
    ano_2018.mes,
    round((ano_2019.receita/ano_2018.receita)::numeric *100, 2) || '%' as percentual_2018_2019,
    round((ano_2018.receita/ano_2017.receita)::numeric *100, 2) || '%' as percentual_2017_2018
FROM (
    SELECT
            extract(month from dia_emissao_nota) mes,
            sum(receita) receita
        FROM tb_faturamento
        WHERE extract(year from dia_emissao_nota) = '2018' group by mes) ano_2018

LEFT JOIN (
            SELECT
                extract(month from dia_emissao_nota) mes,
                sum(receita) receita
            FROM tb_faturamento
            WHERE extract(year from dia_emissao_nota) > '2018' group by mes
            ) ano_2019
            on ano_2018.mes = ano_2019.mes

INNER JOIN (
    SELECT
        extract(month from dia_emissao_nota) mes,
        sum(receita) receita
    FROM tb_faturamento
    WHERE extract(year from dia_emissao_nota) = '2017'
    group by mes) ano_2017
    ON ano_2018.mes = ano_2017.mes;