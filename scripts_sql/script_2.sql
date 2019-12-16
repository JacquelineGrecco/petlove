/*
Escreva uma única consulta que aplique a regra de rateio do frete definida com a àrea de negócio e
 apresente o custo total de frete gasto no Setor "Alimentos".
 ***Regra rateio = (Custo de frete / Peso total) *  Peso unitário
*/

SELECT
    SUM((frete.custo_frete/query_valor.valor_total) * peso.peso_unitario) total_valor_rateio
FROM (SELECT
        (peso.peso_unitario * faturamento.quantidade) valor_total,
        faturamento.uf_entrega,
        faturamento.sku,
        faturamento.dia_emissao_nota
    FROM tb_faturamento as faturamento
    INNER JOIN tb_familia_setor setor   ON faturamento.sku = setor.sku
                                        AND setor.setor    = 'Alimentos'
    INNER JOIN tb_peso_unitario as peso ON faturamento.sku = peso.sku) query_valor

    INNER JOIN tb_frete as frete            ON query_valor.uf_entrega = frete.uf_entrega
                                            AND cast(query_valor.dia_emissao_nota as date) = cast(frete.dia as date)
    INNER JOIN tb_peso_unitario as peso     ON query_valor.sku = peso.sku ;