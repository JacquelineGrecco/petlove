/*
Escreva uma única consulta que apresente a quantidade total de SKUs distintos comprados em 2018 na família "Cachorros"
e também o total de SKUs distintos comprados em cada mês em 2018 na família "Cachorros".
*/

select
    extract(year from f.dia_emissao_nota) as ano,
    extract(month from f.dia_emissao_nota) as mes,
    count(distinct f.sku) quantidade,
   ano_faturamento.quantidade_ano
from tb_faturamento as f
inner join tb_familia_setor as s
    on f.sku = s.sku
    and s.familia = 'Cachorros'
inner join (select
                extract(year from faturamento.dia_emissao_nota) as ano,
                count(distinct faturamento.sku) as quantidade_ano
            from tb_faturamento as faturamento
            inner join tb_familia_setor as s
                on faturamento.sku = s.sku
                and s.familia = 'Cachorros'
             group by ano
            ) ano_faturamento
      on ano_faturamento.ano = extract(year from f.dia_emissao_nota)
      and f.sku = s.sku
where extract(year from f.dia_emissao_nota) = '2018'
group by mes, extract(year from f.dia_emissao_nota), ano_faturamento.quantidade_ano
order by mes asc;