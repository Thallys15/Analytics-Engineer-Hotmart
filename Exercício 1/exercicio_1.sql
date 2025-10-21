--Exercicio 1 
--Quais sao os 50 maiores produtores em faturamento ($) de 2021?

select
    p.producer_id,                             
    sum(p.purchase_total_value) as total_revenue 
where p.release_date is not null               
  and extract(year from p.release_date) = 2021
group by p.producer_id
order by total_revenue desc
limit 50; 

-- 2) Top 2 produtos que mais faturaram ($) de cada produtor
with revenue_per_product as (
    select
        p.producer_id,
        pi.product_id,
        sum(pi.purchase_value) as product_revenue
    from purchase p
    join product_item pi
      on p.prod_item_id = pi.prod_item_id
    where p.release_date is not null
    group by p.producer_id, pi.product_id
),
ranked_products as (
    select
        producer_id,
        product_id,
        product_revenue,
        row_number() over (partition by producer_id order by product_revenue desc) as rn
    from revenue_per_product
)
select
    producer_id,
    product_id,
    product_revenue
from ranked_products
where rn <= 2
order by producer_id, product_revenue desc;

