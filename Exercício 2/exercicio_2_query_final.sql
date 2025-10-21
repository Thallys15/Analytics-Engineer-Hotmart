-- Exercício 2 - Consulta final GMV diário considerando histórico

with last_version_per_day as (
    select
        purchase_id,
        transaction_date,
        subsidiary,
        purchase_value,
        row_number() over (
            partition by purchase_id, transaction_date
            order by created_at desc
        ) as rn
    from fact_gmv_history
    where is_paid = true
)

select
    transaction_date,
    subsidiary,
    sum(purchase_value) as gmv
from last_version_per_day
where rn = 1
group by transaction_date, subsidiary
order by transaction_date, subsidiary;
