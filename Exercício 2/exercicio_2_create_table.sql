-- Exercício 2 - DDL da Tabela Histórica de GMV

create table fact_gmv_history (
    purchase_id bigint,        -- id da transação
    product_id bigint,         -- produto (para granularidade de item)
    producer_id bigint,        -- produtor (para análise por produtor)
    subsidiary string,         -- subsidiária (nacional ou internacional)
    transaction_date date,     -- data do snapshot (partição)
    release_date date,         -- data de pagamento
    purchase_value float,      -- valor da transação
    item_quantity int,         -- quantidade do item
    is_paid boolean,           -- flag se foi pago
    is_current boolean,        -- flag se é versão mais recente
    created_at timestamp       -- data/hora da versão
)
partitioned by (transaction_date);

