-- KPI journaliers par catégorie de marchand : base du reporting BI
select
    f.event_date,
    m.category,
    count(*)                                                    as nb_transactions,
    sum(case when f.is_approved then f.amount else 0 end)       as volume_approuve,
    avg(f.amount)                                               as panier_moyen,
    avg(case when f.is_approved then 1.0 else 0.0 end)          as taux_acceptation,
    sum(case when f.is_fraud then 1 else 0 end)                 as nb_fraudes,
    sum(case when f.is_fraud then f.amount else 0 end)          as montant_fraude
from {{ ref('fct_transactions') }} f
join {{ ref('dim_merchant') }} m using (merchant_id)
group by 1, 2
