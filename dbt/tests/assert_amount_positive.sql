-- Test singulier : aucun montant négatif ou nul ne doit atteindre la table de faits.
select transaction_id, amount from {{ ref('fct_transactions') }} where amount <= 0
