-- =============================================================================
-- STG_CUSTOMERS — Nettoyage des clients bruts
-- Source  : staging.raw_customers (données brutes chargées par Kafka consumer)
-- Cible   : marts.stg_customers (données nettoyées)
-- Rôle    : sélectionner les colonnes utiles, normaliser le texte
--           et dédupliquer les données
--
-- Note : toutes les colonnes arrivent en TEXT depuis staging
--        car Kafka sérialise tout en string.
--        customer_zip_code_prefix casté en INTEGER.
--
-- Déduplication : DISTINCT ON (customer_id) — même raison que stg_orders.
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('staging', 'raw_customers') }}
),

deduplicated AS (
    -- Supprimer les doublons — garder la première occurrence par customer_id
    SELECT DISTINCT ON (customer_id) *
    FROM source
    WHERE customer_id IS NOT NULL
    ORDER BY customer_id
),

cleaned AS (
    SELECT
        -- Identifiants
        customer_id,
        customer_unique_id,

        -- Code postal casté en INTEGER
        -- Arrive en TEXT depuis Kafka
        CAST(customer_zip_code_prefix AS INTEGER) AS customer_zip_code_prefix,

        -- Normalisation du texte
        LOWER(TRIM(customer_city))  AS customer_city,
        UPPER(TRIM(customer_state)) AS customer_state

    FROM deduplicated
)

SELECT * FROM cleaned