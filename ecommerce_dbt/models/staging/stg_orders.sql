-- =============================================================================
-- STG_ORDERS — Nettoyage des commandes brutes
-- Source  : staging.raw_orders (données brutes chargées par Kafka consumer)
-- Cible   : marts.stg_orders (données nettoyées)
-- Rôle    : sélectionner les colonnes utiles, nettoyer les types
--           et dédupliquer les données
--
-- Note : toutes les colonnes arrivent en TEXT depuis staging
--        car Kafka sérialise tout en string.
--        Les timestamps sont castés en TIMESTAMP ici.
--
-- Déduplication : DISTINCT ON (order_id) — Kafka peut envoyer
--        les mêmes messages plusieurs fois si le pipeline
--        tourne plusieurs fois. On garde la première occurrence.
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('staging', 'raw_orders') }}
),

deduplicated AS (
    -- Supprimer les doublons — garder la première occurrence par order_id
    -- Nécessaire car Kafka conserve les messages des anciens runs
    SELECT DISTINCT ON (order_id) *
    FROM source
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
    ORDER BY order_id
),

cleaned AS (
    SELECT
        -- Identifiants
        order_id,
        customer_id,

        -- Statut de la commande
        order_status,

        -- Dates castées en TIMESTAMP
        -- Arrivent en TEXT depuis Kafka → cast obligatoire
        order_purchase_timestamp::timestamp        AS order_purchase_timestamp,
        order_approved_at::timestamp               AS order_approved_at,
        order_delivered_carrier_date::timestamp    AS order_delivered_carrier_date,
        order_delivered_customer_date::timestamp   AS order_delivered_customer_date,
        order_estimated_delivery_date::timestamp   AS order_estimated_delivery_date

    FROM deduplicated
)

SELECT * FROM cleaned