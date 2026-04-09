-- =============================================================================
-- STG_ORDERS — Nettoyage des commandes brutes
-- Source  : staging.raw_orders (données brutes chargées par Kafka consumer)
-- Cible   : marts.stg_orders (données nettoyées)
-- Rôle    : sélectionner les colonnes utiles et nettoyer les types
-- =============================================================================

WITH source AS (
    -- Lire les données brutes depuis le schéma staging
    SELECT * FROM {{ source('staging', 'raw_orders') }}
),

cleaned AS (
    SELECT
        -- Identifiants
        order_id,
        customer_id,

        -- Statut de la commande
        order_status,

        -- Dates converties en timestamp
        order_purchase_timestamp::timestamp        AS order_purchase_timestamp,
        order_approved_at::timestamp               AS order_approved_at,
        order_delivered_carrier_date::timestamp    AS order_delivered_carrier_date,
        order_delivered_customer_date::timestamp   AS order_delivered_customer_date,
        order_estimated_delivery_date::timestamp   AS order_estimated_delivery_date

    FROM source

    -- Supprimer les lignes sans order_id ou customer_id
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
)

SELECT * FROM cleaned