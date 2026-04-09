-- =============================================================================
-- FACT_ORDERS — Table de faits des commandes
-- Source  : marts.stg_orders + marts.stg_customers
-- Rôle    : table centrale du data warehouse
--           contient tous les événements de commande
--           avec les clés étrangères vers les dimensions
-- =============================================================================

WITH orders AS (
    -- Lire les commandes nettoyées
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    -- Lire les clients nettoyés
    SELECT * FROM {{ ref('stg_customers') }}
),

final AS (
    SELECT
        -- Clé primaire
        o.order_id,

        -- Clés étrangères vers les dimensions
        o.customer_id,

        -- Statut et dates
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,

        -- Délai de livraison en jours
        EXTRACT(
            DAY FROM (
                o.order_delivered_customer_date - o.order_purchase_timestamp
            )
        ) AS delivery_days,

        -- Localisation du client
        c.customer_city,
        c.customer_state

    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
)

SELECT * FROM final