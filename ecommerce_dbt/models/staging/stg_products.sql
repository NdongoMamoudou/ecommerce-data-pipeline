-- =============================================================================
-- STG_PRODUCTS — Nettoyage des produits bruts
-- Source  : staging.raw_products (données brutes chargées par Kafka consumer)
-- Cible   : marts.stg_products (données nettoyées)
-- Rôle    : nettoyer les valeurs nulles et corriger les anomalies
--
-- Corrections :
--   - product_category_name NULL → 'unknown' (610 cas détectés par GE)
--   - product_weight_g = 0 → NULL (4 cas détectés par GE)
--
-- Note : toutes les colonnes arrivent en TEXT depuis staging
--        car Kafka sérialise tout en string.
--        On caste en INTEGER ici pour les calculs dans fact_orders.
--
-- Déduplication : DISTINCT ON (product_id) — même raison que stg_orders.
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('staging', 'raw_products') }}
),

deduplicated AS (
    -- Supprimer les doublons — garder la première occurrence par product_id
    SELECT DISTINCT ON (product_id) *
    FROM source
    WHERE product_id IS NOT NULL
    ORDER BY product_id
),

cleaned AS (
    SELECT
        -- Identifiant
        product_id,

        -- Catégorie — NULL remplacé par 'unknown'
        COALESCE(product_category_name, 'unknown') AS product_category_name,

        -- Dimensions castées en INTEGER
        CAST(product_photos_qty AS INTEGER) AS product_photos_qty,
        CAST(product_weight_g   AS INTEGER) AS product_weight_g,

        -- Poids — 0 remplacé par NULL après cast
        NULLIF(CAST(product_weight_g AS INTEGER), 0) AS product_weight_g_clean,

        CAST(product_length_cm  AS INTEGER) AS product_length_cm,
        CAST(product_height_cm  AS INTEGER) AS product_height_cm,
        CAST(product_width_cm   AS INTEGER) AS product_width_cm

    FROM deduplicated
)

SELECT * FROM cleaned