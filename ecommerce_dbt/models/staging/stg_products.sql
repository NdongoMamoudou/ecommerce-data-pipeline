-- =============================================================================
-- STG_PRODUCTS — Nettoyage des produits bruts
-- Source  : staging.raw_products (données brutes chargées par Kafka consumer)
-- Cible   : dwh.stg_products (données nettoyées)
-- Rôle    : nettoyer les valeurs nulles et corriger les anomalies détectées
--           par Great Expectations :
--           - product_category_name NULL → 'unknown'
--           - product_weight_g = 0 → NULL
--
-- Note : toutes les colonnes arrivent en TEXT depuis staging
--        car Kafka sérialise tout en string.
--        On caste en INTEGER ici pour permettre les calculs dans fact_orders.
-- =============================================================================

WITH source AS (
    SELECT * FROM {{ source('staging', 'raw_products') }}
),

cleaned AS (
    SELECT
        -- Identifiant
        product_id,

        -- Catégorie — NULL remplacé par 'unknown'
        -- 610 produits sans catégorie détectés par Great Expectations
        COALESCE(product_category_name, 'unknown') AS product_category_name,

        -- Dimensions castées en INTEGER — arrivent en TEXT depuis Kafka
        CAST(product_photos_qty    AS INTEGER) AS product_photos_qty,
        CAST(product_weight_g      AS INTEGER) AS product_weight_g,

        -- Poids — 0 remplacé par NULL après cast en INTEGER
        NULLIF(CAST(product_weight_g AS INTEGER), 0) AS product_weight_g_clean,

        CAST(product_length_cm     AS INTEGER) AS product_length_cm,
        CAST(product_height_cm     AS INTEGER) AS product_height_cm,
        CAST(product_width_cm      AS INTEGER) AS product_width_cm

    FROM source

    -- Supprimer les lignes sans product_id
    WHERE product_id IS NOT NULL
)

SELECT * FROM cleaned