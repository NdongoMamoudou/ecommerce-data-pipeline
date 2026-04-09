-- =============================================================================
-- STG_PRODUCTS — Nettoyage des produits bruts
-- Source  : staging.raw_products (données brutes chargées par Kafka consumer)
-- Cible   : marts.stg_products (données nettoyées)
-- Rôle    : nettoyer les valeurs nulles et corriger les anomalies détectées
--           par Great Expectations :
--           - product_category_name NULL → 'unknown'
--           - product_weight_g = 0 → NULL
-- =============================================================================

WITH source AS (
    -- Lire les données brutes depuis le schéma staging
    SELECT * FROM {{ source('staging', 'raw_products') }}
),

cleaned AS (
    SELECT
        -- Identifiant
        product_id,

        -- Catégorie — NULL remplacé par 'unknown'
        -- 610 produits sans catégorie détectés par Great Expectations
        COALESCE(product_category_name, 'unknown') AS product_category_name,

        -- Dimensions du produit
        product_photos_qty,
        product_weight_g,

        -- Poids — 0 remplacé par NULL
        -- 4 produits avec poids = 0 détectés par Great Expectations
        NULLIF(product_weight_g, 0)                AS product_weight_g_clean,

        product_length_cm,
        product_height_cm,
        product_width_cm

    FROM source

    -- Supprimer les lignes sans product_id
    WHERE product_id IS NOT NULL
)

SELECT * FROM cleaned