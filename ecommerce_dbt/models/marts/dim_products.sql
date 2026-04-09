-- =============================================================================
-- DIM_PRODUCTS — Dimension produits
-- Source  : marts.stg_products
-- Rôle    : table de dimension contenant toutes les informations
--           sur les produits
-- Corrections appliquées :
--           - product_category_name NULL → 'unknown'
--           - product_weight_g = 0 → NULL
-- =============================================================================

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

final AS (
    SELECT
        -- Clé primaire
        product_id,

        -- Catégorie normalisée
        product_category_name,

        -- Dimensions physiques
        product_photos_qty,
        product_weight_g_clean  AS product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm

    FROM products
)

SELECT * FROM final