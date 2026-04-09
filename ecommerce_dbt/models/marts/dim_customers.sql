-- =============================================================================
-- DIM_CUSTOMERS — Dimension clients
-- Source  : marts.stg_customers
-- Rôle    : table de dimension contenant toutes les informations
--           sur les clients — utilisée pour enrichir fact_orders
-- =============================================================================

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

final AS (
    SELECT
        -- Clé primaire
        customer_id,

        -- Identifiant universel
        customer_unique_id,

        -- Localisation
        customer_zip_code_prefix,
        customer_city,
        customer_state

    FROM customers
)

SELECT * FROM final