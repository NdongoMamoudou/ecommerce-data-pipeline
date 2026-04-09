-- =============================================================================
-- STG_CUSTOMERS — Nettoyage des clients bruts
-- Source  : staging.raw_customers (données brutes chargées par Kafka consumer)
-- Cible   : marts.stg_customers (données nettoyées)
-- Rôle    : sélectionner les colonnes utiles et normaliser le texte
-- =============================================================================

WITH source AS (
    -- Lire les données brutes depuis le schéma staging
    SELECT * FROM {{ source('staging', 'raw_customers') }}
),

cleaned AS (
    SELECT
        -- Identifiants
        customer_id,
        customer_unique_id,

        -- Localisation — normalisation du texte en minuscules
        customer_zip_code_prefix,
        LOWER(TRIM(customer_city))   AS customer_city,
        UPPER(TRIM(customer_state))  AS customer_state

    FROM source

    -- Supprimer les lignes sans customer_id
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned