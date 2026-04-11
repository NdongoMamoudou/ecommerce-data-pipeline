-- =============================================================================
-- ANALYSES.SQL — Requêtes analytiques sur le Data Warehouse
-- Source  : schéma dwh (fact_orders, dim_customers, dim_products)
-- Rôle    : extraire des insights business avec window functions et CTEs
--
-- Analyses :
--   1. Top 10 états brésiliens par nombre de commandes
--   2. Évolution mensuelle des commandes
--   3. Délai moyen de livraison par état
--   4. Taux de commandes livrées vs annulées par mois
--   5. Top 10 catégories de produits les plus commandées
--   6. Ranking des états par délai de livraison
--   7. Clients avec le plus de commandes
--   8. Analyse de cohorte — commandes par trimestre
-- =============================================================================


-- =============================================================================
-- ANALYSE 1 — Top 10 états par volume de commandes
-- Objectif : identifier les marchés les plus actifs au Brésil
-- Technique : GROUP BY + ORDER BY + LIMIT
-- =============================================================================
SELECT
    customer_state,
    COUNT(order_id)                          AS nb_commandes,
    ROUND(COUNT(order_id) * 100.0 /
          SUM(COUNT(order_id)) OVER (), 2)   AS pct_total
FROM dwh.fact_orders
WHERE order_status = 'delivered'
GROUP BY customer_state
ORDER BY nb_commandes DESC
LIMIT 10;


-- =============================================================================
-- ANALYSE 2 — Évolution mensuelle des commandes
-- Objectif : voir la tendance de croissance du business
-- Technique : DATE_TRUNC + window function LAG pour calculer la croissance
-- =============================================================================
WITH commandes_mensuelles AS (
    SELECT
        DATE_TRUNC('month', order_purchase_timestamp) AS mois,
        COUNT(order_id)                               AS nb_commandes
    FROM dwh.fact_orders
    WHERE order_status != 'canceled'
    GROUP BY DATE_TRUNC('month', order_purchase_timestamp)
),

avec_croissance AS (
    SELECT
        mois,
        nb_commandes,

        -- Commandes du mois précédent — LAG regarde la ligne d'avant
        LAG(nb_commandes) OVER (ORDER BY mois) AS nb_commandes_mois_precedent,

        -- Croissance en % par rapport au mois précédent
        ROUND(
            (nb_commandes - LAG(nb_commandes) OVER (ORDER BY mois)) * 100.0
            / NULLIF(LAG(nb_commandes) OVER (ORDER BY mois), 0),
            2
        ) AS croissance_pct

    FROM commandes_mensuelles
)

SELECT
    TO_CHAR(mois, 'YYYY-MM') AS mois,
    nb_commandes,
    nb_commandes_mois_precedent,
    croissance_pct
FROM avec_croissance
ORDER BY mois;


-- =============================================================================
-- ANALYSE 3 — Délai moyen de livraison par état
-- Objectif : identifier les états avec les meilleures / pires livraisons
-- Technique : AVG + window function RANK pour classer les états
-- =============================================================================
WITH livraison_par_etat AS (
    SELECT
        customer_state,
        COUNT(order_id)               AS nb_commandes_livrees,
        ROUND(AVG(delivery_days), 1)  AS delai_moyen_jours,
        ROUND(MIN(delivery_days), 1)  AS delai_min_jours,
        ROUND(MAX(delivery_days), 1)  AS delai_max_jours
    FROM dwh.fact_orders
    WHERE order_status = 'delivered'
      AND delivery_days IS NOT NULL
      AND delivery_days > 0
    GROUP BY customer_state
)

SELECT
    customer_state,
    nb_commandes_livrees,
    delai_moyen_jours,
    delai_min_jours,
    delai_max_jours,

    -- Rank du meilleur délai (1 = livraison la plus rapide)
    RANK() OVER (ORDER BY delai_moyen_jours ASC)  AS rank_rapidite,

    -- Rank du pire délai (1 = livraison la plus lente)
    RANK() OVER (ORDER BY delai_moyen_jours DESC) AS rank_lenteur

FROM livraison_par_etat
ORDER BY delai_moyen_jours;


-- =============================================================================
-- ANALYSE 4 — Taux de livraison vs annulation par mois
-- Objectif : suivre la qualité opérationnelle dans le temps
-- Technique : FILTER + window function pour les pourcentages
-- =============================================================================
WITH statuts_mensuels AS (
    SELECT
        DATE_TRUNC('month', order_purchase_timestamp) AS mois,
        COUNT(order_id)                               AS total_commandes,

        -- Compter uniquement les commandes livrées
        COUNT(order_id) FILTER (
            WHERE order_status = 'delivered'
        )                                             AS nb_livrees,

        -- Compter uniquement les commandes annulées
        COUNT(order_id) FILTER (
            WHERE order_status = 'canceled'
        )                                             AS nb_annulees

    FROM dwh.fact_orders
    GROUP BY DATE_TRUNC('month', order_purchase_timestamp)
)

SELECT
    TO_CHAR(mois, 'YYYY-MM')                               AS mois,
    total_commandes,
    nb_livrees,
    nb_annulees,
    ROUND(nb_livrees  * 100.0 / NULLIF(total_commandes, 0), 2) AS taux_livraison_pct,
    ROUND(nb_annulees * 100.0 / NULLIF(total_commandes, 0), 2) AS taux_annulation_pct
FROM statuts_mensuels
ORDER BY mois;


-- =============================================================================
-- ANALYSE 5 — Top 10 catégories de produits les plus commandées
-- Objectif : identifier les catégories qui génèrent le plus de volume
-- Technique : JOIN fact_orders → dim_products + GROUP BY
-- =============================================================================
SELECT
    p.product_category_name,
    COUNT(f.order_id)                        AS nb_commandes,
    ROUND(AVG(p.product_weight_g), 0)        AS poids_moyen_g,

    -- Part de marché de chaque catégorie
    ROUND(
        COUNT(f.order_id) * 100.0
        / SUM(COUNT(f.order_id)) OVER (),
        2
    )                                        AS part_marche_pct

FROM dwh.fact_orders f
JOIN dwh.dim_products p ON f.order_id = f.order_id  -- jointure via stg_orders
WHERE f.order_status = 'delivered'
GROUP BY p.product_category_name
ORDER BY nb_commandes DESC
LIMIT 10;


-- =============================================================================
-- ANALYSE 6 — Commandes cumulées par état dans le temps
-- Objectif : voir la progression de chaque état mois après mois
-- Technique : SUM window function avec PARTITION BY + ORDER BY (cumul glissant)
-- =============================================================================
WITH commandes_etat_mois AS (
    SELECT
        customer_state,
        DATE_TRUNC('month', order_purchase_timestamp) AS mois,
        COUNT(order_id)                               AS nb_commandes
    FROM dwh.fact_orders
    WHERE order_status = 'delivered'
    GROUP BY customer_state,
             DATE_TRUNC('month', order_purchase_timestamp)
)

SELECT
    customer_state,
    TO_CHAR(mois, 'YYYY-MM') AS mois,
    nb_commandes,

    -- Cumul des commandes pour cet état depuis le début
    SUM(nb_commandes) OVER (
        PARTITION BY customer_state
        ORDER BY mois
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS commandes_cumulees

FROM commandes_etat_mois
ORDER BY customer_state, mois;


-- =============================================================================
-- ANALYSE 7 — Distribution des délais de livraison
-- Objectif : comprendre la répartition des délais (percentiles)
-- Technique : PERCENTILE_CONT (fonction analytique PostgreSQL)
-- =============================================================================
SELECT
    customer_state,
    COUNT(order_id)                                    AS nb_commandes,
    ROUND(AVG(delivery_days), 1)                       AS delai_moyen,

    -- Médiane — 50% des commandes sont livrées en moins de X jours
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY delivery_days
    )::numeric::int                                    AS delai_median,

    -- 90e percentile — 90% des commandes livrées en moins de X jours
    PERCENTILE_CONT(0.9) WITHIN GROUP (
        ORDER BY delivery_days
    )::numeric::int                                    AS delai_p90

FROM dwh.fact_orders
WHERE order_status = 'delivered'
  AND delivery_days IS NOT NULL
  AND delivery_days > 0
GROUP BY customer_state
ORDER BY delai_median;


-- =============================================================================
-- ANALYSE 8 — Analyse de cohorte trimestrielle
-- Objectif : voir comment les commandes évoluent trimestre par trimestre
-- Technique : DATE_TRUNC('quarter') + window function NTILE pour segmenter
-- =============================================================================
WITH commandes_trimestrielles AS (
    SELECT
        DATE_TRUNC('quarter', order_purchase_timestamp) AS trimestre,
        customer_state,
        COUNT(order_id)                                  AS nb_commandes,
        ROUND(AVG(delivery_days), 1)                     AS delai_moyen

    FROM dwh.fact_orders
    WHERE order_status = 'delivered'
      AND delivery_days IS NOT NULL
    GROUP BY DATE_TRUNC('quarter', order_purchase_timestamp),
             customer_state
)

SELECT
    TO_CHAR(trimestre, 'YYYY-"Q"Q') AS trimestre,
    customer_state,
    nb_commandes,
    delai_moyen,

    -- Classer chaque trimestre en 4 quartiles selon le volume
    NTILE(4) OVER (
        PARTITION BY trimestre
        ORDER BY nb_commandes DESC
    ) AS quartile_volume

FROM commandes_trimestrielles
ORDER BY trimestre, nb_commandes DESC;