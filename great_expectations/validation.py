# =============================================================================
# GREAT EXPECTATIONS — Validation de la qualité des données
# Rôle : vérifier que les données dans staging sont correctes
#        avant de les transformer avec dbt
# Flux : staging.raw_* → validation → OK/FAIL
# =============================================================================

import logging
import os
import pandas as pd
from dotenv import load_dotenv
import great_expectations as ge

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def create_connection():
    """
    Crée une connexion psycopg2 directe vers PostgreSQL.
    On utilise psycopg2 plutôt que SQLAlchemy pour éviter
    les warnings de compatibilité avec pandas.
    """
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    return conn


def validate_orders(df):
    """
    Valide la qualité des données de raw_orders.

    Problème connu : Kafka sérialise tout en JSON string,
    donc les timestamps arrivent comme des strings.
    On les convertit avant validation.

    Tests :
        - order_id unique et not null
        - customer_id not null
        - order_status dans les valeurs acceptées
        - order_purchase_timestamp not null
    """
    logging.info("Validation de raw_orders...")

    # Les timestamps sont stockés comme strings à cause de Kafka
    # On les convertit pour éviter des erreurs de type
    df['order_purchase_timestamp'] = df['order_purchase_timestamp'].astype(str)

    gdf = ge.from_pandas(df)

    # order_id = clé primaire → doit être unique et jamais null
    gdf.expect_column_values_to_be_unique('order_id')
    gdf.expect_column_values_to_not_be_null('order_id')

    # customer_id obligatoire — chaque commande doit avoir un client
    gdf.expect_column_values_to_not_be_null('customer_id')

    # order_status doit être dans cette liste précise
    # toute autre valeur = donnée corrompue
    gdf.expect_column_values_to_be_in_set(
        'order_status',
        [
            'delivered',
            'shipped',
            'canceled',
            'processing',
            'invoiced',
            'unavailable',
            'approved',
            'created'
        ]
    )

    # Date d'achat obligatoire
    gdf.expect_column_values_to_not_be_null('order_purchase_timestamp')

    return gdf.validate()


def validate_customers(df):
    """
    Valide la qualité des données de raw_customers.

    Tests :
        - customer_id unique et not null
        - customer_state not null
        - customer_city not null
    """
    logging.info("Validation de raw_customers...")

    gdf = ge.from_pandas(df)

    # customer_id = clé primaire → unique et not null
    gdf.expect_column_values_to_be_unique('customer_id')
    gdf.expect_column_values_to_not_be_null('customer_id')

    # Localisation obligatoire pour les analyses géographiques
    gdf.expect_column_values_to_not_be_null('customer_state')
    gdf.expect_column_values_to_not_be_null('customer_city')

    return gdf.validate()


def validate_products(df):
    """
    Valide la qualité des données de raw_products.

    Problème connu : Kafka sérialise tout en JSON string,
    donc les colonnes numériques arrivent comme strings.
    Ex : product_weight_g = "225" au lieu de 225
    → GE ne peut pas comparer "225" >= 0 (str vs int) → TypeError
    Solution : pd.to_numeric() avant validation.

    Tests :
        - product_id unique et not null
        - product_weight_g entre 0 et 50000

    Notes sur les données Olist :
        - product_category_name : 610 valeurs NULL → corrigé par dbt (COALESCE)
        - product_weight_g = 0 pour 4 produits → corrigé par dbt (NULLIF)
    """
    logging.info("Validation de raw_products...")

    # Convertir toutes les colonnes numériques qui arrivent comme strings
    # errors='coerce' → les valeurs non convertibles deviennent NaN (pas d'erreur)
    cols_numeriques = [
        'product_weight_g',
        'product_length_cm',
        'product_height_cm',
        'product_width_cm',
        'product_name_lenght',
        'product_description_lenght',
        'product_photos_qty'
    ]
    for col in cols_numeriques:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    gdf = ge.from_pandas(df)

    # product_id = clé primaire → unique et not null
    gdf.expect_column_values_to_be_unique('product_id')
    gdf.expect_column_values_to_not_be_null('product_id')

    # Poids entre 0 et 50000g — max réel dans Olist = 40425g
    # Maintenant possible car la colonne est bien numérique
    gdf.expect_column_values_to_be_between(
        'product_weight_g',
        min_value=0,
        max_value=50000
    )

    return gdf.validate()


def afficher_resultats(result, table_name):
    """
    Affiche un résumé lisible des résultats de validation.
    Retourne True si tous les tests sont passés, False sinon.
    """
    total   = result['statistics']['evaluated_expectations']
    success = result['statistics']['successful_expectations']
    failed  = result['statistics']['unsuccessful_expectations']

    logging.info(f"{'='*50}")
    logging.info(f"Résultats {table_name} :")
    logging.info(f"  Total tests  : {total}")
    logging.info(f"  Réussis      : {success}")
    logging.info(f"  Echoués      : {failed}")

    if failed > 0:
        logging.error(f"{table_name} — ECHEC : {failed} test(s) échoué(s) !")
        # Détail des tests échoués pour faciliter le débogage
        for r in result['results']:
            if not r['success']:
                logging.error(
                    f"  Test échoué : {r['expectation_config']['expectation_type']} "
                    f"sur colonne '{r['expectation_config']['kwargs'].get('column', '')}'"
                )
    else:
        logging.info(f"{table_name} — SUCCES : toutes les données sont valides !")

    logging.info(f"{'='*50}")

    return failed == 0


if __name__ == "__main__":

    # Connexion PostgreSQL
    conn = create_connection()

    # Charger les 3 tables depuis le schéma staging
    logging.info("Chargement des données depuis staging...")
    df_orders    = pd.read_sql("SELECT * FROM staging.raw_orders",    conn)
    df_customers = pd.read_sql("SELECT * FROM staging.raw_customers", conn)
    df_products  = pd.read_sql("SELECT * FROM staging.raw_products",  conn)

    conn.close()

    logging.info(f"raw_orders    : {len(df_orders)} lignes")
    logging.info(f"raw_customers : {len(df_customers)} lignes")
    logging.info(f"raw_products  : {len(df_products)} lignes")

    # Lancer les validations
    result_orders    = validate_orders(df_orders)
    result_customers = validate_customers(df_customers)
    result_products  = validate_products(df_products)

    # Afficher les résultats
    ok_orders    = afficher_resultats(result_orders,    'raw_orders')
    ok_customers = afficher_resultats(result_customers, 'raw_customers')
    ok_products  = afficher_resultats(result_products,  'raw_products')

    # Résumé final — si une validation échoue on bloque le pipeline
    # Airflow détecte le exit(1) comme un échec de tâche
    if ok_orders and ok_customers and ok_products:
        logging.info("VALIDATION COMPLETE — toutes les données sont valides !")
        logging.info("Prêt pour la transformation dbt !")
    else:
        logging.error("VALIDATION ECHOUEE — corriger les données avant dbt !")
        exit(1)