# =============================================================================
# GREAT EXPECTATIONS — Validation de la qualité des données
# Rôle : vérifier que les données dans staging sont correctes
#        avant de les transformer avec dbt
# Flux : staging.raw_* → validation → OK/FAIL
# =============================================================================

import logging
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import great_expectations as ge

load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_engine_postgres():
    """
    Crée une connexion SQLAlchemy vers PostgreSQL.
    Les credentials sont lus depuis le fichier .env
    pour ne jamais exposer les mots de passe dans le code.
    """
    return create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )

def validate_orders(df):
    """
    Valide la qualité des données de la table raw_orders.
    Tests :
        - order_id unique et not null
        - customer_id not null
        - order_status dans les valeurs acceptées
        - order_purchase_timestamp not null
    """
    logging.info("Validation de raw_orders...")

    gdf = ge.from_pandas(df)

    # order_id doit être unique
    gdf.expect_column_values_to_be_unique('order_id')

    # order_id ne doit pas être null
    gdf.expect_column_values_to_not_be_null('order_id')

    # customer_id ne doit pas être null
    gdf.expect_column_values_to_not_be_null('customer_id')

    # order_status doit être dans une liste précise
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

    # order_purchase_timestamp ne doit pas être null
    gdf.expect_column_values_to_not_be_null('order_purchase_timestamp')

    result = gdf.validate()
    return result

def validate_customers(df):
    """
    Valide la qualité des données de la table raw_customers.
    Tests :
        - customer_id unique et not null
        - customer_state not null
        - customer_city not null
    """
    logging.info("Validation de raw_customers...")

    gdf = ge.from_pandas(df)

    # customer_id doit être unique
    gdf.expect_column_values_to_be_unique('customer_id')

    # customer_id ne doit pas être null
    gdf.expect_column_values_to_not_be_null('customer_id')

    # customer_state ne doit pas être null
    gdf.expect_column_values_to_not_be_null('customer_state')

    # customer_city ne doit pas être null
    gdf.expect_column_values_to_not_be_null('customer_city')

    result = gdf.validate()
    return result

def validate_products(df):
    """
    Valide la qualité des données de la table raw_products.
    Tests :
        - product_id unique et not null
        - product_weight_g entre 0 et 50000
    Note :
        - product_category_name peut être NULL (610 cas réels)
          → sera corrigé par dbt avec COALESCE(..., 'unknown')
        - product_weight_g = 0 pour 4 produits
          → sera corrigé par dbt avec NULLIF(..., 0)
    """
    logging.info("Validation de raw_products...")

    gdf = ge.from_pandas(df)

    # product_id doit être unique
    gdf.expect_column_values_to_be_unique('product_id')

    # product_id ne doit pas être null
    gdf.expect_column_values_to_not_be_null('product_id')

    # product_weight_g doit être entre 0 et 50000
    # max réel dans les données = 40425g
    gdf.expect_column_values_to_be_between(
        'product_weight_g',
        min_value=0,
        max_value=50000
    )

    result = gdf.validate()
    return result

def print_results(result, table_name):
    """
    Affiche un résumé clair des résultats de validation.
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
    engine = create_engine_postgres()

    # Charger les tables staging
    logging.info("Chargement des données depuis staging...")
    df_orders    = pd.read_sql("SELECT * FROM staging.raw_orders",    engine)
    df_customers = pd.read_sql("SELECT * FROM staging.raw_customers", engine)
    df_products  = pd.read_sql("SELECT * FROM staging.raw_products",  engine)

    logging.info(f"raw_orders    : {len(df_orders)} lignes")
    logging.info(f"raw_customers : {len(df_customers)} lignes")
    logging.info(f"raw_products  : {len(df_products)} lignes")

    # Valider chaque table
    result_orders    = validate_orders(df_orders)
    result_customers = validate_customers(df_customers)
    result_products  = validate_products(df_products)

    # Afficher les résultats
    ok_orders    = print_results(result_orders,    'raw_orders')
    ok_customers = print_results(result_customers, 'raw_customers')
    ok_products  = print_results(result_products,  'raw_products')

    # Résumé final
    if ok_orders and ok_customers and ok_products:
        logging.info("VALIDATION COMPLETE — toutes les données sont valides !")
        logging.info("Prêt pour la transformation dbt !")
    else:
        logging.error("VALIDATION ECHOUEE — corriger les données avant dbt !")
        exit(1)