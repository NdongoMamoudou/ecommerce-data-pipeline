# =============================================================================
# CONSUMER KAFKA
# Rôle : lire les messages Kafka et les charger dans PostgreSQL staging
# Flux : Kafka topic -> consumer.py -> ecommerce_db.staging (données brutes)
# =============================================================================

import json
import logging
import os
import time
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def create_engine_postgres():
    """
    Crée une connexion SQLAlchemy vers PostgreSQL.
    Utilisée uniquement pour exécuter des requêtes SQL (ex: TRUNCATE).
    Les schémas sont déjà créés via init.sql au démarrage Docker.
    """
    logging.info("Connexion à PostgreSQL via SQLAlchemy...")
    return create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )


def create_pg_connection():
    """
    Crée une connexion psycopg2 native vers PostgreSQL.
    Utilisée pour insérer les données avec execute_values —
    plus performant que pandas.to_sql() pour les gros volumes.
    """
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )


def create_table_if_not_exists(pg_conn, table_name, columns):
    """
    Crée la table cible dans staging si elle n'existe pas.
    Toutes les colonnes sont en TEXT — zone brute, pas de typage fort.
    Le typage sera fait par dbt lors de la transformation.
    """
    with pg_conn.cursor() as cur:
        columns_sql = ", ".join([f'"{col}" TEXT' for col in columns])
        query = f"""
        CREATE TABLE IF NOT EXISTS staging."{table_name}" (
            {columns_sql}
        )
        """
        cur.execute(query)
    pg_conn.commit()


def create_staging_tables(pg_conn):
    """
    Crée les tables staging minimales si elles n'existent pas encore.

    Pourquoi c'est nécessaire :
        Dans ce pipeline, une table était créée seulement au moment
        du premier insert. Donc si un topic Kafka ne renvoyait aucun
        message pendant un run, la table n'était jamais créée,
        même si le consumer se terminait avec succès.

    Cette fonction garantit que les 3 tables existent dès le départ,
    même si un topic est vide au moment du run.
    """
    tables = {
        "raw_orders": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ],
        "raw_customers": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state"
        ],
        "raw_products": [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        ]
    }

    for table_name, columns in tables.items():
        create_table_if_not_exists(pg_conn, table_name, columns)

    logging.info("Tables staging prêtes !")


def truncate_staging(engine):
    """
    Vide les tables staging avant chaque run du pipeline.

    Pourquoi c'est nécessaire :
        Sans ça, chaque run du DAG Airflow accumule les données
        → doublons → la validation Great Expectations échoue
        sur le test d'unicité.

    ⚠️ PostgreSQL ne supporte PAS :
        TRUNCATE TABLE IF EXISTS ...

    Donc on vérifie d'abord si la table existe avant de la vider.
    """

    tables = [
        "staging.raw_orders",
        "staging.raw_customers",
        "staging.raw_products"
    ]

    with engine.begin() as conn:
        for table in tables:
            exists = conn.execute(
                text("SELECT to_regclass(:table_name)"),
                {"table_name": table}
            ).scalar()

            if exists:
                conn.execute(text(f"TRUNCATE TABLE {table}"))
                logging.info(f"Table {table} vidée")
            else:
                logging.info(f"Table {table} absente — rien à vider")

    logging.info("Tables staging vidées — insertion propre !")


def insert_dataframe(pg_conn, df, table_name):
    """
    Insère un DataFrame pandas dans PostgreSQL avec psycopg2.

    Toutes les valeurs sont converties en string —
    pas de problème de types dans la zone staging.
    NaN → NULL PostgreSQL.
    """
    if df.empty:
        logging.info(f"[{table_name}] DataFrame vide — aucune insertion")
        return

    df = df.where(pd.notnull(df), None)

    create_table_if_not_exists(pg_conn, table_name, df.columns.tolist())

    columns = list(df.columns)
    values = [
        tuple(None if v is None else str(v) for v in row)
        for row in df.to_numpy()
    ]

    columns_sql = ", ".join([f'"{col}"' for col in columns])

    query = f'''
        INSERT INTO staging."{table_name}" ({columns_sql})
        VALUES %s
    '''

    logging.info(f"[{table_name}] Insertion de {len(values)} lignes")

    with pg_conn.cursor() as cur:
        execute_values(cur, query, values)

    pg_conn.commit()


def consume_topic(topic, table_name, engine, batch_size=100):
    """
    Consomme tous les messages d'un topic Kafka et les charge
    dans PostgreSQL par batch.

    Pourquoi un group_id dynamique :
        Kafka mémorise les offsets.
        Avec un ID unique → relit tout le topic à chaque run.
    """
    logging.info(f"Consumer démarré sur [{topic}]")

    run_id = int(time.time())

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=os.getenv('KAFKA_BROKER', 'localhost:9092'),
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'consumer_{topic}_{run_id}',
        consumer_timeout_ms=10000
    )

    pg_conn = create_pg_connection()
    batch = []
    total_messages = 0

    for message in consumer:
        batch.append(message.value)
        total_messages += 1

        if len(batch) >= batch_size:
            insert_dataframe(pg_conn, pd.DataFrame(batch), table_name)
            logging.info(f"[{table_name}] {len(batch)} lignes insérées")
            batch = []

    if batch:
        insert_dataframe(pg_conn, pd.DataFrame(batch), table_name)
        logging.info(f"[{table_name}] {len(batch)} lignes restantes")

    logging.info(f"[{table_name}] Total messages lus : {total_messages}")

    consumer.close()
    pg_conn.close()
    logging.info(f"[{table_name}] Terminé")


if __name__ == "__main__":
    engine = create_engine_postgres()

    # Création des tables staging (les schémas sont déjà créés par init.sql)
    pg_conn = create_pg_connection()
    create_staging_tables(pg_conn)
    pg_conn.close()

    # Nettoyage des tables
    truncate_staging(engine)

    # Consommation des topics Kafka
    consume_topic('raw_orders', 'raw_orders', engine)
    consume_topic('raw_customers', 'raw_customers', engine)
    consume_topic('raw_products', 'raw_products', engine)

    logging.info("Consumer terminé — toutes les données chargées !")
    
    