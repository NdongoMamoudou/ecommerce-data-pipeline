# =============================================================================
# CONSUMER KAFKA
# Rôle : lire les messages Kafka et les charger dans PostgreSQL staging
# Flux : Kafka topic -> consumer.py -> ecommerce_db.staging (données brutes)
# =============================================================================

import json
import logging
import os
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
    Utilisée uniquement pour créer le schéma staging.
    Les credentials sont lus depuis .env — jamais en dur dans le code.
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

def create_staging_schema(engine):
    """
    Crée le schéma 'staging' dans PostgreSQL s'il n'existe pas.
    C'est ici que les données brutes sont stockées
    avant transformation par dbt.
    """
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
    logging.info("Schéma staging prêt !")

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

def insert_dataframe(pg_conn, df, table_name):
    """
    Insère un DataFrame pandas dans PostgreSQL avec psycopg2.
    Toutes les valeurs sont converties en string —
    pas de problème de types dans la zone staging.
    NaN → None pour PostgreSQL NULL.
    """
    if df.empty:
        return

    # Remplacer NaN par None → PostgreSQL NULL
    df = df.where(pd.notnull(df), None)

    # Créer la table si elle n'existe pas encore
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

    with pg_conn.cursor() as cur:
        execute_values(cur, query, values)

    pg_conn.commit()

def consume_topic(topic, table_name, engine, batch_size=100):
    """
    Consomme tous les messages d'un topic Kafka et les charge
    dans PostgreSQL par batch pour optimiser les performances.

    Args:
        topic      : nom du topic Kafka à consommer
        table_name : nom de la table staging cible
        engine     : connexion PostgreSQL SQLAlchemy
        batch_size : nombre de messages à accumuler avant d'insérer

    Stratégie batch :
        On accumule batch_size messages puis on insère d'un coup.
        Beaucoup plus rapide qu'une insertion ligne par ligne.
    """
    logging.info(f"Consumer démarré sur le topic [{topic}]...")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=os.getenv('KAFKA_BROKER', 'localhost:9092'),

        # Convertit les bytes Kafka en dict Python
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),

        # Lire depuis le début du topic
        auto_offset_reset='earliest',

        # Valider automatiquement la position de lecture
        enable_auto_commit=True,

        # Identifiant unique de ce consumer group
        group_id=f'consumer_{topic}',

        # Arrêter après 10 secondes sans nouveaux messages
        consumer_timeout_ms=10000
    )

    # Connexion psycopg2 pour les insertions
    pg_conn = create_pg_connection()

    # Accumulateur de messages
    batch = []

    for message in consumer:
        data = message.value
        batch.append(data)

        # Quand le batch est plein → insérer dans PostgreSQL
        if len(batch) >= batch_size:
            df = pd.DataFrame(batch)
            insert_dataframe(pg_conn, df, table_name)
            logging.info(f"[{table_name}] {len(batch)} lignes insérées dans staging")
            batch = []

    # Insérer le dernier batch incomplet
    if batch:
        df = pd.DataFrame(batch)
        insert_dataframe(pg_conn, df, table_name)
        logging.info(f"[{table_name}] {len(batch)} lignes restantes insérées")

    consumer.close()
    pg_conn.close()
    logging.info(f"[{table_name}] Terminé — données chargées dans staging !")

if __name__ == "__main__":
    # Connexion PostgreSQL
    engine = create_engine_postgres()

    # Créer le schéma staging si inexistant
    create_staging_schema(engine)

    # Consommer les commandes → staging.raw_orders
    consume_topic('raw_orders',    'raw_orders',    engine)

    # Consommer les clients → staging.raw_customers
    consume_topic('raw_customers', 'raw_customers', engine)

    # Consommer les produits → staging.raw_products
    consume_topic('raw_products',  'raw_products',  engine)

    logging.info("Consumer terminé — toutes les données dans staging !")