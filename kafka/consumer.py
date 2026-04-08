# =============================================================================
# CONSUMER KAFKA
# Rôle : lire les messages Kafka et les charger dans PostgreSQL staging
# Flux : Kafka topic -> consumer.py -> ecommerce_db.staging (données brutes)
# =============================================================================

import json
import logging
import os
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_engine_postgres():
    """
    Crée et retourne une connexion SQLAlchemy vers PostgreSQL.
    Les credentials sont lus depuis le fichier .env
    pour ne jamais exposer les mots de passe dans le code.
    """
    logging.info("Connexion à PostgreSQL...")
    engine = create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}"
        f"/{os.getenv('POSTGRES_DB')}"
    )
    return engine

def create_staging_schema(engine):
    """
    Crée le schéma 'staging' dans PostgreSQL s'il n'existe pas.
    C'est ici que les données brutes seront stockées
    avant transformation par dbt.
    """
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.commit()
    logging.info("Schéma staging prêt !")



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
        Au lieu d'insérer ligne par ligne (lent),
        on accumule 100 messages puis on insère d'un coup (rapide).
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

        # Identifiant unique de ce consumer
        group_id=f'consumer_{topic}',

        # Arrêter après 10 secondes sans nouveaux messages
        consumer_timeout_ms=10000
    )

    # Accumulateur de messages
    batch = []

    for message in consumer:
        # Récupérer les données du message
        data = message.value
        batch.append(data)

        # Quand le batch est plein → insérer dans PostgreSQL
        if len(batch) >= batch_size:
            df = pd.DataFrame(batch)
            
            df.to_sql(
                name=table_name,
                schema='staging',    # schéma staging dans ecommerce_db
                con=engine,
                if_exists='append',  # ajouter sans écraser
                index=False
            )
            logging.info(f"[{table_name}] {len(batch)} lignes insérées dans staging")
            batch = []  # vider le batch

    # Insérer les messages restants (dernier batch incomplet)
    if batch:
        df = pd.DataFrame(batch)
        df.to_sql(
            name=table_name,
            schema='staging',
            con=engine,
            if_exists='append',
            index=False
        )
        logging.info(f"[{table_name}] {len(batch)} lignes restantes insérées")

    consumer.close()
    logging.info(f"[{table_name}] Terminé — données chargées dans staging !")

if __name__ == "__main__":
    # Créer la connexion PostgreSQL
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