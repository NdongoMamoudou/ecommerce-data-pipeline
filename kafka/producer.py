# =============================================================================
# PRODUCER KAFKA
# Rôle : lire les fichiers CSV et envoyer chaque ligne dans Kafka
# Flux : CSV -> producer.py -> Kafka topic
# =============================================================================

import pandas as pd
import json
import time
import logging
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Chemin de base des données
# Sur ton PC : data/
# Dans Docker : /opt/airflow/data
DATA_DIR = os.getenv('DATA_DIR', 'data')

def create_producer():
    """
    Crée et retourne un producer Kafka.
    Le value_serializer convertit chaque message Python dict
    en JSON puis en bytes — format attendu par Kafka.
    default=str gère les types non sérialisables (dates, NaN...)
    """
    broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
    logging.info(f"Connexion au broker Kafka : {broker}")
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
    )

def send_csv_to_topic(producer, csv_file, topic):
    """
    Lit un fichier CSV ligne par ligne et envoie chaque ligne
    comme un message dans le topic Kafka spécifié.

    Args:
        producer  : instance KafkaProducer
        csv_file  : nom du fichier CSV (sans le chemin)
        topic     : nom du topic Kafka cible
    """
    full_path = os.path.join(DATA_DIR, csv_file)
    logging.info(f"Lecture du fichier : {full_path}")

    df = pd.read_csv(full_path)
    total = len(df)
    logging.info(f"{total} lignes trouvées dans {full_path}")

    for index, row in df.iterrows():
        message = row.to_dict()
        producer.send(topic, value=message)

        if index % 1000 == 0:
            logging.info(f"[{topic}] {index} / {total} lignes envoyées")

        time.sleep(0.001)

    producer.flush()
    logging.info(f"[{topic}] Terminé — {total} lignes envoyées dans Kafka !")

if __name__ == "__main__":
    producer = create_producer()

    send_csv_to_topic(
        producer,
        csv_file='olist_orders_dataset.csv',
        topic='raw_orders'
    )

    send_csv_to_topic(
        producer,
        csv_file='olist_customers_dataset.csv',
        topic='raw_customers'
    )

    send_csv_to_topic(
        producer,
        csv_file='olist_products_dataset.csv',
        topic='raw_products'
    )

    logging.info("Producer terminé — tous les CSV envoyés dans Kafka !")