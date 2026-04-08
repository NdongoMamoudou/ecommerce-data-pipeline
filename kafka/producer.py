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

# Charger les variables d'environnement depuis .env
load_dotenv()

# Configuration du logging pour tracer toutes les étapes
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_producer():
    """
    Crée et retourne un producer Kafka.
    Le value_serializer convertit chaque message Python dict
    en JSON puis en bytes — format attendu par Kafka.
    default=str gère les types non sérialisables (dates, NaN...)
    """
    logging.info("Connexion au broker Kafka...")
    return KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BROKER', 'localhost:9092'),
        value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
    )

def send_csv_to_topic(producer, csv_file, topic):
    """
    Lit un fichier CSV ligne par ligne et envoie chaque ligne
    comme un message dans le topic Kafka spécifié.
    
    Args:
        producer  : instance KafkaProducer
        csv_file  : chemin vers le fichier CSV
        topic     : nom du topic Kafka cible
    """
    logging.info(f"Lecture du fichier : {csv_file}")
    df = pd.read_csv(csv_file)
    total = len(df)
    logging.info(f"{total} lignes trouvées dans {csv_file}")

    for index, row in df.iterrows():
        # Convertir la ligne en dictionnaire Python
        message = row.to_dict()

        # Envoyer le message dans le topic Kafka
        producer.send(topic, value=message)

        # Log tous les 1000 messages pour suivre la progression
        if index % 1000 == 0:
            logging.info(f"[{topic}] {index} / {total} lignes envoyées")

        # Pause courte pour simuler un flux temps réel
        time.sleep(0.001)

    # S'assurer que tous les messages sont bien envoyés avant de continuer
    producer.flush()
    logging.info(f"[{topic}] Terminé — {total} lignes envoyées dans Kafka !")

if __name__ == "__main__":
    # Créer la connexion Kafka
    producer = create_producer()

    # Envoyer les commandes dans le topic raw_orders
    send_csv_to_topic(
        producer,
        csv_file='data/olist_orders_dataset.csv',
        topic='raw_orders'
    )

    # Envoyer les clients dans le topic raw_customers
    send_csv_to_topic(
        producer,
        csv_file='data/olist_customers_dataset.csv',
        topic='raw_customers'
    )

    # Envoyer les produits dans le topic raw_products
    send_csv_to_topic(
        producer,
        csv_file='data/olist_products_dataset.csv',
        topic='raw_products'
    )

    logging.info("Producer terminé — tous les CSV envoyés dans Kafka !")