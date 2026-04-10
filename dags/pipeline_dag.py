# =============================================================================
# DAG AIRFLOW — Pipeline E-commerce complet
# Rôle : orchestrer toutes les étapes du pipeline automatiquement
# Schedule : tous les jours à minuit
#
# Flux :
# truncate_staging → reset_topics → producer → consumer → validation → dbt run → dbt test
#
# Pourquoi truncate_staging en premier :
#   Vider les tables staging avant chaque run évite les doublons
#   si le pipeline tourne plusieurs fois dans la même journée.
# =============================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import logging
import sys
import os

# =============================================================================
# Configuration par défaut du DAG
# Ces paramètres s'appliquent à toutes les tâches sauf si on les surcharge
# =============================================================================
default_args = {
    'owner': 'mamou_ndongo',

    # Réessayer 3 fois si une tâche échoue
    'retries': 0,

    # Attendre 5 minutes entre chaque tentative
    'retry_delay': timedelta(minutes=5),

    # Pas d'email en cas d'échec (pas de serveur mail configuré)
    'email_on_failure': False,
    'email_on_retry': False,
}

# =============================================================================
# Fonctions Python — une par tâche
# Chaque fonction lance un script via subprocess pour isoler les dépendances
# =============================================================================

def run_truncate_staging():
    """
    Vide les tables staging avant chaque run du pipeline.

    Pourquoi c'est nécessaire :
        Sans ça, chaque run accumule les données dans staging
        → doublons → Great Expectations échoue sur le test d'unicité
        → le pipeline est bloqué.

    On utilise psycopg2 directement pour éviter les problèmes
    de compatibilité SQLAlchemy 2.0 avec pandas.
    """
    import psycopg2
    logging.info("Vidage des tables staging avant le run...")

    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        dbname=os.getenv('POSTGRES_DB', 'ecommerce_db'),
        user=os.getenv('POSTGRES_USER', 'ecommerce_user'),
        password=os.getenv('POSTGRES_PASSWORD', 'ecommerce_password')
    )

    tables = [
        'staging.raw_orders',
        'staging.raw_customers',
        'staging.raw_products'
    ]

    with conn.cursor() as cur:
        for table in tables:
            # Vérifier si la table existe avant de la vider
            schema, table_name = table.split('.')
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
            """, (schema, table_name))
            exists = cur.fetchone()[0]

            if exists:
                cur.execute(f"TRUNCATE TABLE {table}")
                logging.info(f"Table {table} vidée")
            else:
                logging.info(f"Table {table} absente — ignorée")

    conn.commit()
    conn.close()
    logging.info("Tables staging vidées — prêt pour un run propre !")


def reset_kafka_topics():
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import UnknownTopicOrPartitionError, TopicAlreadyExistsError
    from kafka import KafkaConsumer
    import time

    logging.info("Réinitialisation des topics Kafka...")
    broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
    topics = ["raw_orders", "raw_customers", "raw_products"]
    admin = KafkaAdminClient(bootstrap_servers=broker)

    # Supprimer les topics
    try:
        admin.delete_topics(topics)
        logging.info("Topics supprimés — attente 30 secondes...")
    except UnknownTopicOrPartitionError:
        logging.info("Topics n'existaient pas — OK")

    # Attendre 30 secondes — Kafka a besoin de temps pour purger
    time.sleep(30)

    # Vérifier que les topics sont bien supprimés
    for i in range(20):
        existing = admin.list_topics()
        remaining = [t for t in topics if t in existing]
        if not remaining:
            logging.info("Topics bien supprimés !")
            break
        logging.info(f"Attente : {remaining} encore présents...")
        time.sleep(5)

    # Recréer les topics
    new_topics = [
        NewTopic(name=t, num_partitions=1, replication_factor=1)
        for t in topics
    ]
    try:
        admin.create_topics(new_topics)
        logging.info("Topics recréés !")
    except TopicAlreadyExistsError:
        logging.info("Topics déjà recréés — OK")

    admin.close()
    logging.info("Topics Kafka réinitialisés !")

def run_producer():
    """
    Lance le producer Kafka.
    Lit les CSV depuis /opt/airflow/data
    et envoie chaque ligne dans les topics Kafka correspondants.

    On passe les variables d'environnement explicitement pour que
    le script trouve bien Kafka même depuis le container Airflow.
    """
    logging.info("Lancement du producer Kafka...")
    result = subprocess.run(
        [sys.executable, '/opt/airflow/kafka/producer.py'],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            'KAFKA_BROKER': os.getenv('KAFKA_BROKER', 'kafka:9092'),
            'DATA_DIR': '/opt/airflow/data'
        }
    )
    if result.returncode != 0:
        raise Exception(f"Producer échoué :\n{result.stderr}")
    logging.info(result.stdout)
    logging.info("Producer terminé !")


def run_consumer():
    """
    Lance le consumer Kafka.
    Lit les messages depuis les topics Kafka
    et les charge dans PostgreSQL schéma staging.

    Le consumer utilise un group_id dynamique (basé sur timestamp)
    pour relire tout le topic à chaque run — pas d'offset mémorisé.
    """
    logging.info("Lancement du consumer Kafka...")
    result = subprocess.run(
        [sys.executable, '/opt/airflow/kafka/consumer.py'],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            'KAFKA_BROKER': os.getenv('KAFKA_BROKER', 'kafka:9092'),
            'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres'),
            'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
            'POSTGRES_USER': os.getenv('POSTGRES_USER', 'ecommerce_user'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'ecommerce_password'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB', 'ecommerce_db')
        }
    )
    if result.returncode != 0:
        raise Exception(f"Consumer échoué :\n{result.stderr}")
    logging.info(result.stdout)
    logging.info("Consumer terminé !")


def run_validation():
    """
    Lance Great Expectations.
    Vérifie la qualité des données dans staging avant transformation dbt.

    Tests effectués :
        - raw_orders    : order_id unique, not null, status valide
        - raw_customers : customer_id unique, not null, state not null
        - raw_products  : product_id unique, not null, weight entre 0 et 50000

    Si une règle échoue → exit(1) → Airflow marque FAILED
    → dbt ne se lance pas → données corrompues bloquées ici.
    """
    logging.info("Lancement de la validation Great Expectations...")
    result = subprocess.run(
        [sys.executable, '/opt/airflow/great_expectations/validation.py'],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres'),
            'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
            'POSTGRES_USER': os.getenv('POSTGRES_USER', 'ecommerce_user'),
            'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'ecommerce_password'),
            'POSTGRES_DB': os.getenv('POSTGRES_DB', 'ecommerce_db')
        }
    )
    if result.returncode != 0:
        raise Exception(f"Validation échouée :\n{result.stderr}")
    logging.info(result.stdout)
    logging.info("Validation terminée — données valides !")


# =============================================================================
# Définition du DAG
# =============================================================================
with DAG(
    dag_id='pipeline_ecommerce',
    description='Pipeline e-commerce : truncate → Kafka → staging → validation → dbt',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),

    # Lancer automatiquement tous les jours à minuit
    schedule_interval='@daily',

    # Ne pas rattraper les runs passés depuis start_date
    catchup=False,

    tags=['ecommerce', 'kafka', 'dbt', 'data-engineering']
) as dag:

    # =========================================================================
    # TACHE 1 — Truncate staging
    # Vide les tables staging pour éviter les doublons
    # =========================================================================
    task_truncate = PythonOperator(
        task_id='truncate_staging',
        python_callable=run_truncate_staging,
        doc_md="Vide les tables staging avant le run pour éviter les doublons"
    )

    # =========================================================================
    # TACHE 2 — Reset topics Kafka
    # Supprime et recrée les topics pour repartir sur un run propre
    # =========================================================================
    task_reset_topics = PythonOperator(
        task_id='reset_kafka_topics',
        python_callable=reset_kafka_topics,
        doc_md="Supprime et recrée les topics Kafka pour éviter les doublons"
    )

    # =========================================================================
    # TACHE 3 — Producer Kafka
    # Lit les CSV Kaggle et envoie chaque ligne dans Kafka
    # =========================================================================
    task_producer = PythonOperator(
        task_id='kafka_producer',
        python_callable=run_producer,
        doc_md="Lance producer.py — envoie les CSV dans Kafka"
    )

    # =========================================================================
    # TACHE 4 — Consumer Kafka
    # Lit les messages Kafka et charge dans PostgreSQL staging
    # =========================================================================
    task_consumer = PythonOperator(
        task_id='kafka_consumer',
        python_callable=run_consumer,
        doc_md="Lance consumer.py — charge Kafka dans PostgreSQL staging"
    )

    # =========================================================================
    # TACHE 5 — Validation Great Expectations
    # Vérifie la qualité des données staging avant dbt
    # Si échec → pipeline bloqué ici, dbt ne tourne pas
    # =========================================================================
    task_validation = PythonOperator(
        task_id='great_expectations_validation',
        python_callable=run_validation,
        doc_md="Lance validation.py — vérifie la qualité des données staging"
    )

    # =========================================================================
    # TACHE 6 — dbt run
    # Transforme les données brutes staging en modèles propres dans marts
    # stg_orders, stg_customers, stg_products → fact_orders, dim_customers, dim_products
    #
    # --profiles-dir pointe vers le dossier contenant profiles.yml
    # =========================================================================
    task_dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/ecommerce_dbt && dbt run --profiles-dir /opt/airflow/ecommerce_dbt',
        doc_md="Lance dbt run — transforme staging en marts"
    )

    # =========================================================================
    # TACHE 7 — dbt test
    # Vérifie la qualité des modèles dbt après transformation
    # Tests : unique, not_null, accepted_values
    # =========================================================================
    task_dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/ecommerce_dbt && dbt test --profiles-dir /opt/airflow/ecommerce_dbt',
        doc_md="Lance dbt test — vérifie la qualité des modèles"
    )

    # =========================================================================
    # ORDRE D'EXECUTION
    # >> signifie "cette tâche doit finir avant de lancer la suivante"
    # truncate → reset topics → producer → consumer → validation → dbt run → dbt test
    # =========================================================================
    task_truncate >> task_reset_topics >> task_producer >> task_consumer >> task_validation >> task_dbt_run >> task_dbt_test