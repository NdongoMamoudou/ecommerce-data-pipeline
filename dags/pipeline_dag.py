# =============================================================================
# DAG AIRFLOW — Pipeline E-commerce complet
# Rôle : orchestrer toutes les étapes du pipeline automatiquement
# Schedule : tous les jours à minuit
#
# Flux :
# producer.py → consumer.py → validation.py → dbt run → dbt test
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
# =============================================================================
default_args = {
    'owner': 'mamou_ndongo',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# =============================================================================
# Fonctions Python
# =============================================================================

def run_producer():
    """
    Lance le producer Kafka.
    Lit les CSV depuis /opt/airflow/data
    et envoie les données dans les topics Kafka.
    """
    logging.info("Lancement du producer Kafka...")
    result = subprocess.run(
        [sys.executable, '/opt/airflow/kafka/producer.py'],
        capture_output=True,
        text=True,
        env={**os.environ,
             'KAFKA_BROKER': os.getenv('KAFKA_BROKER', 'kafka:9092'),
             'DATA_DIR': '/opt/airflow/data'}
    )
    if result.returncode != 0:
        raise Exception(f"Producer échoué :\n{result.stderr}")
    logging.info(result.stdout)
    logging.info("Producer terminé !")

def run_consumer():
    """
    Lance le consumer Kafka.
    Lit les messages Kafka et charge dans PostgreSQL staging.
    """
    logging.info("Lancement du consumer Kafka...")
    result = subprocess.run(
        [sys.executable, '/opt/airflow/kafka/consumer.py'],
        capture_output=True,
        text=True,
        env={**os.environ,
             'KAFKA_BROKER': os.getenv('KAFKA_BROKER', 'kafka:9092'),
             'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres'),
             'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
             'POSTGRES_USER': os.getenv('POSTGRES_USER', 'ecommerce_user'),
             'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'ecommerce_password'),
             'POSTGRES_DB': os.getenv('POSTGRES_DB', 'ecommerce_db')}
    )
    if result.returncode != 0:
        raise Exception(f"Consumer échoué :\n{result.stderr}")
    logging.info(result.stdout)
    logging.info("Consumer terminé !")

def run_validation():
    """
    Lance Great Expectations.
    Vérifie la qualité des données dans staging.
    Si la validation échoue → le pipeline s'arrête.
    """
    logging.info("Lancement de la validation Great Expectations...")
    result = subprocess.run(
        [sys.executable, '/opt/airflow/great_expectations/validation.py'],
        capture_output=True,
        text=True,
        env={**os.environ,
             'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres'),
             'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
             'POSTGRES_USER': os.getenv('POSTGRES_USER', 'ecommerce_user'),
             'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'ecommerce_password'),
             'POSTGRES_DB': os.getenv('POSTGRES_DB', 'ecommerce_db')}
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
    description='Pipeline e-commerce : Kafka → staging → validation → dbt',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'kafka', 'dbt', 'data-engineering']
) as dag:

    # =========================================================================
    # TACHE 1 — Producer Kafka
    # Lit les CSV et envoie dans Kafka
    # =========================================================================
    task_producer = PythonOperator(
        task_id='kafka_producer',
        python_callable=run_producer,
        doc_md="Lance producer.py — envoie les CSV dans Kafka"
    )

    # =========================================================================
    # TACHE 2 — Consumer Kafka
    # Lit Kafka et charge dans PostgreSQL staging
    # =========================================================================
    task_consumer = PythonOperator(
        task_id='kafka_consumer',
        python_callable=run_consumer,
        doc_md="Lance consumer.py — charge Kafka dans PostgreSQL staging"
    )

    # =========================================================================
    # TACHE 3 — Validation Great Expectations
    # Vérifie la qualité des données dans staging
    # =========================================================================
    task_validation = PythonOperator(
        task_id='great_expectations_validation',
        python_callable=run_validation,
        doc_md="Lance validation.py — vérifie la qualité des données staging"
    )

    # =========================================================================
    # TACHE 4 — dbt run
    # Transforme les données staging en marts
    # =========================================================================
    task_dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/ecommerce_dbt && dbt run --profiles-dir /opt/airflow/dbt_profiles',
        doc_md="Lance dbt run — transforme staging en marts"
    )

    # =========================================================================
    # TACHE 5 — dbt test
    # Vérifie la qualité des modèles dbt
    # =========================================================================
    task_dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/ecommerce_dbt && dbt test --profiles-dir /opt/airflow/dbt_profiles',
        doc_md="Lance dbt test — vérifie la qualité des modèles"
    )

    # =========================================================================
    # ORDRE D'EXECUTION
    # =========================================================================
    task_producer >> task_consumer >> task_validation >> task_dbt_run >> task_dbt_test