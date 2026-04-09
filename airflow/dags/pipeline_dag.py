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
# retry_delay : attendre 5 minutes entre chaque tentative
# retries     : réessayer 3 fois si une tâche échoue
# =============================================================================
default_args = {
    'owner': 'mamou_ndongo',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# =============================================================================
# Fonctions Python — une par tâche
# =============================================================================

def run_producer():
    """
    Lance le producer Kafka.
    Lit les CSV et envoie les données dans les topics Kafka.
    """
    logging.info("Lancement du producer Kafka...")
    result = subprocess.run(
        [sys.executable, '/opt/airflow/kafka/producer.py'],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Producer échoué : {result.stderr}")
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
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Consumer échoué : {result.stderr}")
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
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"Validation échouée : {result.stderr}")
    logging.info(result.stdout)
    logging.info("Validation terminée — données valides !")

# =============================================================================
# Définition du DAG
# =============================================================================
with DAG(
    dag_id='pipeline_ecommerce',
    description='Pipeline e-commerce complet : Kafka → staging → validation → dbt',
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
        doc_md="""
        Lance producer.py — lit les CSV Kaggle et envoie
        chaque ligne dans les topics Kafka correspondants.
        """
    )

    # =========================================================================
    # TACHE 2 — Consumer Kafka
    # Lit Kafka et charge dans PostgreSQL staging
    # =========================================================================
    task_consumer = PythonOperator(
        task_id='kafka_consumer',
        python_callable=run_consumer,
        doc_md="""
        Lance consumer.py — lit les messages Kafka et
        charge les données brutes dans PostgreSQL staging.
        """
    )

    # =========================================================================
    # TACHE 3 — Validation Great Expectations
    # Vérifie la qualité des données dans staging
    # =========================================================================
    task_validation = PythonOperator(
        task_id='great_expectations_validation',
        python_callable=run_validation,
        doc_md="""
        Lance validation.py — vérifie la qualité des données
        dans staging avant transformation dbt.
        Si des données invalides sont détectées → pipeline s'arrête.
        """
    )

    # =========================================================================
    # TACHE 4 — dbt run
    # Transforme les données staging en marts
    # =========================================================================
    task_dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/ecommerce_dbt && dbt run --profiles-dir /opt/airflow',
        doc_md="""
        Lance dbt run — transforme les données staging en modèles
        marts : stg_orders, stg_customers, stg_products,
        fact_orders, dim_customers, dim_products.
        """
    )

    # =========================================================================
    # TACHE 5 — dbt test
    # Vérifie la qualité des modèles dbt
    # =========================================================================
    task_dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/ecommerce_dbt && dbt test --profiles-dir /opt/airflow',
        doc_md="""
        Lance dbt test — vérifie la qualité des modèles dbt :
        unique, not_null, accepted_values.
        Si des tests échouent → pipeline s'arrête.
        """
    )

    # =========================================================================
    # ORDRE D'EXECUTION
    # producer → consumer → validation → dbt run → dbt test
    # =========================================================================
    task_producer >> task_consumer >> task_validation >> task_dbt_run >> task_dbt_test