import logging
from etl.extract import extract_all
from etl.transform import transform_all

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def run_pipeline():
    """
    Pipeline ETL simple :
    1. Lire les CSV
    2. Nettoyer les données
    3. Retourner les DataFrames prêts pour Kafka
    """
    logging.info("Début du pipeline ETL")

    # Extraction
    datasets = extract_all()

    # Transformation légère
    cleaned_data = transform_all(datasets)

    logging.info("Pipeline ETL terminé avec succès")
    return cleaned_data


if __name__ == "__main__":
    data = run_pipeline()

    # Afficher un aperçu de chaque DataFrame
    for name, df in data.items():
        print(f"\n===== {name.upper()} =====")
        print(df.head())
        print(f"Shape : {df.shape}")