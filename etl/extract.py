from pathlib import Path
import pandas as pd
import logging

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Dossier racine du projet
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"


def load_csv(file_path: Path) -> pd.DataFrame:
    """
    Lire un fichier CSV et retourner un DataFrame pandas.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {file_path}")

    logging.info(f"Lecture du fichier : {file_path.name}")
    df = pd.read_csv(file_path)
    logging.info(f"{file_path.name} chargé : {df.shape[0]} lignes, {df.shape[1]} colonnes")

    return df


def extract_all() -> dict:
    """
    Lire les 3 fichiers CSV du projet.
    """
    files = {
        "orders": DATA_DIR / "olist_orders_dataset.csv",
        "customers": DATA_DIR / "olist_customers_dataset.csv",
        "products": DATA_DIR / "olist_products_dataset.csv",
    }

    datasets = {}

    for name, path in files.items():
        datasets[name] = load_csv(path)

    return datasets