import pandas as pd
import logging

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mettre les noms de colonnes en minuscules et enlever les espaces inutiles.
    """
    df = df.copy()
    df.columns = [col.strip().lower() for col in df.columns]
    return df


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyage léger du fichier orders.
    """
    df = standardize_columns(df)

    # Vérifier que les colonnes importantes existent
    required_columns = ["order_id", "customer_id", "order_purchase_timestamp"]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Colonne manquante dans orders : {col}")

    # Supprimer les lignes où order_id ou customer_id est vide
    df = df.dropna(subset=["order_id", "customer_id"])

    # Supprimer les doublons sur la commande
    df = df.drop_duplicates(subset=["order_id"])

    # Convertir la date
    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"],
        errors="coerce"
    )

    # Supprimer les dates invalides
    df = df.dropna(subset=["order_purchase_timestamp"])

    logging.info(f"Orders nettoyé : {df.shape[0]} lignes restantes")
    return df


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyage léger du fichier customers.
    """
    df = standardize_columns(df)

    if "customer_id" not in df.columns:
        raise ValueError("Colonne manquante dans customers : customer_id")

    # Supprimer les lignes sans customer_id
    df = df.dropna(subset=["customer_id"])

    # Supprimer les doublons
    df = df.drop_duplicates(subset=["customer_id"])

    # Nettoyage simple du texte
    if "customer_city" in df.columns:
        df["customer_city"] = df["customer_city"].astype(str).str.strip()

    if "customer_state" in df.columns:
        df["customer_state"] = df["customer_state"].astype(str).str.strip()

    logging.info(f"Customers nettoyé : {df.shape[0]} lignes restantes")
    return df


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoyage léger du fichier products.
    """
    df = standardize_columns(df)

    if "product_id" not in df.columns:
        raise ValueError("Colonne manquante dans products : product_id")

    # Supprimer les lignes sans product_id
    df = df.dropna(subset=["product_id"])

    # Supprimer les doublons
    df = df.drop_duplicates(subset=["product_id"])

    # Convertir quelques colonnes numériques si elles existent
    numeric_columns = [
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]

    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Nettoyage simple d'une colonne texte
    if "product_category_name" in df.columns:
        df["product_category_name"] = df["product_category_name"].astype(str).str.strip()

    logging.info(f"Products nettoyé : {df.shape[0]} lignes restantes")
    return df


def transform_all(datasets: dict) -> dict:
    """
    Nettoyer tous les jeux de données.
    """
    return {
        "orders": clean_orders(datasets["orders"]),
        "customers": clean_customers(datasets["customers"]),
        "products": clean_products(datasets["products"]),
    }