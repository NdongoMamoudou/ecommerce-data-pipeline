-- Création de la base Airflow (pour ses métadonnées internes)
CREATE DATABASE airflow_db;

-- On se connecte à notre base principale
\connect ecommerce_db;

-- Schéma pour les données brutes (ce qu'on reçoit tel quel)
CREATE SCHEMA IF NOT EXISTS staging;

-- Schéma pour le Data Warehouse (données transformées)
CREATE SCHEMA IF NOT EXISTS dwh;

-- Schéma pour les vues de reporting
CREATE SCHEMA IF NOT EXISTS reporting;

