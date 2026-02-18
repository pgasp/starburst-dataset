# naval_maintenance/naval_maintenance_data.py

import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
import os
import sys
import logging
import argparse

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
# ------------------------------

# --- External Utility Imports ---
try:
    from shared_tools.lakehouse_utils import setup_schema, upload_to_starburst_parallel
    from shared_tools.deploy import scan_and_deploy
    from shared_tools.env_utils import load_project_env
except ImportError as e:
    logging.critical(f"FATAL ERROR: Could not import utility functions. Did you run 'pip install -e .' from the project root? Details: {e}")
    sys.exit(1)

# Load environment variables
load_project_env(__file__)

fake = Faker('fr_FR')

# --- Configuration (Volume) --- ## MODIFIÉ POUR UN DATASET PLUS PETIT ##
NUM_NAVIRES = 10                                # Modifié de 25
NUM_TYPES_CAPTEURS = 8                          # Modifié de 15
NUM_INTERVENTIONS_MAINTENANCE_MAX = 2000        # Modifié de 50000
NUM_TELEMETRIE_READINGS = 10000                 # Modifié de 200000

def get_config():
    """Loads configuration for the RAW data target from environment variables."""
    try:
        config = {
            "host": os.environ["SB_HOST"], "port": os.environ["SB_PORT"],
            "user": os.environ["SB_USER"], "password": os.environ["SB_PASSWORD"],
            "catalog": os.environ["NAVAL_RAW_CATALOG"],
            "schema": os.environ["NAVAL_RAW_SCHEMA"],
            "location": os.environ["NAVAL_SB_SCHEMA_LOCATION"]
        }
        return config
    except KeyError as e:
        logging.error(f"--- ERREUR : Configuration manquante pour la Maintenance Navale : {e} ---")
        logging.error("Veuillez vérifier les variables dans votre .env racine et dans data_products/naval_maintenance/.env.")
        sys.exit(1)

def generate_naval_data():
    logging.info("Début de la génération des données de maintenance navale...")

    # --- 1. Flotte de Navires ---
    classes_navires = ['Frégate', 'Porte-avions', 'Sous-marin', 'Patrouilleur', 'Navire de soutien']
    noms_navires = ['Charles de Gaulle', 'Forbin', 'Aquitaine', 'Le Triomphant', 'La Fayette', 'Chevalier Paul', 'Surcouf', 'Mistral', 'Tonnerre']
    navires = []
    for i in range(NUM_NAVIRES):
        navires.append({
            "ID_Navire": f"FS-{700+i}", # FS for French Ship
            "Nom_Navire": f"{random.choice(noms_navires)} {i}" if i >= len(noms_navires) else noms_navires[i],
            "Classe_Navire": random.choice(classes_navires),
            "Statut_Operationnel": random.choices(['En Mission', 'A Quai', 'En Maintenance'], weights=[70, 20, 10])[0],
            "Date_Mise_En_Service": fake.date_of_birth(minimum_age=5, maximum_age=30)
        })
    flotte_navires_df = pd.DataFrame(navires)
    id_navires = flotte_navires_df['ID_Navire'].tolist()

    # --- 2. Catalogue des Capteurs ---
    types_capteurs = ['Radar de navigation', 'Sonar de coque', 'Système de communication', 'Capteur de température moteur', 'GPS', 'Radar de veille aérienne', 'Détecteur de radiation']
    fabricants = ['Thales', 'Safran', 'Naval Group', 'MBDA', 'Dassault']
    catalogue = []
    for i in range(NUM_TYPES_CAPTEURS):
        catalogue.append({
            "ID_Type_Capteur": f"SENS-{100+i}",
            "Type_Capteur": random.choice(types_capteurs) + f" Modèle {chr(65+i)}",
            "Fabricant": random.choice(fabricants),
            "Intervalle_Maintenance_Jours": random.randint(90, 365),
            "Cout_Maintenance_Standard": round(random.uniform(5000, 50000), 2)
        })
    catalogue_capteurs_df = pd.DataFrame(catalogue)

    # --- 3. Inventaire des Équipements par Navire ---
    inventaire = []
    equip_counter = 1
    for navire_id in id_navires:
        num_equip = random.randint(20, 50)
        for _ in range(num_equip):
            capteur_type = catalogue_capteurs_df.sample(1).iloc[0]
            date_installation = fake.date_time_between(start_date='-10y', end_date='-1y')
            inventaire.append({
                "ID_Equipement_Unique": f"{navire_id}-E{equip_counter}",
                "ID_Navire": navire_id,
                "ID_Type_Capteur": capteur_type['ID_Type_Capteur'],
                "Date_Installation": date_installation,
                "Derniere_Maintenance": fake.date_time_between(start_date='-1y', end_date='now')
            })
            equip_counter += 1
    inventaire_equipements_df = pd.DataFrame(inventaire)
    id_equipements = inventaire_equipements_df['ID_Equipement_Unique'].tolist()

    # --- 4. Journal de Maintenance ---
    journal = []
    for i in range(NUM_INTERVENTIONS_MAINTENANCE_MAX):
        equipement = inventaire_equipements_df.sample(1).iloc[0]
        type_maintenance = random.choices(['Préventive', 'Corrective'], weights=[80, 20])[0]
        cout_standard = catalogue_capteurs_df[catalogue_capteurs_df['ID_Type_Capteur'] == equipement['ID_Type_Capteur']].iloc[0]['Cout_Maintenance_Standard']
        journal.append({
            "ID_Maintenance": f"MAINT-{10000+i}",
            "ID_Equipement_Unique": equipement['ID_Equipement_Unique'],
            "Date_Maintenance": fake.date_time_between(start_date='-5y', end_date='now'),
            "Type_Maintenance": type_maintenance,
            "Resultat": random.choices(['Succès', 'Remplacement requis', 'Échec'], weights=[95, 4, 1])[0],
            "Cout_Reel": round(cout_standard * random.uniform(0.9, 1.5 if type_maintenance == 'Corrective' else 1.1), 2)
        })
    journal_maintenance_df = pd.DataFrame(journal)

    # --- 5. Télémétrie Temps Réel ---
    telemetrie = []
    for _ in range(NUM_TELEMETRIE_READINGS):
        statut = random.choices(['NORMAL', 'AVERTISSEMENT', 'CRITIQUE'], weights=[97, 2.5, 0.5])[0]
        valeur = 0
        if statut == 'NORMAL': valeur = random.uniform(20, 80)
        elif statut == 'AVERTISSEMENT': valeur = random.uniform(80, 100)
        else: valeur = random.uniform(100, 120)

        telemetrie.append({
            "ID_Lecture": fake.uuid4(),
            "ID_Equipement_Unique": random.choice(id_equipements),
            "Timestamp_Lecture": fake.date_time_between(start_date='-7d', end_date='now', tzinfo=None),
            "Valeur": round(valeur, 4),
            "Statut_Alerte": statut
        })
    telemetrie_temps_reel_df = pd.DataFrame(telemetrie)

    logging.info(f"Généré {len(flotte_navires_df)} navires, {len(inventaire_equipements_df)} équipements, et {len(telemetrie_temps_reel_df)} lectures de télémétrie.")

    return {
        "flotte_navires": flotte_navires_df,
        "catalogue_capteurs": catalogue_capteurs_df,
        "inventaire_equipements_navires": inventaire_equipements_df,
        "journal_maintenance": journal_maintenance_df,
        "telemetrie_temps_reel": telemetrie_temps_reel_df
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Générer les données de maintenance navale et déployer les Data Products.")
    parser.add_argument('--deploy-only', action='store_true', help='Sauter la création du schéma et l\'ingestion des données, déployer uniquement les Data Products.')
    args = parser.parse_args()

    config = get_config()
    engine_string = f"trino://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['catalog']}"

    try:
        engine = create_engine(engine_string)

        if not args.deploy_only:
            if setup_schema(engine, config['catalog'], config['schema'], config['location']):
                data_tables = generate_naval_data()
                upload_to_starburst_parallel(engine, config['schema'], data_tables)
            else:
                logging.error("La création du schéma a échoué. Déploiement des Data Products annulé.")
                sys.exit(1)
        else:
            logging.info("Mode déploiement seul : Création du schéma et ingestion des données sautées.")

        deploy_path = os.path.dirname(os.path.abspath(__file__))
        scan_and_deploy(deploy_path)

        logging.info("Pipeline de données de maintenance navale exécuté avec succès.")

    except Exception as e:
        logging.error(f"L'exécution du pipeline a échoué : {e}")
