"""Schéma explicite des fichiers géo-DVF, décliné pour Spark et pour DuckDB.

Le schéma est écrit une seule fois ici. Les deux moteurs lisent le CSV avec ces
types et refusent toute valeur qui ne s'y convertit pas.
"""

from __future__ import annotations

import gzip
from pathlib import Path

# (nom, type logique). Les identifiants restent des chaînes : codes INSEE avec
# zéro initial, départements "2A"/"2B", numéros de disposition "000001".
COLONNES: tuple[tuple[str, str], ...] = (
    ("id_mutation", "string"),
    ("date_mutation", "date"),
    ("numero_disposition", "string"),
    ("nature_mutation", "string"),
    ("valeur_fonciere", "decimal"),
    ("adresse_numero", "int"),
    ("adresse_suffixe", "string"),
    ("adresse_nom_voie", "string"),
    ("adresse_code_voie", "string"),
    ("code_postal", "string"),
    ("code_commune", "string"),
    ("nom_commune", "string"),
    ("code_departement", "string"),
    ("ancien_code_commune", "string"),
    ("ancien_nom_commune", "string"),
    ("id_parcelle", "string"),
    ("ancien_id_parcelle", "string"),
    ("numero_volume", "string"),
    ("lot1_numero", "string"),
    ("lot1_surface_carrez", "double"),
    ("lot2_numero", "string"),
    ("lot2_surface_carrez", "double"),
    ("lot3_numero", "string"),
    ("lot3_surface_carrez", "double"),
    ("lot4_numero", "string"),
    ("lot4_surface_carrez", "double"),
    ("lot5_numero", "string"),
    ("lot5_surface_carrez", "double"),
    ("nombre_lots", "int"),
    ("code_type_local", "int"),
    ("type_local", "string"),
    ("surface_reelle_bati", "int"),
    ("nombre_pieces_principales", "int"),
    ("code_nature_culture", "string"),
    ("nature_culture", "string"),
    ("code_nature_culture_speciale", "string"),
    ("nature_culture_speciale", "string"),
    ("surface_terrain", "int"),
    ("longitude", "double"),
    ("latitude", "double"),
)
NOMS = tuple(nom for nom, _ in COLONNES)

# Valeur foncière : jusqu'à 14 milliards d'euros observés, centimes inclus.
DECIMAL_PRECISION, DECIMAL_ECHELLE = 15, 2

# Colonnes de partition ajoutées en bronze.
PARTITIONS = ("annee", "code_departement")

_DUCKDB = {
    "string": "VARCHAR",
    "date": "DATE",
    "decimal": f"DECIMAL({DECIMAL_PRECISION},{DECIMAL_ECHELLE})",
    "int": "INTEGER",
    "double": "DOUBLE",
}


def duckdb_colonnes() -> dict[str, str]:
    """Dictionnaire {colonne: type} pour l'option `columns` de read_csv."""
    return {nom: _DUCKDB[t] for nom, t in COLONNES}


def spark_schema():
    from pyspark.sql import types as T

    correspondance = {
        "string": T.StringType(),
        "date": T.DateType(),
        "decimal": T.DecimalType(DECIMAL_PRECISION, DECIMAL_ECHELLE),
        "int": T.IntegerType(),
        "double": T.DoubleType(),
    }
    return T.StructType([T.StructField(nom, correspondance[t], True) for nom, t in COLONNES])


def spark_schema_bronze():
    """Schéma de relecture du bronze, colonnes de partition comprises."""
    from pyspark.sql import types as T

    champs = [f for f in spark_schema().fields if f.name != "code_departement"]
    champs += [T.StructField("annee", T.IntegerType(), True),
               T.StructField("code_departement", T.StringType(), True)]
    return T.StructType(champs)


def verifier_entete(fichier: Path) -> None:
    """Vérifie que la première ligne du CSV porte exactement les colonnes attendues."""
    ouvrir = gzip.open if str(fichier).endswith(".gz") else open
    with ouvrir(fichier, "rt", encoding="utf-8") as f:
        entete = f.readline().rstrip("\r\n").split(",")
    if tuple(entete) != NOMS:
        manquantes = set(NOMS) - set(entete)
        en_trop = set(entete) - set(NOMS)
        raise ValueError(
            f"{fichier.name} : en-tête inattendu (manquantes={sorted(manquantes)}, "
            f"en trop={sorted(en_trop)}, ordre identique={set(entete) == set(NOMS)})"
        )
