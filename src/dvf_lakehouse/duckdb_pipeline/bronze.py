"""Bronze DuckDB : CSV.gz -> Parquet partitionné par année et département."""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb

from dvf_lakehouse.config import Chemins
from dvf_lakehouse.duckdb_pipeline.connection import compter, copier, lire_parquet, litteral
from dvf_lakehouse.schema import COLONNES, NOMS, PARTITIONS, duckdb_colonnes, verifier_entete

ENTIERS = tuple(nom for nom, t in COLONNES if t == "int")


def _entier_strict(colonne: str) -> str:
    # DuckDB convertit "50.5" en 51 sans broncher quand la colonne est INTEGER ;
    # Spark refuse. On lit en texte et on lève une erreur si ce n'est pas un entier.
    return (f"CASE WHEN {colonne} IS NULL OR regexp_full_match({colonne}, '-?[0-9]+') "
            f"THEN CAST({colonne} AS INTEGER) "
            f"ELSE error('{colonne} : entier attendu, lu ' || {colonne}) END AS {colonne}")


def requete_csv(fichiers: list[Path]) -> str:
    types = {nom: ("VARCHAR" if nom in ENTIERS else t) for nom, t in duckdb_colonnes().items()}
    colonnes = ", ".join(f"{litteral(nom)}: '{t}'" for nom, t in types.items())
    selection = ", ".join(_entier_strict(c) if c in ENTIERS else c for c in NOMS)
    liste = ", ".join(litteral(f) for f in fichiers)
    # auto_detect désactivé : l'en-tête est vérifié à part, les types viennent du schéma
    # et une valeur non convertible fait échouer la lecture (comme FAILFAST côté Spark).
    return f"""
        SELECT {selection}, CAST(year(date_mutation) AS INTEGER) AS annee
        FROM read_csv([{liste}],
            columns = {{{colonnes}}},
            header = true, auto_detect = false,
            delim = ',', quote = '"', escape = '"',
            dateformat = '%Y-%m-%d', strict_mode = true)
    """


def construire(con: duckdb.DuckDBPyConnection, fichiers: list[Path], chemins: Chemins) -> dict:
    for fichier in fichiers:
        verifier_entete(fichier)
    shutil.rmtree(chemins.bronze, ignore_errors=True)
    copier(con, requete_csv(fichiers), chemins.bronze, PARTITIONS)
    return {"lignes": compter(con, lire_parquet(chemins.bronze, {"annee": "INTEGER", "code_departement": "VARCHAR"}))}
