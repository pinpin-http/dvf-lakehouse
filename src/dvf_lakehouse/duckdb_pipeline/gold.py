"""Gold DuckDB : même logique que spark_pipeline/gold.py, écrite en SQL."""

from __future__ import annotations

import shutil

import duckdb

from dvf_lakehouse.config import (
    ANNEES_PARTIELLES, BORDEAUX_METROPOLE, CODE_BORDEAUX, GOLD_CLES, GOLD_EFFECTIF_MIN, Chemins,
)
from dvf_lakehouse.duckdb_pipeline.connection import compter, copier, lire_parquet
from dvf_lakehouse.duckdb_pipeline.silver import SILVER_HIVE

STATS_PRIX = """
    count(*) AS nb_ventes,
    quantile_cont(prix_m2, 0.5) AS prix_m2_median,
    quantile_cont(prix_m2, 0.25) AS prix_m2_q1,
    quantile_cont(prix_m2, 0.75) AS prix_m2_q3
"""
ANNEE_COMPLETE = f"annee NOT IN ({', '.join(str(a) for a in ANNEES_PARTIELLES)})"
COMMUNES_BM = ", ".join(f"'{c}'" for c in BORDEAUX_METROPOLE)


def _evolutions(partition: str) -> str:
    w = f"OVER (PARTITION BY {partition} ORDER BY annee)"
    suivie = f"lag(annee) {w} = annee - 1"
    return f"""
        CASE WHEN {suivie} THEN 100 * (prix_m2_median / lag(prix_m2_median) {w} - 1) END
            AS evolution_prix_m2_pct,
        CASE WHEN {suivie} AND lag(annee_complete) {w} AND annee_complete
             THEN 100 * (nb_ventes / lag(nb_ventes) {w} - 1) END AS evolution_nb_ventes_pct
    """


def prix_commune_annee(ventes: str) -> str:
    return f"""
        WITH agg AS (
            SELECT code_commune, annee, type_local,
                max(code_departement) AS code_departement,
                max(nom_commune) AS nom_commune,
                {STATS_PRIX},
                quantile_cont(CAST(surface_reelle_bati AS DOUBLE), 0.5) AS surface_mediane,
                quantile_cont(valeur_fonciere, 0.5) AS valeur_mediane,
                {ANNEE_COMPLETE} AS annee_complete
            FROM {ventes}
            GROUP BY code_commune, annee, type_local
            HAVING count(*) >= {GOLD_EFFECTIF_MIN}
        )
        SELECT code_departement, code_commune, nom_commune, annee, annee_complete, type_local,
            nb_ventes, prix_m2_median, prix_m2_q1, prix_m2_q3, surface_mediane, valeur_mediane,
            {_evolutions("code_commune, type_local")}
        FROM agg
    """


def marche_france_mois(ventes: str) -> str:
    return f"""
        SELECT annee, mois, type_local, count(*) AS nb_ventes,
            quantile_cont(prix_m2, 0.5) AS prix_m2_median
        FROM {ventes}
        GROUP BY annee, mois, type_local
    """


def _perimetres(ventes: str) -> str:
    return f"""
        SELECT 'France' AS perimetre, * FROM {ventes}
        UNION ALL
        SELECT 'Bordeaux Métropole', * FROM {ventes} WHERE code_commune IN ({COMMUNES_BM})
        UNION ALL
        SELECT 'Bordeaux', * FROM {ventes} WHERE code_commune = '{CODE_BORDEAUX}'
    """


def marche_annee(ventes: str) -> str:
    return f"""
        WITH agg AS (
            SELECT perimetre, annee, type_local, {STATS_PRIX}, {ANNEE_COMPLETE} AS annee_complete
            FROM ({_perimetres(ventes)})
            GROUP BY perimetre, annee, type_local
        )
        SELECT perimetre, annee, annee_complete, type_local, nb_ventes, prix_m2_median,
            prix_m2_q1, prix_m2_q3, {_evolutions("perimetre, type_local")}
        FROM agg
    """


def marche_semestre(ventes: str) -> str:
    return f"""
        SELECT perimetre, semestre, type_local, {STATS_PRIX}
        FROM ({_perimetres(ventes)})
        GROUP BY perimetre, semestre, type_local
    """


def bordeaux_metropole(communes: str) -> str:
    return f"""
        SELECT code_commune, nom_commune, annee, annee_complete, type_local, nb_ventes,
            prix_m2_median, prix_m2_q1, prix_m2_q3, surface_mediane, valeur_mediane,
            evolution_prix_m2_pct, evolution_nb_ventes_pct,
            CAST(rank() OVER (PARTITION BY annee, type_local ORDER BY prix_m2_median DESC) AS INTEGER)
                AS rang_prix
        FROM {communes}
        WHERE code_commune IN ({COMMUNES_BM})
    """


def construire(con: duckdb.DuckDBPyConnection, chemins: Chemins) -> dict:
    ventes = lire_parquet(chemins.silver_ventes, SILVER_HIVE)
    requetes = {
        "prix_commune_annee": prix_commune_annee(ventes),
        "marche_france_mois": marche_france_mois(ventes),
        "marche_annee": marche_annee(ventes),
        "marche_semestre": marche_semestre(ventes),
    }
    stats = {nom: _ecrire(con, requete, chemins, nom) for nom, requete in requetes.items()}
    # Le zoom métropole repart de la table commune déjà écrite.
    stats["bordeaux_metropole"] = _ecrire(
        con, bordeaux_metropole(lire_parquet(chemins.gold("prix_commune_annee"))), chemins, "bordeaux_metropole")
    return stats


def _ecrire(con: duckdb.DuckDBPyConnection, requete: str, chemins: Chemins, nom: str) -> int:
    chemin = chemins.gold(nom)
    shutil.rmtree(chemin, ignore_errors=True)
    copier(con, f"SELECT * FROM ({requete}) ORDER BY {', '.join(GOLD_CLES[nom])}", chemin)
    return compter(con, lire_parquet(chemin))
