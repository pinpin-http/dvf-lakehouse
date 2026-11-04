"""Silver DuckDB : même logique que spark_pipeline/silver.py, écrite en SQL.

1. regrouper les lignes DVF par id_mutation (la valeur foncière y est répétée) ;
2. compter les logements distincts, dépendances et locaux d'activité ;
3. attribuer un motif d'exclusion (premier motif applicable, voir config.MOTIFS) ;
4. filtre robuste sur ln(prix au m²) : écart à la médiane rapporté au MAD,
   par commune x type, ou par département x type si la commune a trop peu de ventes.
"""

from __future__ import annotations

import shutil

import duckdb

from dvf_lakehouse.config import REGLES, Chemins
from dvf_lakehouse.duckdb_pipeline.connection import compter, copier, lire_parquet

BRONZE_HIVE = {"annee": "INTEGER", "code_departement": "VARCHAR"}
SILVER_HIVE = {"annee": "INTEGER"}

COLONNES_MUTATIONS = """
    id_mutation, date_mutation, annee, nature_mutation, nb_lignes, nb_dispositions,
    nb_valeurs, valeur_fonciere, nb_parcelles, nb_logements, nb_dependances,
    nb_locaux_activite, type_local, surface_reelle_bati, nombre_pieces_principales,
    code_departement, code_commune, nom_commune, code_postal, longitude, latitude,
    prix_m2, reference_mad, score_mad, motif_exclusion
"""

COLONNES_VENTES = """
    id_mutation, date_mutation, annee, mois, semestre, code_departement, code_commune,
    nom_commune, code_postal, type_local, valeur_fonciere, surface_reelle_bati,
    nombre_pieces_principales, nb_dependances, nb_parcelles, prix_m2, longitude, latitude,
    reference_mad, score_mad
"""


def requete_mutations(source_bronze: str) -> str:
    # La même maison peut apparaître sur plusieurs lignes (une par nature de culture
    # du terrain) : un local est identifié par parcelle, type, surface, pièces et lot.
    cle_local = " || '|' || ".join(
        f"coalesce(CAST({c} AS VARCHAR), '')"
        for c in ("id_parcelle", "code_type_local", "surface_reelle_bati", "nombre_pieces_principales", "lot1_numero")
    )
    logement = "code_type_local IN (1, 2)"
    return f"""
        WITH lignes AS (
            SELECT *, {cle_local} AS cle_local FROM {source_bronze}
        ),
        agregees AS (
            SELECT
                id_mutation,
                min(date_mutation) AS date_mutation,
                min(nature_mutation) AS nature_mutation,
                count(*) AS nb_lignes,
                count(DISTINCT numero_disposition) AS nb_dispositions,
                count(DISTINCT valeur_fonciere) AS nb_valeurs,
                max(valeur_fonciere) AS valeur_fonciere,
                count(DISTINCT id_parcelle) AS nb_parcelles,
                count(DISTINCT cle_local) FILTER (WHERE {logement}) AS nb_logements,
                count(DISTINCT cle_local) FILTER (WHERE code_type_local = 3) AS nb_dependances,
                count(DISTINCT cle_local) FILTER (WHERE code_type_local = 4) AS nb_locaux_activite,
                max(type_local) FILTER (WHERE {logement}) AS type_local,
                max(surface_reelle_bati) FILTER (WHERE {logement}) AS surface_reelle_bati,
                max(nombre_pieces_principales) FILTER (WHERE {logement}) AS nombre_pieces_principales,
                max(code_departement) AS code_departement,
                coalesce(max(code_commune) FILTER (WHERE {logement}), max(code_commune)) AS code_commune,
                coalesce(max(nom_commune) FILTER (WHERE {logement}), max(nom_commune)) AS nom_commune,
                coalesce(max(code_postal) FILTER (WHERE {logement}), max(code_postal)) AS code_postal,
                max(longitude) FILTER (WHERE {logement}) AS longitude,
                max(latitude) FILTER (WHERE {logement}) AS latitude
            FROM lignes
            GROUP BY id_mutation
        )
        SELECT *,
            CAST(year(date_mutation) AS INTEGER) AS annee,
            CASE WHEN surface_reelle_bati > 0 AND valeur_fonciere IS NOT NULL
                 THEN CAST(valeur_fonciere AS DOUBLE) / surface_reelle_bati END AS prix_m2
        FROM agregees
    """


def expression_motif(r=REGLES) -> str:
    return f"""
        CASE
            WHEN nature_mutation <> '{r.nature_mutation}' THEN 'nature_mutation'
            WHEN nb_dispositions > 1 THEN 'plusieurs_dispositions'
            WHEN nb_valeurs = 0 THEN 'valeur_absente'
            WHEN nb_valeurs > 1 THEN 'valeurs_multiples'
            WHEN nb_locaux_activite > 0 THEN 'local_activite'
            WHEN nb_logements = 0 THEN 'sans_logement'
            WHEN nb_logements > 1 THEN 'plusieurs_logements'
            WHEN surface_reelle_bati IS NULL OR surface_reelle_bati = 0 THEN 'surface_absente'
            WHEN surface_reelle_bati < {r.surface_min} OR surface_reelle_bati > {r.surface_max}
                THEN 'surface_hors_bornes'
            WHEN valeur_fonciere < {r.valeur_min} THEN 'valeur_hors_bornes'
            WHEN prix_m2 < {r.prix_m2_min} OR prix_m2 > {r.prix_m2_max} THEN 'prix_m2_hors_bornes'
        END
    """


def _reference(niveau: str, suffixe: str) -> str:
    return f"""
        med_{suffixe} AS (
            SELECT {niveau}, type_local, count(*) AS n_{suffixe},
                   quantile_cont(ln_prix, 0.5) AS med_{suffixe}
            FROM candidats GROUP BY {niveau}, type_local
        ),
        ref_{suffixe} AS (
            SELECT m.{niveau}, m.type_local, m.n_{suffixe}, m.med_{suffixe},
                   quantile_cont(abs(c.ln_prix - m.med_{suffixe}), 0.5) AS mad_{suffixe}
            FROM candidats c JOIN med_{suffixe} m USING ({niveau}, type_local)
            GROUP BY m.{niveau}, m.type_local, m.n_{suffixe}, m.med_{suffixe}
        )
    """


def requete_scores(r=REGLES) -> str:
    commune_ok = f"(n_c >= {r.mad_effectif_min} AND mad_c > 0)"
    dep_ok = f"(n_d >= {r.mad_effectif_min} AND mad_d > 0)"
    return f"""
        WITH candidats AS (
            SELECT id_mutation, code_commune, code_departement, type_local, ln(prix_m2) AS ln_prix
            FROM mutations_brutes WHERE motif_pre IS NULL
        ),
        {_reference("code_commune", "c")},
        {_reference("code_departement", "d")},
        choix AS (
            SELECT id_mutation, ln_prix,
                CASE WHEN {commune_ok} THEN 'commune' WHEN {dep_ok} THEN 'departement' END AS reference_mad,
                CASE WHEN {commune_ok} THEN med_c WHEN {dep_ok} THEN med_d END AS med,
                CASE WHEN {commune_ok} THEN mad_c WHEN {dep_ok} THEN mad_d END AS mad
            FROM candidats
            JOIN ref_c USING (code_commune, type_local)
            JOIN ref_d USING (code_departement, type_local)
        )
        SELECT id_mutation, reference_mad, abs(ln_prix - med) / ({r.mad_facteur} * mad) AS score_mad
        FROM choix
    """


def construire(con: duckdb.DuckDBPyConnection, chemins: Chemins, r=REGLES) -> dict:
    bronze = lire_parquet(chemins.bronze, BRONZE_HIVE)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE mutations_brutes AS
        SELECT *, {expression_motif(r)} AS motif_pre FROM ({requete_mutations(bronze)})
    """)
    con.execute(f"CREATE OR REPLACE TEMP TABLE scores AS {requete_scores(r)}")

    finales = f"""
        SELECT {COLONNES_MUTATIONS} FROM (
            SELECT m.*, s.reference_mad, s.score_mad,
                coalesce(m.motif_pre,
                         CASE WHEN s.score_mad > {r.mad_seuil} THEN 'prix_aberrant' END) AS motif_exclusion
            FROM mutations_brutes m LEFT JOIN scores s USING (id_mutation)
        )
    """
    for chemin in (chemins.silver_mutations, chemins.silver_ventes):
        shutil.rmtree(chemin, ignore_errors=True)
    copier(con, finales, chemins.silver_mutations, ("annee",))
    con.execute("DROP TABLE mutations_brutes; DROP TABLE scores")

    ventes = f"""
        SELECT {COLONNES_VENTES} FROM (
            SELECT * REPLACE (CAST(valeur_fonciere AS DOUBLE) AS valeur_fonciere),
                CAST(month(date_mutation) AS INTEGER) AS mois,
                CAST(annee AS VARCHAR) || '-S' || CASE WHEN month(date_mutation) <= 6 THEN '1' ELSE '2' END
                    AS semestre
            FROM {lire_parquet(chemins.silver_mutations, SILVER_HIVE)}
            WHERE motif_exclusion IS NULL
        )
    """
    copier(con, ventes, chemins.silver_ventes, ("annee",))
    return {
        "mutations": compter(con, lire_parquet(chemins.silver_mutations, SILVER_HIVE)),
        "ventes": compter(con, lire_parquet(chemins.silver_ventes, SILVER_HIVE)),
    }
