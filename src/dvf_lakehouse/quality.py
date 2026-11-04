"""Contrôles qualité entre couches, avec rapport Markdown et JSON.

Les contrôles lisent les fichiers Parquet produits par un moteur (via DuckDB,
quel que soit le moteur qui les a écrits). Un contrôle « bloquant » en échec
fait sortir la commande avec le code 1 ; un contrôle « info » documente.
"""

from __future__ import annotations

import json
import sys
import time
import unicodedata
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from dvf_lakehouse import config
from dvf_lakehouse.duckdb_pipeline.bronze import requete_csv
from dvf_lakehouse.duckdb_pipeline.connection import connecter, lire_parquet
from dvf_lakehouse.duckdb_pipeline.silver import BRONZE_HIVE, SILVER_HIVE

# Emprises (lat min, lat max, lon min, lon max) par territoire.
EMPRISES = {
    "metropole": (41.0, 51.2, -5.3, 9.7),
    "971": (15.8, 18.2, -63.2, -60.9),
    "972": (14.3, 14.95, -61.3, -60.75),
    "973": (2.0, 5.9, -54.7, -51.5),
    "974": (-21.5, -20.8, 55.15, 55.9),
}


@dataclass
class Controle:
    couche: str
    nom: str
    statut: str  # "ok", "echec" ou "info"
    valeur: object
    attendu: str = ""


def _normaliser(nom: str) -> str:
    sans_accents = unicodedata.normalize("NFKD", nom).encode("ascii", "ignore").decode()
    mots = sans_accents.lower().replace("-", " ").replace("'", " ").split()
    abreviations = {"st": "saint", "ste": "sainte"}  # DVF écrit "St Louis de Montferrand"
    return " ".join(abreviations.get(m, m) for m in mots)


class Verificateur:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con
        self.controles: list[Controle] = []

    def valeur(self, sql: str):
        return self.con.execute(sql).fetchone()[0]

    def egal(self, couche: str, nom: str, obtenu, attendu) -> None:
        self.controles.append(Controle(couche, nom, "ok" if obtenu == attendu else "echec", obtenu,
                                       f"= {_format(attendu)}"))

    def zero(self, couche: str, nom: str, sql: str) -> None:
        self.egal(couche, nom, self.valeur(sql), 0)

    def info(self, couche: str, nom: str, valeur) -> None:
        self.controles.append(Controle(couche, nom, "info", valeur))


def _hors_emprise() -> str:
    conditions = []
    for code, (la0, la1, lo0, lo1) in EMPRISES.items():
        territoire = "code_departement NOT LIKE '97%'" if code == "metropole" else f"code_departement = '{code}'"
        conditions.append(f"({territoire} AND (latitude NOT BETWEEN {la0} AND {la1} "
                          f"OR longitude NOT BETWEEN {lo0} AND {lo1}))")
    return " OR ".join(conditions)


def verifier(engine: str, fichiers: list[Path] | None = None, lake_dir: Path | None = None,
             con: duckdb.DuckDBPyConnection | None = None, metropole_complete: bool = True) -> list[Controle]:
    """Lance tous les contrôles. `metropole_complete=False` saute la vérification des
    28 communes de Bordeaux Métropole (utile sur la fixture de test)."""
    from dvf_lakehouse.pipeline import fichiers_sources

    ch = config.chemins(engine, lake_dir)
    con = con or connecter()
    v = Verificateur(con)
    r = config.REGLES
    bronze = lire_parquet(ch.bronze, BRONZE_HIVE)
    mutations = lire_parquet(ch.silver_mutations, SILVER_HIVE)
    ventes = lire_parquet(ch.silver_ventes, SILVER_HIVE)
    gold = {t: lire_parquet(ch.gold(t)) for t in config.GOLD_TABLES}

    # --- source -> bronze ---------------------------------------------------
    fichiers = fichiers or fichiers_sources()
    n_csv = v.valeur(f"SELECT count(*) FROM ({requete_csv(fichiers)})")
    n_bronze = v.valeur(f"SELECT count(*) FROM {bronze}")
    v.egal("bronze", "lignes bronze = lignes des CSV source", n_bronze, n_csv)
    par_annee = con.execute(f"SELECT annee, count(*) FROM {bronze} GROUP BY 1 ORDER BY 1").fetchall()
    v.info("bronze", "lignes par année", {str(a): n for a, n in par_annee})
    v.info("bronze", "départements", v.valeur(f"SELECT count(DISTINCT code_departement) FROM {bronze}"))
    for col in ("id_mutation", "date_mutation", "nature_mutation", "code_commune", "code_departement", "id_parcelle"):
        v.zero("bronze", f"{col} jamais nul", f"SELECT count(*) FROM {bronze} WHERE {col} IS NULL")
    v.zero("bronze", "partition annee = année de date_mutation",
           f"SELECT count(*) FROM {bronze} WHERE annee <> year(date_mutation)")
    v.zero("bronze", "une seule date et un seul département par mutation",
           f"""SELECT count(*) FROM (SELECT id_mutation FROM {bronze} GROUP BY 1
               HAVING count(DISTINCT date_mutation) > 1 OR count(DISTINCT code_departement) > 1)""")
    v.zero("bronze", "code_type_local cohérent avec type_local",
           f"""SELECT count(*) FROM {bronze} WHERE NOT (
               (code_type_local IS NULL AND type_local IS NULL)
               OR (code_type_local = 1 AND type_local = 'Maison')
               OR (code_type_local = 2 AND type_local = 'Appartement')
               OR (code_type_local = 3 AND type_local = 'Dépendance')
               OR (code_type_local = 4 AND type_local LIKE 'Local industriel%'))""")
    v.info("bronze", "valeur_fonciere nulle (% des lignes)",
           round(v.valeur(f"SELECT 100 * avg((valeur_fonciere IS NULL)::INT) FROM {bronze}"), 3))
    v.info("bronze", "coordonnées absentes (% des lignes)",
           round(v.valeur(f"SELECT 100 * avg((latitude IS NULL)::INT) FROM {bronze}"), 3))
    v.info("bronze", "coordonnées hors de l'emprise du territoire (lignes)",
           v.valeur(f"SELECT count(*) FROM {bronze} WHERE latitude IS NOT NULL AND ({_hors_emprise()})"))

    # --- bronze -> silver ---------------------------------------------------
    n_id = v.valeur(f"SELECT count(DISTINCT id_mutation) FROM {bronze}")
    n_mut = v.valeur(f"SELECT count(*) FROM {mutations}")
    n_ventes = v.valeur(f"SELECT count(*) FROM {ventes}")
    v.egal("silver", "une ligne par mutation du bronze", n_mut, n_id)
    v.zero("silver", "id_mutation unique (mutations)",
           f"SELECT count(*) - count(DISTINCT id_mutation) FROM {mutations}")
    v.zero("silver", "id_mutation unique (ventes)", f"SELECT count(*) - count(DISTINCT id_mutation) FROM {ventes}")
    v.egal("silver", "ventes = mutations sans motif d'exclusion", n_ventes,
           v.valeur(f"SELECT count(*) FROM {mutations} WHERE motif_exclusion IS NULL"))
    v.zero("silver", "motifs connus",
           f"SELECT count(*) FROM {mutations} WHERE motif_exclusion NOT IN ({', '.join(repr(m) for m in config.MOTIFS)})")
    for col in ("date_mutation", "annee", "code_commune", "nom_commune", "type_local", "valeur_fonciere",
                "surface_reelle_bati", "prix_m2"):
        v.zero("silver", f"ventes : {col} jamais nul", f"SELECT count(*) FROM {ventes} WHERE {col} IS NULL")
    v.zero("silver", "ventes : type Appartement ou Maison",
           f"SELECT count(*) FROM {ventes} WHERE type_local NOT IN ('Appartement', 'Maison')")
    v.zero("silver", f"ventes : surface entre {r.surface_min:g} et {r.surface_max:g} m²",
           f"SELECT count(*) FROM {ventes} WHERE surface_reelle_bati NOT BETWEEN {r.surface_min} AND {r.surface_max}")
    v.zero("silver", f"ventes : valeur >= {r.valeur_min:g} €",
           f"SELECT count(*) FROM {ventes} WHERE valeur_fonciere < {r.valeur_min}")
    v.zero("silver", f"ventes : prix entre {r.prix_m2_min:g} et {r.prix_m2_max:g} €/m²",
           f"SELECT count(*) FROM {ventes} WHERE prix_m2 NOT BETWEEN {r.prix_m2_min} AND {r.prix_m2_max}")
    v.zero("silver", f"ventes : score MAD <= {r.mad_seuil}",
           f"SELECT count(*) FROM {ventes} WHERE score_mad > {r.mad_seuil}")
    v.zero("silver", "ventes : prix_m2 = valeur / surface",
           f"SELECT count(*) FROM {ventes} WHERE abs(prix_m2 * surface_reelle_bati - valeur_fonciere) > 0.01")
    entonnoir = con.execute(f"""
        SELECT coalesce(motif_exclusion, 'vente_retenue'), count(*) FROM {mutations} GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    v.info("silver", "mutations par motif d'exclusion", dict(entonnoir))
    v.info("silver", "référence du filtre MAD (ventes)", dict(con.execute(
        f"SELECT coalesce(reference_mad, 'aucune'), count(*) FROM {ventes} GROUP BY 1 ORDER BY 2 DESC").fetchall()))
    v.info("silver", "ventes sans coordonnées (%)",
           round(v.valeur(f"SELECT 100 * avg((latitude IS NULL)::INT) FROM {ventes}"), 3))

    # --- silver -> gold -----------------------------------------------------
    v.egal("gold", "somme des ventes mensuelles = ventes silver",
           v.valeur(f"SELECT sum(nb_ventes) FROM {gold['marche_france_mois']}"), n_ventes)
    v.egal("gold", "somme des ventes annuelles France = ventes silver",
           v.valeur(f"SELECT sum(nb_ventes) FROM {gold['marche_annee']} WHERE perimetre = 'France'"), n_ventes)
    codes_bm = ", ".join(f"'{c}'" for c in config.BORDEAUX_METROPOLE)
    v.egal("gold", "ventes annuelles Bordeaux Métropole = ventes silver des 28 communes",
           v.valeur(f"SELECT sum(nb_ventes) FROM {gold['marche_annee']} WHERE perimetre = 'Bordeaux Métropole'"),
           v.valeur(f"SELECT count(*) FROM {ventes} WHERE code_commune IN ({codes_bm})"))
    for table, cles in config.GOLD_CLES.items():
        v.zero("gold", f"{table} : clé unique ({', '.join(cles)})",
               f"SELECT count(*) - count(DISTINCT ({', '.join(cles)})) FROM {gold[table]}")
        if table != "marche_france_mois":
            v.zero("gold", f"{table} : q1 <= médiane <= q3",
                   f"SELECT count(*) FROM {gold[table]} WHERE NOT (prix_m2_q1 <= prix_m2_median AND prix_m2_median <= prix_m2_q3)")
        v.zero("gold", f"{table} : médiane dans les bornes de prix",
               f"SELECT count(*) FROM {gold[table]} WHERE prix_m2_median NOT BETWEEN {r.prix_m2_min} AND {r.prix_m2_max}")
    v.zero("gold", f"prix_commune_annee : au moins {config.GOLD_EFFECTIF_MIN} ventes par ligne",
           f"SELECT count(*) FROM {gold['prix_commune_annee']} WHERE nb_ventes < {config.GOLD_EFFECTIF_MIN}")
    v.info("gold", "lignes par table", {t: v.valeur(f"SELECT count(*) FROM {s}") for t, s in gold.items()})

    if not metropole_complete:
        return v.controles
    noms = dict(con.execute(
        f"SELECT code_commune, max(nom_commune) FROM {ventes} WHERE code_commune IN ({codes_bm}) GROUP BY 1").fetchall())
    v.egal("gold", "les 28 communes de Bordeaux Métropole ont des ventes", len(noms), 28)
    differents = {c: n for c, n in noms.items() if _normaliser(n) != _normaliser(config.BORDEAUX_METROPOLE[c])}
    v.egal("gold", "noms des communes de la métropole conformes aux codes INSEE", differents, {})
    return v.controles


def _format(valeur) -> str:
    if isinstance(valeur, dict):
        return "<br>".join(f"{k} : {_format(x)}" for k, x in valeur.items()) or "aucun"
    if isinstance(valeur, bool):
        return str(valeur)
    if isinstance(valeur, int):
        return f"{valeur:,}".replace(",", " ")
    if isinstance(valeur, float):
        return f"{valeur:g}".replace(".", ",")
    return str(valeur)


def rapport_markdown(engine: str, controles: list[Controle], duree: float) -> str:
    echecs = sum(c.statut == "echec" for c in controles)
    bloquants = sum(c.statut != "info" for c in controles)
    lignes = [
        f"# Contrôles qualité ({engine})",
        "",
        f"{bloquants - echecs} contrôles bloquants réussis sur {bloquants}, "
        f"{sum(c.statut == 'info' for c in controles)} indicateurs. Durée : {duree:.0f} s.",
        "",
        "Généré par `make quality ENGINE=" + engine + "`.",
    ]
    for couche in ("bronze", "silver", "gold"):
        lignes += ["", f"## {couche.capitalize()}", "", "| Contrôle | Statut | Valeur | Attendu |", "|---|---|---|---|"]
        for c in controles:
            if c.couche == couche:
                statut = {"ok": "ok", "echec": "**ÉCHEC**", "info": "info"}[c.statut]
                lignes.append(f"| {c.nom} | {statut} | {_format(c.valeur)} | {c.attendu} |")
    return "\n".join(lignes) + "\n"


def main(engine: str) -> int:
    debut = time.perf_counter()
    controles = verifier(engine)
    duree = time.perf_counter() - debut
    racine = config.chemins(engine).racine
    (racine / "_qualite.json").write_text(json.dumps([asdict(c) for c in controles], indent=2,
                                                     ensure_ascii=False, default=str))
    config.DOCS_DIR.mkdir(exist_ok=True)
    (config.DOCS_DIR / f"qualite_{engine}.md").write_text(rapport_markdown(engine, controles, duree))
    for c in controles:
        if c.statut != "info":
            print(f"{c.statut.upper():<6} {c.couche:<7} {c.nom} : {c.valeur}")
    echecs = [c for c in controles if c.statut == "echec"]
    print(f"{len(echecs)} échec(s), rapport : docs/qualite_{engine}.md")
    return 1 if echecs else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "duckdb"))
