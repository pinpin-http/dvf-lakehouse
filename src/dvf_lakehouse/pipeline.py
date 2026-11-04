"""Lance une étape (bronze, silver, gold) avec l'un ou l'autre moteur."""

from __future__ import annotations

import json
import time
from pathlib import Path

from dvf_lakehouse import config

ETAPES = ("bronze", "silver", "gold")


def fichiers_sources(dossier: Path | None = None) -> list[Path]:
    dossier = dossier or config.RAW_DIR
    fichiers = [dossier / s.nom_fichier for s in config.SOURCES]
    manquants = [f.name for f in fichiers if not f.exists()]
    if manquants:
        raise FileNotFoundError(f"fichiers absents de {dossier} : {manquants} (lancer `make download`)")
    return fichiers


def _module(engine: str, etape: str):
    if engine == "spark":
        from dvf_lakehouse.spark_pipeline import bronze, gold, silver
    else:
        from dvf_lakehouse.duckdb_pipeline import bronze, gold, silver
    return {"bronze": bronze, "silver": silver, "gold": gold}[etape]


def executer(etapes: list[str] | tuple[str, ...], engine: str, *, fichiers: list[Path] | None = None,
             lake_dir: Path | None = None, spark=None, con=None, journaliser: bool = True) -> list[dict]:
    """Exécute les étapes demandées dans l'ordre, avec une seule session ou connexion.

    `spark` ou `con` permettent de réutiliser une session existante (tests).
    Le temps mesuré pour chaque étape exclut le démarrage du moteur, compté à part.
    """
    chemins = config.chemins(engine, lake_dir)
    debut = time.perf_counter()
    propre = False
    if engine == "spark" and spark is None:
        from dvf_lakehouse.spark_pipeline.session import creer_session
        spark, propre = creer_session(), True
    elif engine == "duckdb" and con is None:
        from dvf_lakehouse.duckdb_pipeline.connection import connecter
        con, propre = connecter(), True
    demarrage = time.perf_counter() - debut
    moteur = spark if engine == "spark" else con

    resultats = []
    try:
        for etape in etapes:
            module = _module(engine, etape)
            t0 = time.perf_counter()
            if etape == "bronze":
                stats = module.construire(moteur, fichiers or fichiers_sources(), chemins)
            else:
                stats = module.construire(moteur, chemins)
            resultat = {
                "moteur": engine,
                "etape": etape,
                "duree_s": round(time.perf_counter() - t0, 3),
                "demarrage_moteur_s": round(demarrage, 3),
                "stats": stats,
            }
            resultats.append(resultat)
            print(f"[{engine}] {etape} : {resultat['duree_s']:.1f} s {stats}", flush=True)
            if journaliser:
                journal = chemins.racine / "_runs" / f"{etape}.json"
                journal.parent.mkdir(parents=True, exist_ok=True)
                journal.write_text(json.dumps(resultat, indent=2, ensure_ascii=False))
    finally:
        if propre:
            spark.stop() if engine == "spark" else con.close()
    return resultats
