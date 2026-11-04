"""Couche gold : valeurs recalculées à la main et égalité Spark / DuckDB."""

from __future__ import annotations

import duckdb
import numpy as np
import pytest

from dvf_lakehouse import config
from dvf_lakehouse.compare import comparer
from dvf_lakehouse.duckdb_pipeline.connection import lire_parquet
from dvf_lakehouse.duckdb_pipeline.silver import SILVER_HIVE
from dvf_lakehouse.quality import verifier

MOTEURS = ["duckdb", "spark"]


def _gold(lac, engine, table):
    return duckdb.sql(f"SELECT * FROM {lire_parquet(config.chemins(engine, lac).gold(table))}").df()


def _ventes(lac, engine):
    return duckdb.sql(f"SELECT * FROM {lire_parquet(config.chemins(engine, lac).silver_ventes, SILVER_HIVE)}").df()


def test_spark_et_duckdb_donnent_les_memes_resultats(lac):
    ecarts = comparer(lac)
    assert {e.table for e in ecarts} == {f"gold.{t}" for t in config.GOLD_TABLES} | {
        "silver.ventes", "silver.mutations"}
    for e in ecarts:
        assert e.ok, f"{e.table} : {e.details}"


@pytest.mark.parametrize("engine", MOTEURS)
def test_mediane_commune_recalculee(lac, engine):
    ventes = _ventes(lac, engine)
    gold = _gold(lac, engine, "prix_commune_annee").set_index(["code_commune", "annee", "type_local"])
    for (commune, annee, type_local), groupe in ventes.groupby(["code_commune", "annee", "type_local"]):
        if len(groupe) < config.GOLD_EFFECTIF_MIN:
            assert (commune, annee, type_local) not in gold.index
            continue
        ligne = gold.loc[(commune, annee, type_local)]
        assert ligne.nb_ventes == len(groupe)
        assert ligne.prix_m2_median == pytest.approx(np.median(groupe.prix_m2), rel=1e-12)
        assert ligne.prix_m2_q1 == pytest.approx(np.quantile(groupe.prix_m2, 0.25), rel=1e-12)


@pytest.mark.parametrize("engine", MOTEURS)
def test_evolution_annuelle(lac, engine):
    gold = _gold(lac, engine, "prix_commune_annee")
    bx = gold[(gold.code_commune == "33063") & (gold.type_local == "Appartement")].set_index("annee")
    assert np.isnan(bx.loc[2021, "evolution_prix_m2_pct"])
    attendu = 100 * (bx.loc[2023, "prix_m2_median"] / bx.loc[2022, "prix_m2_median"] - 1)
    assert bx.loc[2023, "evolution_prix_m2_pct"] == pytest.approx(attendu)
    # 2021 ne couvre que six mois : pas d'évolution des volumes 2021 -> 2022.
    assert np.isnan(bx.loc[2022, "evolution_nb_ventes_pct"])
    assert bx.loc[2022, "annee_complete"]
    assert not bx.loc[2021, "annee_complete"]


@pytest.mark.parametrize("engine", MOTEURS)
def test_totaux_gold(lac, engine):
    n = len(_ventes(lac, engine))
    assert _gold(lac, engine, "marche_france_mois").nb_ventes.sum() == n
    annee = _gold(lac, engine, "marche_annee")
    assert annee[annee.perimetre == "France"].nb_ventes.sum() == n
    bm = _gold(lac, engine, "bordeaux_metropole")
    assert set(bm.code_commune) <= set(config.BORDEAUX_METROPOLE)
    assert bm.groupby(["annee", "type_local"]).rang_prix.min().eq(1).all()


@pytest.mark.parametrize("engine", MOTEURS)
def test_controles_qualite_passent_sur_la_fixture(lac, fixture_csv, engine):
    controles = verifier(engine, fichiers=[fixture_csv], lake_dir=lac, metropole_complete=False)
    echecs = [c for c in controles if c.statut == "echec"]
    assert not echecs, echecs
    assert any(c.nom == "lignes bronze = lignes des CSV source" and c.statut == "ok" for c in controles)
