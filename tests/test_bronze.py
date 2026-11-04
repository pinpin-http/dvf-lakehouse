from __future__ import annotations

import gzip
import shutil

import duckdb
import pytest

from dvf_lakehouse import config
from dvf_lakehouse.duckdb_pipeline.connection import lire_parquet
from dvf_lakehouse.duckdb_pipeline.silver import BRONZE_HIVE
from dvf_lakehouse.pipeline import executer
from dvf_lakehouse.schema import NOMS, verifier_entete

MOTEURS = ["duckdb", "spark"]


def _lignes_fixture(fixture_csv) -> int:
    with gzip.open(fixture_csv, "rt") as f:
        return sum(1 for _ in f) - 1


@pytest.mark.parametrize("engine", MOTEURS)
def test_bronze_garde_toutes_les_lignes(lac, fixture_csv, engine):
    bronze = lire_parquet(config.chemins(engine, lac).bronze, BRONZE_HIVE)
    n = duckdb.sql(f"SELECT count(*) FROM {bronze}").fetchone()[0]
    assert n == _lignes_fixture(fixture_csv)


@pytest.mark.parametrize("engine", MOTEURS)
def test_bronze_partitionne_par_annee_et_departement(lac, engine):
    racine = config.chemins(engine, lac).bronze
    annees = sorted(p.name for p in racine.glob("annee=*"))
    assert annees == [f"annee={a}" for a in range(2021, 2027)]
    departements = {p.name for p in racine.glob("annee=2024/code_departement=*")}
    assert {"code_departement=33", "code_departement=75", "code_departement=23"} <= departements


@pytest.mark.parametrize("engine", MOTEURS)
def test_bronze_types_et_guillemets(lac, engine):
    bronze = lire_parquet(config.chemins(engine, lac).bronze, BRONZE_HIVE)
    types = {r[0]: r[1] for r in duckdb.sql(f"DESCRIBE SELECT * FROM {bronze}").fetchall()}
    assert set(types) == set(NOMS) | {"annee"}
    assert types["valeur_fonciere"] == "DECIMAL(15,2)"
    assert types["date_mutation"] == "DATE"
    assert types["surface_reelle_bati"] == "INTEGER"
    assert types["code_departement"] == "VARCHAR"
    adresse = duckdb.sql(
        f"SELECT adresse_nom_voie FROM {bronze} WHERE id_mutation = '2024-T-APPART-DEP' AND code_type_local = 2"
    ).fetchone()[0]
    assert adresse == "RTE D'AIGRE , LES \"CHATELETS\""


def _fixture_alteree(tmp_path, fixture_csv, ancien: str, nouveau: str):
    with gzip.open(fixture_csv, "rt") as f:
        texte = f.read()
    assert ancien in texte
    cible = tmp_path / "altere.csv.gz"
    with gzip.open(cible, "wt") as f:
        f.write(texte.replace(ancien, nouveau, 1))
    return cible


@pytest.mark.parametrize("engine", MOTEURS)
def test_bronze_refuse_une_valeur_non_conforme(tmp_path, fixture_csv, spark, engine):
    # Une surface décimale dans une colonne entière doit arrêter la lecture.
    altere = _fixture_alteree(tmp_path, fixture_csv, ",Appartement,50,", ",Appartement,50.5,")
    with pytest.raises(Exception):
        executer(["bronze"], engine, fichiers=[altere], lake_dir=tmp_path / "lac", spark=spark, journaliser=False)


def test_entete_verifiee(tmp_path, fixture_csv):
    altere = _fixture_alteree(tmp_path, fixture_csv, "valeur_fonciere", "valeur")
    with pytest.raises(ValueError, match="en-tête inattendu"):
        verifier_entete(altere)
    shutil.copy(fixture_csv, tmp_path / "ok.csv.gz")
    verifier_entete(tmp_path / "ok.csv.gz")


def test_fixture_a_jour(fixture_csv):
    """La fixture commitée correspond au générateur."""
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "generer_fixture", Path(__file__).parent / "fixtures" / "generer_fixture.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with gzip.open(fixture_csv, "rt", encoding="utf-8") as f:
        assert f.read() == module.contenu()
