"""Reconstruction des mutations multi-lignes et filtres de la couche silver."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import pytest

from dvf_lakehouse import config
from dvf_lakehouse.duckdb_pipeline.connection import lire_parquet
from dvf_lakehouse.duckdb_pipeline.silver import SILVER_HIVE

MOTEURS = ["duckdb", "spark"]

_spec = importlib.util.spec_from_file_location(
    "generer_fixture", Path(__file__).parent / "fixtures" / "generer_fixture.py")
generer_fixture = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(generer_fixture)
ATTENDUS = generer_fixture.ATTENDUS


def _mutations(lac, engine):
    source = lire_parquet(config.chemins(engine, lac).silver_mutations, SILVER_HIVE)
    df = duckdb.sql(f"SELECT * FROM {source}").df()
    return df.set_index("id_mutation")


def _ventes(lac, engine):
    source = lire_parquet(config.chemins(engine, lac).silver_ventes, SILVER_HIVE)
    return duckdb.sql(f"SELECT * FROM {source}").df().set_index("id_mutation")


@pytest.mark.parametrize("engine", MOTEURS)
def test_une_ligne_par_mutation(lac, engine):
    mutations = _mutations(lac, engine)
    assert mutations.index.is_unique
    bronze = lire_parquet(config.chemins(engine, lac).bronze, {"annee": "INTEGER", "code_departement": "VARCHAR"})
    assert len(mutations) == duckdb.sql(f"SELECT count(DISTINCT id_mutation) FROM {bronze}").fetchone()[0]


@pytest.mark.parametrize("engine", MOTEURS)
@pytest.mark.parametrize("id_mutation, motif", sorted(ATTENDUS.items()))
def test_motif_des_cas_particuliers(lac, engine, id_mutation, motif):
    obtenu = _mutations(lac, engine).loc[id_mutation, "motif_exclusion"]
    assert (None if obtenu is None or obtenu != obtenu else obtenu) == motif


@pytest.mark.parametrize("engine", MOTEURS)
def test_valeur_repetee_comptee_une_fois(lac, engine):
    """Maison + deux terrains : trois lignes, une vente, valeur non multipliée."""
    v = _ventes(lac, engine).loc["2024-T-MAISON-TERRAINS"]
    assert v.valeur_fonciere == 495000
    assert v.surface_reelle_bati == 100
    assert v.prix_m2 == pytest.approx(4950)
    assert v.nb_parcelles == 3
    m = _mutations(lac, engine).loc["2024-T-MAISON-TERRAINS"]
    assert (m.nb_lignes, m.nb_logements, m.nb_valeurs) == (3, 1, 1)


@pytest.mark.parametrize("engine", MOTEURS)
def test_maison_repetee_par_nature_de_culture(lac, engine):
    """La même maison sur deux lignes (sols + jardin) reste un seul logement."""
    m = _mutations(lac, engine).loc["2024-T-MAISON-DOUBLON"]
    assert (m.nb_lignes, m.nb_logements) == (2, 1)
    assert _ventes(lac, engine).loc["2024-T-MAISON-DOUBLON"].prix_m2 == pytest.approx(5000)


@pytest.mark.parametrize("engine", MOTEURS)
def test_appartement_avec_dependance(lac, engine):
    v = _ventes(lac, engine).loc["2024-T-APPART-DEP"]
    assert v.type_local == "Appartement"
    assert v.nb_dependances == 1
    assert v.prix_m2 == pytest.approx(230000 / 50)


@pytest.mark.parametrize("engine", MOTEURS)
def test_deux_appartements_identiques_mais_lots_differents(lac, engine):
    m = _mutations(lac, engine).loc["2024-T-DEUX-APPARTS-IDENTIQUES"]
    assert m.nb_logements == 2


@pytest.mark.parametrize("engine", MOTEURS)
def test_reference_mad(lac, engine):
    mutations = _mutations(lac, engine)
    # Bordeaux : assez de ventes pour une référence communale.
    assert mutations.loc["2024-T-CHER", "reference_mad"] == "commune"
    assert mutations.loc["2024-T-CHER", "score_mad"] > config.REGLES.mad_seuil
    # Pessac : trop peu de ventes, repli sur la Gironde.
    assert mutations.loc["2024-T-PESSAC", "reference_mad"] == "departement"
    # Guéret : ni la commune ni la Creuse n'atteignent le seuil, pas de filtre MAD.
    gueret = mutations.loc["2024-T-GUERET"]
    assert gueret.reference_mad is None and gueret.score_mad != gueret.score_mad  # NaN


@pytest.mark.parametrize("engine", MOTEURS)
def test_ventes_respectent_les_bornes(lac, engine):
    v = _ventes(lac, engine)
    r = config.REGLES
    assert set(v.type_local) == {"Appartement", "Maison"}
    assert v.surface_reelle_bati.between(r.surface_min, r.surface_max).all()
    assert v.prix_m2.between(r.prix_m2_min, r.prix_m2_max).all()
    assert (v.valeur_fonciere >= r.valeur_min).all()
    assert (v.score_mad.dropna() <= r.mad_seuil).all()
    assert (v.prix_m2 * v.surface_reelle_bati - v.valeur_fonciere).abs().max() < 0.01


@pytest.mark.parametrize("engine", MOTEURS)
def test_ventes_ordinaires_conservees(lac, engine):
    """Les ventes simples tirées au hasard ne doivent presque jamais être exclues."""
    m = _mutations(lac, engine)
    ordinaires = m[~m.index.str.contains("-T-")]
    assert ordinaires.motif_exclusion.isna().mean() > 0.97
