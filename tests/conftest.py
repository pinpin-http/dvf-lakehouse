from __future__ import annotations

from pathlib import Path

import pytest

from dvf_lakehouse.pipeline import ETAPES, executer

FIXTURE = Path(__file__).parent / "fixtures" / "dvf_fixture.csv.gz"


def pytest_configure(config):
    # --basetemp=.tmp/pytest (pyproject.toml) : pytest crée le dossier mais pas son parent,
    # absent d'un clone neuf.
    if config.option.basetemp:
        Path(config.option.basetemp).parent.mkdir(parents=True, exist_ok=True)


@pytest.fixture(scope="session")
def fixture_csv() -> Path:
    return FIXTURE


@pytest.fixture(scope="session")
def spark():
    from dvf_lakehouse.spark_pipeline.session import creer_session

    session = creer_session(master="local[2]", shuffle_partitions=4, driver_memory="1g")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def lac(tmp_path_factory, spark, fixture_csv) -> Path:
    """Lac construit sur la fixture par les deux moteurs."""
    racine = tmp_path_factory.mktemp("lac")
    executer(ETAPES, "duckdb", fichiers=[fixture_csv], lake_dir=racine, journaliser=False)
    executer(ETAPES, "spark", fichiers=[fixture_csv], lake_dir=racine, spark=spark, journaliser=False)
    return racine
