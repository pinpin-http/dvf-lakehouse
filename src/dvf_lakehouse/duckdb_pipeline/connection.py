"""Connexion DuckDB en mémoire, bridée pour une machine partagée."""

from __future__ import annotations

from pathlib import Path

import duckdb

from dvf_lakehouse import config


def connecter(threads: int | None = None, memory_limit: str | None = None) -> duckdb.DuckDBPyConnection:
    tmp = config.TMP_DIR / "duckdb"
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute(f"SET threads = {int(threads or config.DUCKDB_THREADS)}")
    con.execute(f"SET memory_limit = '{memory_limit or config.DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory = '{tmp}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("SET enable_progress_bar = false")
    con.execute("SET TimeZone = 'UTC'")
    return con


def litteral(chemin: Path | str) -> str:
    """Chemin en littéral SQL."""
    return "'" + str(chemin).replace("'", "''") + "'"


def lire_parquet(chemin: Path, hive: dict[str, str] | None = None) -> str:
    """Expression FROM pour un dossier Parquet (partitionné ou non)."""
    motif = litteral(Path(chemin) / "**" / "*.parquet")
    if not hive:
        return f"read_parquet({motif})"
    types = ", ".join(f"{litteral(k)}: '{v}'" for k, v in hive.items())
    return f"read_parquet({motif}, hive_partitioning = true, hive_types = {{{types}}})"


def copier(con: duckdb.DuckDBPyConnection, requete: str, cible: Path, partitions: tuple[str, ...] = ()) -> None:
    """Écrit le résultat d'une requête en Parquet zstd (dossier, partitionné ou non)."""
    if partitions:
        Path(cible).parent.mkdir(parents=True, exist_ok=True)
        options = f"FORMAT parquet, COMPRESSION zstd, PARTITION_BY ({', '.join(partitions)})"
        con.execute(f"COPY ({requete}) TO {litteral(cible)} ({options})")
    else:
        Path(cible).mkdir(parents=True, exist_ok=True)
        con.execute(f"COPY ({requete}) TO {litteral(Path(cible) / 'part-0.parquet')} (FORMAT parquet, COMPRESSION zstd)")


def compter(con: duckdb.DuckDBPyConnection, source: str) -> int:
    return con.execute(f"SELECT count(*) FROM {source}").fetchone()[0]
