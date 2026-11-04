"""Compare les sorties silver et gold des deux moteurs.

Règle : mêmes colonnes avec les mêmes types, mêmes clés, entiers et chaînes
identiques, flottants égaux à une tolérance relative près (les médianes passent
par des calculs en double précision faits par la JVM d'un côté et par DuckDB de
l'autre). La comparaison se fait en SQL (jointure sur la clé), ce qui tient sur
les 2 à 3 millions de ventes du jeu complet.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from dvf_lakehouse import config
from dvf_lakehouse.duckdb_pipeline.connection import lire_parquet

TOLERANCE_RELATIVE = 1e-9
TYPES_FLOTTANTS = {"DOUBLE", "FLOAT"}


@dataclass
class Ecart:
    table: str
    ok: bool
    lignes: int
    details: list[str] = field(default_factory=list)


def _source(chemin: Path) -> str:
    partitionne = any(p.name.startswith("annee=") for p in chemin.iterdir())
    return lire_parquet(chemin, {"annee": "INTEGER"} if partitionne else None)


def comparer_sources(con: duckdb.DuckDBPyConnection, source_a: str, source_b: str, nom: str,
                     cles: tuple[str, ...], rtol: float = TOLERANCE_RELATIVE) -> Ecart:
    """Compare deux relations (expressions FROM) ; `a` est Spark, `b` DuckDB par convention."""
    con.execute(f"CREATE OR REPLACE TEMP VIEW cmp_a AS SELECT * FROM {source_a}")
    con.execute(f"CREATE OR REPLACE TEMP VIEW cmp_b AS SELECT * FROM {source_b}")
    types_a = {r[0]: r[1] for r in con.execute("DESCRIBE cmp_a").fetchall()}
    types_b = {r[0]: r[1] for r in con.execute("DESCRIBE cmp_b").fetchall()}
    n_a = con.execute("SELECT count(*) FROM cmp_a").fetchone()[0]
    n_b = con.execute("SELECT count(*) FROM cmp_b").fetchone()[0]
    details = []
    if set(types_a) != set(types_b):
        details.append(f"colonnes différentes : {sorted(set(types_a) ^ set(types_b))}")
        return Ecart(nom, False, n_a, details)
    details += [f"type de {c} : {types_a[c]} / {types_b[c]}" for c in types_a if types_a[c] != types_b[c]]
    if n_a != n_b:
        details.append(f"nombre de lignes : {n_a} / {n_b}")

    jointure = " AND ".join(f"a.{c} IS NOT DISTINCT FROM b.{c}" for c in cles)
    seul_a = con.execute(f"SELECT count(*) FROM cmp_a a ANTI JOIN cmp_b b ON {jointure}").fetchone()[0]
    seul_b = con.execute(f"SELECT count(*) FROM cmp_b b ANTI JOIN cmp_a a ON {jointure}").fetchone()[0]
    doublons = con.execute(
        f"SELECT count(*) - count(DISTINCT ({', '.join(cles)})) FROM cmp_a").fetchone()[0]
    if seul_a or seul_b or doublons:
        details.append(f"clés : {seul_a} seulement Spark, {seul_b} seulement DuckDB, {doublons} doublons")

    autres = [c for c in types_a if c not in cles]
    tests = []
    for c in autres:
        if types_a[c] in TYPES_FLOTTANTS:
            tests.append(f"count(*) FILTER (WHERE (a.{c} IS NULL) <> (b.{c} IS NULL) OR "
                         f"abs(a.{c} - b.{c}) > {rtol} * abs(b.{c})) AS \"{c}\"")
            tests.append(f"max(abs(a.{c} - b.{c}) / nullif(abs(b.{c}), 0)) AS \"{c}__ecart\"")
        else:
            tests.append(f"count(*) FILTER (WHERE a.{c} IS DISTINCT FROM b.{c}) AS \"{c}\"")
    if tests:
        ligne = con.execute(
            f"SELECT {', '.join(tests)} FROM cmp_a a JOIN cmp_b b ON {jointure}").fetchone()
        valeurs = dict(zip([t.rsplit(' AS ', 1)[1].strip('"') for t in tests], ligne))
        ecarts_max = {}
        for c in autres:
            if valeurs.get(c):
                suffixe = ""
                if f"{c}__ecart" in valeurs:
                    suffixe = f" (écart relatif max {valeurs[f'{c}__ecart']:.1e})"
                details.append(f"{c} : {valeurs[c]} valeurs différentes{suffixe}")
            if f"{c}__ecart" in valeurs and valeurs[f"{c}__ecart"] is not None:
                ecarts_max[c] = valeurs[f"{c}__ecart"]
        if ecarts_max and not details:
            pire = max(ecarts_max, key=ecarts_max.get)
            details.append(f"écart relatif max {ecarts_max[pire]:.1e} ({pire})")
            return Ecart(nom, True, n_a, details)
    return Ecart(nom, not details, n_a, details)


def comparer(lake_dir: Path | None = None, rtol: float = TOLERANCE_RELATIVE) -> list[Ecart]:
    spark, duck = config.chemins("spark", lake_dir), config.chemins("duckdb", lake_dir)
    tables = {f"gold.{t}": (spark.gold(t), duck.gold(t), config.GOLD_CLES[t]) for t in config.GOLD_TABLES}
    tables["silver.ventes"] = (spark.silver_ventes, duck.silver_ventes, ("id_mutation",))
    tables["silver.mutations"] = (spark.silver_mutations, duck.silver_mutations, ("id_mutation",))
    con = duckdb.connect()
    con.execute(f"SET threads = {config.DUCKDB_THREADS}; SET memory_limit = '{config.DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET temp_directory = '{config.TMP_DIR / 'duckdb'}'")
    ecarts = []
    for nom, (chemin_spark, chemin_duck, cles) in tables.items():
        if not (chemin_spark.exists() and chemin_duck.exists()):
            ecarts.append(Ecart(nom, False, 0, ["table absente pour l'un des moteurs"]))
            continue
        ecarts.append(comparer_sources(con, _source(chemin_spark), _source(chemin_duck), nom, cles, rtol))
    con.close()
    return ecarts


def _n(x: int) -> str:
    return f"{x:,}".replace(",", " ")


def rapport_markdown(ecarts: list[Ecart], rtol: float = TOLERANCE_RELATIVE) -> str:
    lignes = [
        "# Parité Spark / DuckDB",
        "",
        "Sorties des deux moteurs sur le jeu complet, jointes sur leur clé. Types de colonnes, "
        f"entiers et chaînes : égalité stricte. Flottants : tolérance relative {rtol:g}.",
        "",
        "| Table | Lignes | Résultat |",
        "|---|---:|---|",
    ]
    for e in ecarts:
        verdict = ("identique" if e.ok else "ÉCART") + (f" ; {'; '.join(e.details)}" if e.details else "")
        lignes.append(f"| {e.table} | {_n(e.lignes)} | {verdict} |")
    return "\n".join(lignes) + "\n"


def main() -> int:
    ecarts = comparer()
    for e in ecarts:
        print(f"{'OK   ' if e.ok else 'ECART'} {e.table:<28} {e.lignes:>10} lignes  {'; '.join(e.details)}")
    config.DOCS_DIR.mkdir(exist_ok=True)
    (config.DOCS_DIR / "parite.md").write_text(rapport_markdown(ecarts))
    return 0 if all(e.ok for e in ecarts) else 1


if __name__ == "__main__":
    sys.exit(main())
