"""Gold Spark : agrégats de prix au m² et de volumes."""

from __future__ import annotations

import shutil
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dvf_lakehouse.config import (
    ANNEES_PARTIELLES, BORDEAUX_METROPOLE, CODE_BORDEAUX, GOLD_CLES, GOLD_EFFECTIF_MIN, Chemins,
)
from dvf_lakehouse.spark_pipeline.silver import lire_parquet


def _stats_prix() -> list[Column]:
    return [
        F.count(F.lit(1)).alias("nb_ventes"),
        F.percentile("prix_m2", 0.5).alias("prix_m2_median"),
        F.percentile("prix_m2", 0.25).alias("prix_m2_q1"),
        F.percentile("prix_m2", 0.75).alias("prix_m2_q3"),
    ]


def _annee_complete() -> Column:
    return ~F.col("annee").isin(*ANNEES_PARTIELLES)


def _evolutions(df: DataFrame, partition: list[str]) -> DataFrame:
    """Évolution en % par rapport à l'année précédente, si elle existe dans la table."""
    w = Window.partitionBy(*partition).orderBy("annee")
    suivie = F.lag("annee").over(w) == F.col("annee") - 1
    deux_completes = F.lag("annee_complete").over(w) & F.col("annee_complete")
    return df.withColumns({
        "evolution_prix_m2_pct": F.when(
            suivie, 100 * (F.col("prix_m2_median") / F.lag("prix_m2_median").over(w) - 1)),
        "evolution_nb_ventes_pct": F.when(
            suivie & deux_completes, 100 * (F.col("nb_ventes") / F.lag("nb_ventes").over(w) - 1)),
    })


def prix_commune_annee(ventes: DataFrame) -> DataFrame:
    agg = (
        ventes.groupBy("code_commune", "annee", "type_local")
        .agg(
            F.max("code_departement").alias("code_departement"),
            F.max("nom_commune").alias("nom_commune"),
            *_stats_prix(),
            F.percentile("surface_reelle_bati", 0.5).alias("surface_mediane"),
            F.percentile("valeur_fonciere", 0.5).alias("valeur_mediane"),
        )
        .filter(F.col("nb_ventes") >= GOLD_EFFECTIF_MIN)
        .withColumn("annee_complete", _annee_complete())
    )
    return _evolutions(agg, ["code_commune", "type_local"]).select(
        "code_departement", "code_commune", "nom_commune", "annee", "annee_complete", "type_local",
        "nb_ventes", "prix_m2_median", "prix_m2_q1", "prix_m2_q3", "surface_mediane",
        "valeur_mediane", "evolution_prix_m2_pct", "evolution_nb_ventes_pct",
    )


def marche_france_mois(ventes: DataFrame) -> DataFrame:
    return (
        ventes.groupBy("annee", "mois", "type_local")
        .agg(F.count(F.lit(1)).alias("nb_ventes"), F.percentile("prix_m2", 0.5).alias("prix_m2_median"))
        .select("annee", "mois", "type_local", "nb_ventes", "prix_m2_median")
    )


def _perimetres(ventes: DataFrame) -> list[tuple[str, DataFrame]]:
    return [
        ("France", ventes),
        ("Bordeaux Métropole", ventes.filter(F.col("code_commune").isin(*BORDEAUX_METROPOLE))),
        ("Bordeaux", ventes.filter(F.col("code_commune") == CODE_BORDEAUX)),
    ]


def marche_annee(ventes: DataFrame) -> DataFrame:
    parts = [
        df.groupBy("annee", "type_local").agg(*_stats_prix()).withColumn("perimetre", F.lit(nom))
        for nom, df in _perimetres(ventes)
    ]
    agg = reduce(DataFrame.unionByName, parts).withColumn("annee_complete", _annee_complete())
    return _evolutions(agg, ["perimetre", "type_local"]).select(
        "perimetre", "annee", "annee_complete", "type_local", "nb_ventes", "prix_m2_median",
        "prix_m2_q1", "prix_m2_q3", "evolution_prix_m2_pct", "evolution_nb_ventes_pct",
    )


def marche_semestre(ventes: DataFrame) -> DataFrame:
    parts = [
        df.groupBy("semestre", "type_local").agg(*_stats_prix()).withColumn("perimetre", F.lit(nom))
        for nom, df in _perimetres(ventes)
    ]
    return reduce(DataFrame.unionByName, parts).select(
        "perimetre", "semestre", "type_local", "nb_ventes", "prix_m2_median", "prix_m2_q1", "prix_m2_q3")


def bordeaux_metropole(communes: DataFrame) -> DataFrame:
    w = Window.partitionBy("annee", "type_local").orderBy(F.col("prix_m2_median").desc())
    return (
        communes.filter(F.col("code_commune").isin(*BORDEAUX_METROPOLE))
        .withColumn("rang_prix", F.rank().over(w))
        .select("code_commune", "nom_commune", "annee", "annee_complete", "type_local", "nb_ventes",
                "prix_m2_median", "prix_m2_q1", "prix_m2_q3", "surface_mediane", "valeur_mediane",
                "evolution_prix_m2_pct", "evolution_nb_ventes_pct", "rang_prix")
    )


def construire(spark: SparkSession, chemins: Chemins) -> dict:
    ventes = lire_parquet(spark, chemins.silver_ventes)
    communes = prix_commune_annee(ventes)
    tables = {
        "prix_commune_annee": communes,
        "marche_france_mois": marche_france_mois(ventes),
        "marche_annee": marche_annee(ventes),
        "marche_semestre": marche_semestre(ventes),
    }
    stats = {}
    for nom, df in tables.items():
        stats[nom] = _ecrire(spark, df, chemins, nom)
    # Le zoom métropole repart de la table commune déjà écrite.
    stats["bordeaux_metropole"] = _ecrire(
        spark, bordeaux_metropole(spark.read.parquet(str(chemins.gold("prix_commune_annee")))),
        chemins, "bordeaux_metropole")
    return stats


def _ecrire(spark: SparkSession, df: DataFrame, chemins: Chemins, nom: str) -> int:
    chemin = chemins.gold(nom)
    shutil.rmtree(chemin, ignore_errors=True)
    # Un seul fichier trié par la clé de la table.
    df.repartition(1).sortWithinPartitions(*GOLD_CLES[nom]).write.mode("overwrite").parquet(str(chemin))
    return spark.read.parquet(str(chemin)).count()
