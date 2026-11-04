"""Bronze Spark : CSV.gz -> Parquet partitionné par année et département."""

from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dvf_lakehouse.config import Chemins
from dvf_lakehouse.schema import PARTITIONS, spark_schema, verifier_entete


def construire(spark: SparkSession, fichiers: list[Path], chemins: Chemins) -> dict:
    for fichier in fichiers:
        verifier_entete(fichier)

    lignes = (
        spark.read.schema(spark_schema())
        .option("header", True)
        .option("enforceSchema", False)  # compare l'en-tête au schéma
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("dateFormat", "yyyy-MM-dd")
        .option("mode", "FAILFAST")  # une valeur non convertible arrête le job
        .csv([str(f) for f in fichiers])
        .withColumn("annee", F.year("date_mutation"))
    )

    shutil.rmtree(chemins.bronze, ignore_errors=True)
    lignes.write.mode("overwrite").partitionBy(*PARTITIONS).parquet(str(chemins.bronze))
    return {"lignes": spark.read.parquet(str(chemins.bronze)).count()}
