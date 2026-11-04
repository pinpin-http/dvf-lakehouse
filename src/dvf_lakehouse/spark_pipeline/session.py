"""Création de la session Spark locale, bridée pour une machine partagée."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

from dvf_lakehouse import config

# Spark 4.2 tourne sur Java 17 et 21. Java 27 échoue au démarrage (Platform.<clinit>).
JAVA_SUPPORTES = (17, 21)


def _version_java(executable: str) -> int | None:
    try:
        sortie = subprocess.run([executable, "-version"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    m = re.search(r'version "(\d+)', sortie.stderr + sortie.stdout)
    return int(m.group(1)) if m else None


def choisir_java() -> None:
    """Garantit un JAVA_HOME compatible avec Spark.

    JAVA_HOME est respecté s'il est défini. Sinon, si le `java` du PATH n'est pas
    supporté, on cherche un JDK 21 ou 17 dans /usr/lib/jvm.
    """
    if os.environ.get("JAVA_HOME"):
        version = _version_java(str(Path(os.environ["JAVA_HOME"]) / "bin" / "java"))
        if version not in JAVA_SUPPORTES:
            raise RuntimeError(f"JAVA_HOME pointe vers Java {version}, Spark attend {JAVA_SUPPORTES}")
        return
    if _version_java("java") in JAVA_SUPPORTES:
        return
    racine = Path("/usr/lib/jvm")
    for version in sorted(JAVA_SUPPORTES, reverse=True):
        for jdk in sorted(racine.glob(f"*{version}*")) if racine.exists() else []:
            if _version_java(str(jdk / "bin" / "java")) == version:
                os.environ["JAVA_HOME"] = str(jdk)
                return
    raise RuntimeError(f"aucun JDK {JAVA_SUPPORTES} trouvé : définir JAVA_HOME")


def creer_session(master: str | None = None, shuffle_partitions: int | None = None,
                  driver_memory: str | None = None):
    from pyspark.sql import SparkSession

    choisir_java()
    tmp = config.TMP_DIR / "spark"
    tmp.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.master(master or config.SPARK_MASTER)
        .appName("dvf-lakehouse")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", driver_memory or config.SPARK_DRIVER_MEMORY)
        .config("spark.local.dir", str(tmp))
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={tmp}")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions or config.SPARK_SHUFFLE_PARTITIONS))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "zstd")
        # "01" doit rester une chaîne quand on relit les partitions.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
