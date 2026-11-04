"""Silver Spark : une ligne par mutation, puis une ligne par vente d'un logement unique.

Étapes :
1. regrouper les lignes DVF par id_mutation (la valeur foncière y est répétée) ;
2. compter les logements distincts, dépendances et locaux d'activité ;
3. attribuer un motif d'exclusion (premier motif applicable, voir config.MOTIFS) ;
4. filtre robuste sur ln(prix au m²) : écart à la médiane rapporté au MAD,
   par commune x type, ou par département x type si la commune a trop peu de ventes.
"""

from __future__ import annotations

import shutil

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from dvf_lakehouse.config import REGLES, Chemins
from dvf_lakehouse.schema import spark_schema_bronze

LOGEMENT = F.col("code_type_local").isin(1, 2)


def _cle_local() -> Column:
    """Identifie un local : la même maison peut apparaître sur plusieurs lignes
    (une par nature de culture du terrain), mais garde parcelle, surface, pièces et lot."""
    parties = ["id_parcelle", "code_type_local", "surface_reelle_bati", "nombre_pieces_principales", "lot1_numero"]
    return F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in parties])


def _nb_distincts(valeur: Column) -> Column:
    # size(collect_set) = count(DISTINCT) sans l'opérateur Expand de Spark,
    # coûteux quand plusieurs comptages distincts sont dans la même agrégation.
    return F.size(F.collect_set(valeur)).cast("long")


def _si_logement(colonne: str) -> Column:
    return F.max(F.when(LOGEMENT, F.col(colonne)))


def reconstruire_mutations(lignes: DataFrame) -> DataFrame:
    lignes = lignes.withColumn("cle_local", _cle_local())
    mutations = lignes.groupBy("id_mutation").agg(
        F.min("date_mutation").alias("date_mutation"),
        F.min("nature_mutation").alias("nature_mutation"),
        F.count(F.lit(1)).alias("nb_lignes"),
        _nb_distincts(F.col("numero_disposition")).alias("nb_dispositions"),
        _nb_distincts(F.col("valeur_fonciere")).alias("nb_valeurs"),
        F.max("valeur_fonciere").alias("valeur_fonciere"),
        _nb_distincts(F.col("id_parcelle")).alias("nb_parcelles"),
        _nb_distincts(F.when(LOGEMENT, F.col("cle_local"))).alias("nb_logements"),
        _nb_distincts(F.when(F.col("code_type_local") == 3, F.col("cle_local"))).alias("nb_dependances"),
        _nb_distincts(F.when(F.col("code_type_local") == 4, F.col("cle_local"))).alias("nb_locaux_activite"),
        _si_logement("type_local").alias("type_local"),
        _si_logement("surface_reelle_bati").alias("surface_reelle_bati"),
        _si_logement("nombre_pieces_principales").alias("nombre_pieces_principales"),
        F.max("code_departement").alias("code_departement"),
        F.coalesce(_si_logement("code_commune"), F.max("code_commune")).alias("code_commune"),
        F.coalesce(_si_logement("nom_commune"), F.max("nom_commune")).alias("nom_commune"),
        F.coalesce(_si_logement("code_postal"), F.max("code_postal")).alias("code_postal"),
        _si_logement("longitude").alias("longitude"),
        _si_logement("latitude").alias("latitude"),
    )
    surface_ok = F.col("surface_reelle_bati") > 0
    return mutations.withColumns({
        "annee": F.year("date_mutation"),
        "prix_m2": F.when(surface_ok & F.col("valeur_fonciere").isNotNull(),
                          F.col("valeur_fonciere").cast("double") / F.col("surface_reelle_bati")),
    })


def motif_avant_mad(r=REGLES) -> Column:
    c = F.col
    return (
        F.when(c("nature_mutation") != r.nature_mutation, "nature_mutation")
        .when(c("nb_dispositions") > 1, "plusieurs_dispositions")
        .when(c("nb_valeurs") == 0, "valeur_absente")
        .when(c("nb_valeurs") > 1, "valeurs_multiples")
        .when(c("nb_locaux_activite") > 0, "local_activite")
        .when(c("nb_logements") == 0, "sans_logement")
        .when(c("nb_logements") > 1, "plusieurs_logements")
        .when(c("surface_reelle_bati").isNull() | (c("surface_reelle_bati") == 0), "surface_absente")
        .when((c("surface_reelle_bati") < r.surface_min) | (c("surface_reelle_bati") > r.surface_max),
              "surface_hors_bornes")
        .when(c("valeur_fonciere") < r.valeur_min, "valeur_hors_bornes")
        .when((c("prix_m2") < r.prix_m2_min) | (c("prix_m2") > r.prix_m2_max), "prix_m2_hors_bornes")
    )


def _reference(candidats: DataFrame, niveau: str, suffixe: str) -> DataFrame:
    cles = [niveau, "type_local"]
    mediane = candidats.groupBy(*cles).agg(
        F.count(F.lit(1)).alias("n"), F.percentile("ln_prix", 0.5).alias("med"))
    mad = (candidats.join(mediane, cles)
           .groupBy(*cles)
           .agg(F.percentile(F.abs(F.col("ln_prix") - F.col("med")), 0.5).alias("mad")))
    return (mediane.join(mad, cles)
            .select(*cles, *[F.col(c).alias(f"{c}_{suffixe}") for c in ("n", "med", "mad")]))


def scorer_mad(candidats: DataFrame, r=REGLES) -> DataFrame:
    """Score robuste |ln(prix) - médiane| / (1,4826 x MAD) avec repli sur le département."""
    candidats = candidats.select(
        "id_mutation", "code_commune", "code_departement", "type_local",
        F.log("prix_m2").alias("ln_prix"))
    avec_ref = (candidats
                .join(_reference(candidats, "code_commune", "c"), ["code_commune", "type_local"])
                .join(_reference(candidats, "code_departement", "d"), ["code_departement", "type_local"]))
    commune_ok = (F.col("n_c") >= r.mad_effectif_min) & (F.col("mad_c") > 0)
    dep_ok = (F.col("n_d") >= r.mad_effectif_min) & (F.col("mad_d") > 0)
    med = F.when(commune_ok, F.col("med_c")).when(dep_ok, F.col("med_d"))
    mad = F.when(commune_ok, F.col("mad_c")).when(dep_ok, F.col("mad_d"))
    return avec_ref.select(
        "id_mutation",
        F.when(commune_ok, "commune").when(dep_ok, "departement").alias("reference_mad"),
        (F.abs(F.col("ln_prix") - med) / (F.lit(r.mad_facteur) * mad)).alias("score_mad"),
    )


COLONNES_MUTATIONS = [
    "id_mutation", "date_mutation", "annee", "nature_mutation", "nb_lignes", "nb_dispositions",
    "nb_valeurs", "valeur_fonciere", "nb_parcelles", "nb_logements", "nb_dependances",
    "nb_locaux_activite", "type_local", "surface_reelle_bati", "nombre_pieces_principales",
    "code_departement", "code_commune", "nom_commune", "code_postal", "longitude", "latitude",
    "prix_m2", "reference_mad", "score_mad", "motif_exclusion",
]

COLONNES_VENTES = [
    "id_mutation", "date_mutation", "annee", "mois", "semestre", "code_departement", "code_commune",
    "nom_commune", "code_postal", "type_local", "valeur_fonciere", "surface_reelle_bati",
    "nombre_pieces_principales", "nb_dependances", "nb_parcelles", "prix_m2", "longitude", "latitude",
    "reference_mad", "score_mad",
]


def lire_bronze(spark: SparkSession, chemins: Chemins) -> DataFrame:
    return spark.read.schema(spark_schema_bronze()).parquet(str(chemins.bronze))


def lire_parquet(spark: SparkSession, chemin) -> DataFrame:
    df = spark.read.parquet(str(chemin))
    return df.withColumn("annee", F.col("annee").cast("int")) if "annee" in df.columns else df


def construire(spark: SparkSession, chemins: Chemins, r=REGLES) -> dict:
    mutations = reconstruire_mutations(lire_bronze(spark, chemins))
    mutations = mutations.withColumn("motif_pre", motif_avant_mad(r))
    mutations.persist(StorageLevel.MEMORY_AND_DISK)

    scores = scorer_mad(mutations.filter(F.col("motif_pre").isNull()), r)
    finales = (
        mutations.join(scores, "id_mutation", "left")
        .withColumn("motif_exclusion", F.coalesce(
            F.col("motif_pre"),
            F.when(F.col("score_mad") > r.mad_seuil, F.lit("prix_aberrant"))))
        .select(*COLONNES_MUTATIONS)
    )
    for chemin in (chemins.silver_mutations, chemins.silver_ventes):
        shutil.rmtree(chemin, ignore_errors=True)
    finales.write.mode("overwrite").partitionBy("annee").parquet(str(chemins.silver_mutations))
    mutations.unpersist()

    ventes = (
        lire_parquet(spark, chemins.silver_mutations)
        .filter(F.col("motif_exclusion").isNull())
        .withColumns({
            "mois": F.month("date_mutation"),
            "semestre": F.concat(F.col("annee").cast("string"), F.lit("-S"),
                                 F.when(F.month("date_mutation") <= 6, "1").otherwise("2")),
            "valeur_fonciere": F.col("valeur_fonciere").cast("double"),
        })
        .select(*COLONNES_VENTES)
    )
    ventes.write.mode("overwrite").partitionBy("annee").parquet(str(chemins.silver_ventes))
    return {
        "mutations": lire_parquet(spark, chemins.silver_mutations).count(),
        "ventes": lire_parquet(spark, chemins.silver_ventes).count(),
    }
