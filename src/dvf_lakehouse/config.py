"""Paramètres partagés par les deux moteurs : chemins, sources, seuils, ressources."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = Path(os.environ.get("DVF_DATA_DIR", ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"
LAKE_DIR = Path(os.environ.get("DVF_LAKE_DIR", ROOT / "lake"))
DOCS_DIR = ROOT / "docs"
# Fichiers temporaires des moteurs (spill Spark et DuckDB), hors de /tmp.
TMP_DIR = Path(os.environ.get("DVF_TMP_DIR", ROOT / ".tmp"))

ENGINES = ("spark", "duckdb")

USER_AGENT = "dvf-lakehouse/0.1"


@dataclass(frozen=True)
class Source:
    """Un fichier annuel de la publication géo-DVF d'Etalab."""

    periode: str  # libellé de la période couverte
    url: str
    sha1: str
    taille: int

    @property
    def nom_fichier(self) -> str:
        return f"dvf_{self.periode}.csv.gz"


# Publication géo-DVF d'octobre 2026 (données DGFiP de juillet 2021 à juin 2026).
# Jeu de données : https://www.data.gouv.fr/datasets/demandes-de-valeurs-foncieres-geolocalisees
# Les sommes SHA-1 et les tailles viennent de l'API data.gouv.fr. Quand Etalab publie
# une nouvelle livraison, `python -m dvf_lakehouse.download --lister` affiche les
# ressources du moment pour mettre cette liste à jour.
_BASE = "https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres-geolocalisees"
SOURCES: tuple[Source, ...] = (
    Source("2021-S2", f"{_BASE}/20261020-113024/full-2021-s2.csv.gz",
           "077a1f07b802c32bb9d490b86f2496b1cec12950", 75_478_922),
    Source("2022", f"{_BASE}/20261020-114218/full-2022.csv.gz",
           "a6e76cd85bf0bf09e52b0e72e0eca0c851925a5c", 141_270_325),
    Source("2023", f"{_BASE}/20261020-114336/full-2023.csv.gz",
           "1fdaf0d76d52c4ca4b6e18fea382cab2faa6d06b", 117_378_804),
    Source("2024", f"{_BASE}/20261020-114532/full-2024.csv.gz",
           "02e77911bdaf882bd63333b3032f1c37b4f4dc3f", 107_356_846),
    Source("2025", f"{_BASE}/20261020-114721/full-2025.csv.gz",
           "3f01863b4913bf2d45e5331959e935e1a224b107", 116_267_867),
    Source("2026-S1", f"{_BASE}/20261020-114825/full-2026-s1.csv.gz",
           "7cf0773e4df06028ab22a569425402739974d6a3", 51_711_646),
)
DATASET_API = "https://www.data.gouv.fr/api/1/datasets/demandes-de-valeurs-foncieres-geolocalisees/"

# Années couvertes en partie seulement par la publication.
ANNEES_PARTIELLES = {2021: "juillet à décembre", 2026: "janvier à juin"}


# --- Règles de la couche silver -------------------------------------------------
@dataclass(frozen=True)
class Regles:
    nature_mutation: str = "Vente"
    surface_min: float = 9.0  # surface habitable minimale d'un logement décent (décret 2002-120)
    surface_max: float = 1000.0
    valeur_min: float = 1000.0  # en dessous : cessions symboliques
    prix_m2_min: float = 100.0
    prix_m2_max: float = 30000.0
    # Filtre robuste : |ln(prix) - médiane| / (1,4826 x MAD) <= seuil (Iglewicz et Hoaglin).
    mad_seuil: float = 3.5
    mad_facteur: float = 1.4826
    # Sous ce nombre de ventes, la référence commune x type est trop instable :
    # on prend celle du département x type.
    mad_effectif_min: int = 30


REGLES = Regles()

# Motifs d'exclusion d'une mutation, dans l'ordre où ils sont testés : une mutation
# reçoit le premier motif qui s'applique.
MOTIFS = (
    "nature_mutation",       # pas une vente simple (échange, VEFA, adjudication...)
    "plusieurs_dispositions",
    "valeur_absente",
    "valeurs_multiples",
    "local_activite",        # contient un local industriel ou commercial
    "sans_logement",         # terrain ou dépendance seule
    "plusieurs_logements",
    "surface_absente",
    "surface_hors_bornes",
    "valeur_hors_bornes",
    "prix_m2_hors_bornes",
    "prix_aberrant",         # filtre MAD
)

# En gold, une médiane n'est publiée que si elle repose sur assez de ventes.
GOLD_EFFECTIF_MIN = 5

TYPES_LOGEMENT = ("Appartement", "Maison")

# Les 28 communes de Bordeaux Métropole (codes INSEE).
BORDEAUX_METROPOLE: dict[str, str] = {
    "33003": "Ambarès-et-Lagrave",
    "33004": "Ambès",
    "33013": "Artigues-près-Bordeaux",
    "33032": "Bassens",
    "33039": "Bègles",
    "33056": "Blanquefort",
    "33063": "Bordeaux",
    "33065": "Bouliac",
    "33069": "Le Bouscat",
    "33075": "Bruges",
    "33096": "Carbon-Blanc",
    "33119": "Cenon",
    "33162": "Eysines",
    "33167": "Floirac",
    "33192": "Gradignan",
    "33200": "Le Haillan",
    "33249": "Lormont",
    "33273": "Martignas-sur-Jalle",
    "33281": "Mérignac",
    "33312": "Parempuyre",
    "33318": "Pessac",
    "33376": "Saint-Aubin-de-Médoc",
    "33434": "Saint-Louis-de-Montferrand",
    "33449": "Saint-Médard-en-Jalles",
    "33487": "Saint-Vincent-de-Paul",
    "33519": "Le Taillan-Médoc",
    "33522": "Talence",
    "33550": "Villenave-d'Ornon",
}
CODE_BORDEAUX = "33063"


# --- Ressources machine ----------------------------------------------------------
SPARK_MASTER = os.environ.get("DVF_SPARK_MASTER", "local[2]")
SPARK_DRIVER_MEMORY = os.environ.get("DVF_SPARK_DRIVER_MEMORY", "3g")
SPARK_SHUFFLE_PARTITIONS = int(os.environ.get("DVF_SPARK_SHUFFLE_PARTITIONS", "16"))
DUCKDB_THREADS = int(os.environ.get("DVF_DUCKDB_THREADS", "2"))
DUCKDB_MEMORY_LIMIT = os.environ.get("DVF_DUCKDB_MEMORY_LIMIT", "2.5GB")


@dataclass(frozen=True)
class Chemins:
    """Emplacements des couches d'un moteur dans le lac."""

    racine: Path

    @property
    def bronze(self) -> Path:
        return self.racine / "bronze" / "dvf"

    @property
    def silver_mutations(self) -> Path:
        return self.racine / "silver" / "mutations"

    @property
    def silver_ventes(self) -> Path:
        return self.racine / "silver" / "ventes"

    def gold(self, table: str) -> Path:
        return self.racine / "gold" / table


def chemins(engine: str, lake_dir: Path | None = None) -> Chemins:
    if engine not in ENGINES:
        raise ValueError(f"moteur inconnu : {engine!r} (attendu : {', '.join(ENGINES)})")
    return Chemins((lake_dir or LAKE_DIR) / engine)


# Tables gold et leur clé (sert à trier les fichiers et à comparer les moteurs).
GOLD_CLES: dict[str, tuple[str, ...]] = {
    "prix_commune_annee": ("code_commune", "annee", "type_local"),
    "marche_france_mois": ("annee", "mois", "type_local"),
    "marche_annee": ("perimetre", "annee", "type_local"),
    "marche_semestre": ("perimetre", "semestre", "type_local"),
    "bordeaux_metropole": ("code_commune", "annee", "type_local"),
}
GOLD_TABLES = tuple(GOLD_CLES)
