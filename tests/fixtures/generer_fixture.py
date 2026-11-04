"""Génère tests/fixtures/dvf_fixture.csv.gz : un petit DVF synthétique.

Les lignes ordinaires sont tirées au hasard (graine fixe) autour d'un prix de base
par commune. Les mutations dont l'identifiant contient "-T-" sont des cas écrits
à la main pour les tests ; ATTENDUS donne le motif d'exclusion attendu (None = vente gardée).

    python tests/fixtures/generer_fixture.py
"""

from __future__ import annotations

import csv
import gzip
import io
import random
from datetime import date, timedelta
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from dvf_lakehouse.schema import NOMS  # noqa: E402

SORTIE = Path(__file__).with_name("dvf_fixture.csv.gz")

# code -> (nom, département, code postal, lat, lon, prix appart, prix maison, nb apparts, nb maisons)
COMMUNES = {
    "33063": ("Bordeaux", "33", "33000", 44.8378, -0.5792, 4600, 5000, 60, 40),
    "33281": ("Mérignac", "33", "33700", 44.8386, -0.6436, 3900, 4200, 35, 35),
    "33318": ("Pessac", "33", "33600", 44.8067, -0.6311, 3600, 3900, 12, 12),
    "75111": ("Paris 11e Arrondissement", "75", "75011", 48.8590, 2.3800, 10500, 0, 40, 0),
    "23096": ("Guéret", "23", "23000", 46.1716, 1.8717, 0, 1300, 0, 12),
}

ATTENDUS: dict[str, str | None] = {
    "2024-T-MAISON-TERRAINS": None,
    "2024-T-APPART-DEP": None,
    "2024-T-MAISON-DOUBLON": None,
    "2024-T-DEUX-APPARTS": "plusieurs_logements",
    "2024-T-DEUX-APPARTS-IDENTIQUES": "plusieurs_logements",
    "2024-T-MIXTE": "local_activite",
    "2024-T-ECHANGE": "nature_mutation",
    "2024-T-VEFA": "nature_mutation",
    "2024-T-SANS-VALEUR": "valeur_absente",
    "2024-T-TERRAIN": "sans_logement",
    "2024-T-DEPENDANCE-SEULE": "sans_logement",
    "2024-T-DISPOS": "plusieurs_dispositions",
    "2024-T-VALEURS": "valeurs_multiples",
    "2024-T-SURFACE-NULLE": "surface_absente",
    "2024-T-STUDIO-5M2": "surface_hors_bornes",
    "2024-T-UN-EURO": "valeur_hors_bornes",
    "2024-T-HORS-BORNES": "prix_m2_hors_bornes",
    "2024-T-CHER": "prix_aberrant",
    "2024-T-BRADE": "prix_aberrant",
    "2024-T-PESSAC": None,
    "2024-T-GUERET": None,
}

DEBUT, FIN = date(2021, 7, 1), date(2026, 6, 30)


class Generateur:
    def __init__(self, graine: int = 2026) -> None:
        self.rng = random.Random(graine)
        self.lignes: list[dict] = []
        self.compteur = {}
        self.parcelle = 0

    def nouvel_id(self, annee: int) -> str:
        self.compteur[annee] = self.compteur.get(annee, 0) + 1
        return f"{annee}-{self.compteur[annee]}"

    def nouvelle_parcelle(self, commune: str) -> str:
        self.parcelle += 1
        return f"{commune}000AB{self.parcelle:04d}"

    def ligne(self, id_mutation: str, jour: date, commune: str, valeur, **champs) -> None:
        nom, dep, cp, lat, lon, *_ = COMMUNES[commune]
        base = {
            "id_mutation": id_mutation,
            "date_mutation": jour.isoformat(),
            "numero_disposition": "000001",
            "nature_mutation": "Vente",
            "valeur_fonciere": "" if valeur is None else f"{valeur:.2f}",
            "adresse_numero": str(self.rng.randint(1, 120)),
            "adresse_nom_voie": "RUE DES TESTS",
            "adresse_code_voie": "0001",
            "code_postal": cp,
            "code_commune": commune,
            "nom_commune": nom,
            "code_departement": dep,
            "nombre_lots": "0",
            "longitude": f"{lon + self.rng.uniform(-0.02, 0.02):.6f}",
            "latitude": f"{lat + self.rng.uniform(-0.015, 0.015):.6f}",
        }
        base.update({k: ("" if v is None else str(v)) for k, v in champs.items()})
        self.lignes.append(base)

    # --- briques ---------------------------------------------------------------
    def appartement(self, idm, jour, commune, valeur, surface, lot="1", parcelle=None, **kw):
        parcelle = parcelle or self.nouvelle_parcelle(commune)
        self.ligne(idm, jour, commune, valeur, id_parcelle=parcelle, lot1_numero=lot,
                   lot1_surface_carrez=f"{surface * 0.97:.2f}", nombre_lots="1", code_type_local=2,
                   type_local="Appartement", surface_reelle_bati=surface,
                   nombre_pieces_principales=max(1, surface // 22), **kw)
        return parcelle

    def dependance(self, idm, jour, commune, valeur, parcelle, lot="200", **kw):
        self.ligne(idm, jour, commune, valeur, id_parcelle=parcelle, lot1_numero=lot, nombre_lots="1",
                   code_type_local=3, type_local="Dépendance", nombre_pieces_principales=0, **kw)

    def maison(self, idm, jour, commune, valeur, surface, parcelle=None, culture=("S", "sols"), **kw):
        parcelle = parcelle or self.nouvelle_parcelle(commune)
        self.ligne(idm, jour, commune, valeur, id_parcelle=parcelle, code_type_local=1, type_local="Maison",
                   surface_reelle_bati=surface, nombre_pieces_principales=max(1, surface // 25),
                   code_nature_culture=culture[0], nature_culture=culture[1],
                   surface_terrain=self.rng.randint(200, 900), **kw)
        return parcelle

    def terrain(self, idm, jour, commune, valeur, culture=("T", "terres"), **kw):
        self.ligne(idm, jour, commune, valeur, id_parcelle=self.nouvelle_parcelle(commune),
                   code_nature_culture=culture[0], nature_culture=culture[1],
                   surface_terrain=self.rng.randint(300, 3000), **kw)

    # --- ventes ordinaires -----------------------------------------------------
    def jour_au_hasard(self) -> date:
        return DEBUT + timedelta(days=self.rng.randint(0, (FIN - DEBUT).days))

    def ventes_ordinaires(self) -> None:
        for commune, (*_, prix_app, prix_maison, n_app, n_maison) in COMMUNES.items():
            for _ in range(n_app):
                jour = self.jour_au_hasard()
                idm = self.nouvel_id(jour.year)
                surface = self.rng.randint(18, 110)
                derive = 1 + 0.015 * (jour.year - 2021)
                valeur = round(prix_app * derive * surface * self.rng.lognormvariate(0, 0.15), -2)
                parcelle = self.appartement(idm, jour, commune, valeur, surface, lot=str(self.rng.randint(1, 80)))
                if self.rng.random() < 0.4:
                    self.dependance(idm, jour, commune, valeur, parcelle, lot=str(self.rng.randint(100, 300)))
            for _ in range(n_maison):
                jour = self.jour_au_hasard()
                idm = self.nouvel_id(jour.year)
                surface = self.rng.randint(60, 180)
                derive = 1 + 0.015 * (jour.year - 2021)
                valeur = round(prix_maison * derive * surface * self.rng.lognormvariate(0, 0.15), -2)
                parcelle = self.maison(idm, jour, commune, valeur, surface)
                if self.rng.random() < 0.3:  # même maison, seconde nature de culture
                    self.maison(idm, jour, commune, valeur, surface, parcelle=parcelle,
                                culture=("J", "jardins"))
                if self.rng.random() < 0.3:
                    self.terrain(idm, jour, commune, valeur)

    # --- cas écrits à la main --------------------------------------------------
    def cas_particuliers(self) -> None:
        j = date(2024, 5, 14)
        bx = "33063"
        self.maison("2024-T-MAISON-TERRAINS", j, bx, 495000, 100)
        self.terrain("2024-T-MAISON-TERRAINS", j, bx, 495000)
        self.terrain("2024-T-MAISON-TERRAINS", j, bx, 495000, culture=("J", "jardins"))

        p = self.appartement("2024-T-APPART-DEP", j, bx, 230000, 50, lot="12",
                             adresse_nom_voie="RTE D'AIGRE , LES \"CHATELETS\"")
        self.dependance("2024-T-APPART-DEP", j, bx, 230000, p, lot="120")

        p = self.maison("2024-T-MAISON-DOUBLON", j, bx, 600000, 120)
        self.maison("2024-T-MAISON-DOUBLON", j, bx, 600000, 120, parcelle=p, culture=("J", "jardins"))

        p = self.appartement("2024-T-DEUX-APPARTS", j, bx, 380000, 40, lot="12")
        self.appartement("2024-T-DEUX-APPARTS", j, bx, 380000, 45, lot="13", parcelle=p)

        p = self.appartement("2024-T-DEUX-APPARTS-IDENTIQUES", j, bx, 360000, 40, lot="21")
        self.appartement("2024-T-DEUX-APPARTS-IDENTIQUES", j, bx, 360000, 40, lot="22", parcelle=p)

        p = self.maison("2024-T-MIXTE", j, bx, 700000, 110)
        self.ligne("2024-T-MIXTE", j, bx, 700000, id_parcelle=p, code_type_local=4,
                   type_local="Local industriel. commercial ou assimilé", surface_reelle_bati=60,
                   nombre_pieces_principales=0)

        self.maison("2024-T-ECHANGE", j, bx, 500000, 100, nature_mutation="Echange")
        self.appartement("2024-T-VEFA", j, bx, 250000, 50,
                         nature_mutation="Vente en l'état futur d'achèvement")
        self.appartement("2024-T-SANS-VALEUR", j, bx, None, 50)
        self.terrain("2024-T-TERRAIN", j, bx, 80000)
        self.dependance("2024-T-DEPENDANCE-SEULE", j, bx, 25000, self.nouvelle_parcelle(bx))

        self.appartement("2024-T-DISPOS", j, bx, 200000, 45)
        self.appartement("2024-T-DISPOS", j, bx, 150000, 35, lot="2", numero_disposition="000002")

        p = self.maison("2024-T-VALEURS", j, bx, 450000, 90)
        self.terrain("2024-T-VALEURS", j, bx, 30000)

        self.maison("2024-T-SURFACE-NULLE", j, bx, 300000, 0)
        self.appartement("2024-T-STUDIO-5M2", j, bx, 40000, 5)
        self.maison("2024-T-UN-EURO", j, bx, 1, 90)
        self.appartement("2024-T-HORS-BORNES", j, bx, 900000, 20)
        self.appartement("2024-T-CHER", j, bx, 1000000, 40)
        self.maison("2024-T-BRADE", j, bx, 60000, 150)
        self.appartement("2024-T-PESSAC", j, "33318", 190000, 52)
        self.maison("2024-T-GUERET", j, "23096", 120000, 95)

    def csv(self) -> str:
        tampon = io.StringIO()
        ecrivain = csv.DictWriter(tampon, fieldnames=NOMS, lineterminator="\n", restval="")
        ecrivain.writeheader()
        ecrivain.writerows(self.lignes)
        return tampon.getvalue()


def contenu() -> str:
    """Texte CSV de la fixture (déterministe)."""
    g = Generateur()
    g.ventes_ordinaires()
    g.cas_particuliers()
    return g.csv()


def main() -> None:
    texte = contenu()
    # mtime=0 : le fichier généré ne change pas d'un lancement à l'autre.
    with open(SORTIE, "wb") as brut, gzip.GzipFile(filename="", fileobj=brut, mode="wb", mtime=0) as gz:
        gz.write(texte.encode("utf-8"))
    print(f"{SORTIE} : {texte.count(chr(10)) - 1} lignes, {SORTIE.stat().st_size} octets")


if __name__ == "__main__":
    main()
