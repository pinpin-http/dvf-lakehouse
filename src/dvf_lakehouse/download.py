"""Téléchargement idempotent des fichiers géo-DVF dans data/raw.

Un fichier déjà présent et dont le SHA-1 correspond n'est pas retéléchargé.
Le téléchargement passe par un fichier .part renommé seulement après vérification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import urllib.request
from pathlib import Path

from dvf_lakehouse.config import DATASET_API, RAW_DIR, SOURCES, USER_AGENT, Source


def sha1_fichier(chemin: Path, bloc: int = 1 << 20) -> str:
    h = hashlib.sha1()
    with chemin.open("rb") as f:
        while morceau := f.read(bloc):
            h.update(morceau)
    return h.hexdigest()


def _requete(url: str) -> urllib.request.Request:
    return urllib.request.Request(url, headers={"User-Agent": USER_AGENT})


def telecharger(source: Source, dossier: Path = RAW_DIR) -> Path:
    dossier.mkdir(parents=True, exist_ok=True)
    cible = dossier / source.nom_fichier
    if cible.exists() and cible.stat().st_size == source.taille and sha1_fichier(cible) == source.sha1:
        print(f"[ok]   {cible.name} déjà présent")
        return cible

    partiel = cible.with_suffix(cible.suffix + ".part")
    print(f"[get]  {source.url}")
    with urllib.request.urlopen(_requete(source.url), timeout=60) as rep, partiel.open("wb") as out:
        shutil.copyfileobj(rep, out, length=1 << 20)

    empreinte = sha1_fichier(partiel)
    if empreinte != source.sha1:
        partiel.unlink()
        raise RuntimeError(
            f"SHA-1 inattendu pour {source.periode} : {empreinte} au lieu de {source.sha1}. "
            "La publication a peut-être changé : voir `python -m dvf_lakehouse.download --lister`."
        )
    partiel.replace(cible)
    print(f"[ok]   {cible.name} ({cible.stat().st_size / 1e6:.0f} Mo)")
    return cible


def lister_ressources() -> None:
    """Affiche les fichiers annuels publiés en ce moment sur data.gouv.fr."""
    with urllib.request.urlopen(_requete(DATASET_API), timeout=60) as rep:
        jeu = json.load(rep)
    for r in jeu["resources"]:
        if r.get("format") == "csv.gz":
            somme = (r.get("checksum") or {}).get("value")
            print(f"{r['title']}\n  {r['url']}\n  taille={r.get('filesize')} sha1={somme}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lister", action="store_true", help="lister les ressources publiées")
    args = parser.parse_args(argv)
    if args.lister:
        lister_ressources()
        return 0
    for source in SOURCES:
        telecharger(source)
    return 0


if __name__ == "__main__":
    sys.exit(main())
