"""Point d'entrée : python -m dvf_lakehouse <commande> [options]."""

from __future__ import annotations

import argparse
import sys

from dvf_lakehouse.config import ENGINES


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dvf_lakehouse")
    sous = parser.add_subparsers(dest="commande", required=True)

    sous.add_parser("download", help="télécharge les fichiers géo-DVF (idempotent)")
    for etape in ("bronze", "silver", "gold", "pipeline"):
        p = sous.add_parser(etape, help=f"construit la couche {etape}" if etape != "pipeline"
                            else "enchaîne bronze, silver et gold")
        p.add_argument("--engine", choices=ENGINES, default="duckdb")
    p = sous.add_parser("qualite", help="contrôles qualité entre couches et rapport")
    p.add_argument("--engine", choices=ENGINES, default="duckdb")
    sous.add_parser("comparer", help="compare silver et gold produits par les deux moteurs")
    p = sous.add_parser("bench", help="mesure temps et mémoire de chaque étape")
    p.add_argument("--engines", nargs="+", choices=ENGINES, default=list(ENGINES))
    p.add_argument("--repetitions", type=int, default=1)
    p = sous.add_parser("figures", help="génère les figures PNG dans docs/")
    p.add_argument("--engine", choices=ENGINES, default="duckdb")

    args = parser.parse_args(argv)

    if args.commande == "download":
        from dvf_lakehouse.download import main as telecharger
        return telecharger([])
    if args.commande in ("bronze", "silver", "gold", "pipeline"):
        from dvf_lakehouse.pipeline import ETAPES, executer
        etapes = ETAPES if args.commande == "pipeline" else (args.commande,)
        executer(etapes, args.engine)
        return 0
    if args.commande == "qualite":
        from dvf_lakehouse.quality import main as qualite
        return qualite(args.engine)
    if args.commande == "comparer":
        from dvf_lakehouse.compare import main as comparer
        return comparer()
    if args.commande == "bench":
        from dvf_lakehouse.bench import main as bench
        return bench(args.engines, args.repetitions)
    if args.commande == "figures":
        from dvf_lakehouse.figures import main as figures
        return figures(args.engine)
    return 1


if __name__ == "__main__":
    sys.exit(main())
