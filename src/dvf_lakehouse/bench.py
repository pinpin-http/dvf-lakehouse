"""Benchmark : temps, CPU et mémoire de chaque étape, pour chaque moteur.

Chaque étape tourne dans un processus séparé (`python -m dvf_lakehouse <étape>`),
comme un job batch. Toutes les 0,2 s on relève, sur l'arbre de processus
(Python + JVM pour Spark), la mémoire résidente (RSS, on garde le pic) et le
temps CPU consommé (user + system, cumulé par processus).

- mur : durée totale du processus, démarrage du moteur compris ;
- étape : durée du traitement seul, mesurée dans le processus ;
- CPU : secondes de processeur consommées, peu sensibles aux ralentissements
  imposés de l'extérieur, contrairement au temps mur.

Si DVF_JOURNAL_PAUSES désigne le journal d'une sonde thermique (une ligne
horodatée toutes les quelques secondes, avec « ralenti=1 » ou « pause=1 » quand
les processus sont bridés), chaque mesure note la part de son temps passée
sous bridage. Une mesure faite sous bridage est relancée, trois fois au plus ;
si aucun essai n'y échappe, on garde le moins bridé et sa part de bridage
figure dans le rapport.
"""

from __future__ import annotations

import json
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import psutil

from dvf_lakehouse import config
from dvf_lakehouse.pipeline import ETAPES

MARQUEURS_BRIDAGE = ("ralenti=1", "pause=1")
RELANCES_MAX = 3


def _lire_instant(ligne: str) -> datetime | None:
    morceaux = ligne.split()
    try:
        if len(morceaux[0]) == 10:  # "2026-11-04 12:16:22 85C load=1.67 ralenti=0"
            return datetime.fromisoformat(f"{morceaux[0]} {morceaux[1]}")
        # "12:20:28 RALENTI 95C"
        return datetime.combine(date.today(), datetime.strptime(morceaux[0], "%H:%M:%S").time())
    except (ValueError, IndexError):
        return None


def part_bridee(debut: datetime, fin: datetime) -> float | None:
    """Part des relevés du journal thermique marqués comme bridés entre `debut` et `fin`."""
    journal = os.environ.get("DVF_JOURNAL_PAUSES")
    if not journal or not Path(journal).exists():
        return None
    total = bridees = 0
    for ligne in Path(journal).read_text(errors="replace").splitlines():
        if "=" not in ligne:  # lignes d'événement (RALENTI, NORMAL...) : les relevés suffisent
            continue
        instant = _lire_instant(ligne)
        if instant is None or not (debut <= instant <= fin):
            continue
        total += 1
        bridees += any(m in ligne for m in MARQUEURS_BRIDAGE)
    return round(bridees / total, 3) if total else 0.0


class Sonde:
    """Relève RSS et temps CPU d'un arbre de processus."""

    def __init__(self, pid: int) -> None:
        self.racine = psutil.Process(pid)
        self.cpu: dict[int, float] = {}
        self.pic_rss = 0

    def relever(self) -> None:
        rss = 0
        try:
            processus = [self.racine, *self.racine.children(recursive=True)]
        except psutil.NoSuchProcess:
            return
        for p in processus:
            try:
                with p.oneshot():
                    rss += p.memory_info().rss
                    t = p.cpu_times()
                    # Pas de children_* : le temps d'un enfant attendu serait compté deux fois.
                    self.cpu[p.pid] = t.user + t.system
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        self.pic_rss = max(self.pic_rss, rss)


def _mesurer_une_fois(engine: str, etape: str) -> dict:
    commande = [sys.executable, "-m", "dvf_lakehouse", etape, "--engine", engine]
    charge = round(os.getloadavg()[0], 1)
    runs = config.chemins(engine).racine / "_runs"
    runs.mkdir(parents=True, exist_ok=True)
    instant_debut = datetime.now()
    debut = time.perf_counter()
    # Sortie du processus dans un fichier : un tube plein bloquerait l'enfant.
    with open(runs / f"{etape}.log", "w") as sortie:
        proc = subprocess.Popen(commande, stdout=sortie, stderr=subprocess.STDOUT)
        sonde = Sonde(proc.pid)
        while proc.poll() is None:
            sonde.relever()
            time.sleep(0.2)
    mur = time.perf_counter() - debut
    if proc.returncode != 0:
        raise RuntimeError(f"{engine} {etape} a échoué :\n{(runs / f'{etape}.log').read_text()[-3000:]}")
    journal = json.loads((runs / f"{etape}.json").read_text())
    return {
        "moteur": engine,
        "etape": etape,
        "debut": instant_debut.isoformat(timespec="seconds"),
        "mur_s": round(mur, 1),
        "etape_s": round(journal["duree_s"], 1),
        "demarrage_s": round(journal["demarrage_moteur_s"], 1),
        "cpu_s": round(sum(sonde.cpu.values()), 1),
        "rss_max_mo": round(sonde.pic_rss / 2**20),
        "charge_1min": charge,
        "part_bridee": part_bridee(instant_debut, datetime.now()),
        "stats": journal["stats"],
    }


def mesurer(engine: str, etape: str) -> dict:
    mesure = _mesurer_une_fois(engine, etape)
    relances = 0
    while mesure["part_bridee"] and relances < RELANCES_MAX:
        relances += 1
        print(f"{engine} {etape} : bridage thermique pendant la mesure, relance {relances}", flush=True)
        nouvelle = _mesurer_une_fois(engine, etape)
        if nouvelle["part_bridee"] <= mesure["part_bridee"]:
            mesure = nouvelle
    mesure["relances"] = relances
    return mesure


def _version_java() -> str:
    from dvf_lakehouse.spark_pipeline.session import choisir_java

    choisir_java()
    java = os.path.join(os.environ["JAVA_HOME"], "bin", "java") if os.environ.get("JAVA_HOME") else "java"
    sortie = subprocess.run([java, "-version"], capture_output=True, text=True)
    return (sortie.stderr or sortie.stdout).splitlines()[0]


def limites_cgroup() -> dict:
    """Plafonds CPU et mémoire du cgroup courant (cgroup v2), s'il y en a."""
    try:
        chemin = Path("/proc/self/cgroup").read_text().strip().split("::", 1)[1]
        dossier = Path("/sys/fs/cgroup") / chemin.lstrip("/")
        quota, periode = (dossier / "cpu.max").read_text().split()
        memoire = (dossier / "memory.max").read_text().strip()
    except (OSError, IndexError, ValueError):
        return {}
    return {
        "cgroup": chemin,
        "cpu_coeurs_max": None if quota == "max" else round(int(quota) / int(periode), 2),
        "memoire_max_go": None if memoire == "max" else round(int(memoire) / 2**30, 1),
    }


def machine() -> dict:
    import duckdb
    import pyspark

    cpu = platform.processor()
    try:
        with open("/proc/cpuinfo") as f:
            cpu = next(ligne.split(":", 1)[1].strip() for ligne in f if ligne.startswith("model name"))
    except (OSError, StopIteration):
        pass
    return {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "cpu": cpu,
        "coeurs_logiques": psutil.cpu_count(),
        "ram_go": round(psutil.virtual_memory().total / 2**30, 1),
        "python": platform.python_version(),
        "pyspark": pyspark.__version__,
        "duckdb": duckdb.__version__,
        "java": _version_java(),
        "spark": {"master": config.SPARK_MASTER, "driver_memory": config.SPARK_DRIVER_MEMORY,
                  "shuffle_partitions": config.SPARK_SHUFFLE_PARTITIONS},
        "duckdb_config": {"threads": config.DUCKDB_THREADS, "memory_limit": config.DUCKDB_MEMORY_LIMIT},
        "plafonds": limites_cgroup(),
    }


def synthetiser(mesures: list[dict], engines: list[str]) -> list[dict]:
    synthese = []
    for engine in engines:
        for etape in ETAPES:
            serie = [m for m in mesures if m["moteur"] == engine and m["etape"] == etape]
            if not serie:
                continue
            parts = [m["part_bridee"] for m in serie if m["part_bridee"] is not None]
            synthese.append({
                "moteur": engine, "etape": etape, "passages": len(serie),
                "mur_s": round(statistics.median(m["mur_s"] for m in serie), 1),
                "mur_min_s": min(m["mur_s"] for m in serie),
                "mur_max_s": max(m["mur_s"] for m in serie),
                "etape_s": round(statistics.median(m["etape_s"] for m in serie), 1),
                "cpu_s": round(statistics.median(m["cpu_s"] for m in serie), 1),
                "rss_max_mo": max(m["rss_max_mo"] for m in serie),
                "part_bridee_max": max(parts) if parts else None,
            })
    return synthese


def _dec(x: float) -> str:
    """Nombre à la française : 1.0 -> "1", 15.3 -> "15,3"."""
    return f"{x:g}".replace(".", ",")


def _pct(x: float | None) -> str:
    return "n.d." if x is None else f"{100 * x:.0f} %"


def rapport_markdown(resultat: dict) -> str:
    m = resultat["machine"]
    plafonds = m.get("plafonds") or {}
    lignes = [
        "# Benchmark Spark / DuckDB",
        "",
        f"Mesuré le {m['date']} sur {m['cpu']} ({m['coeurs_logiques']} cœurs logiques, "
        f"{_dec(m['ram_go'])} Go de RAM).",
        "",
        f"- Spark {m['pyspark']} (`{m['spark']['master']}`, driver {m['spark']['driver_memory']}, "
        f"{m['spark']['shuffle_partitions']} partitions de shuffle), {m['java']}",
        f"- DuckDB {m['duckdb']} (`threads={m['duckdb_config']['threads']}`, "
        f"`memory_limit='{m['duckdb_config']['memory_limit']}'`)",
        f"- Python {m['python']}, {resultat['repetitions']} passage(s) par étape, valeurs médianes",
    ]
    if plafonds.get("cpu_coeurs_max"):
        lignes.append(f"- Processus plafonnés par cgroup (portée systemd) à {_dec(plafonds['cpu_coeurs_max'])} cœur "
                      f"et {_dec(plafonds['memoire_max_go'])} Go")
    lignes += [
        "",
        "Mur : durée du processus, démarrage du moteur compris. Étape : traitement seul. "
        "CPU : secondes de processeur consommées par l'arbre de processus. "
        "RSS max : pic de mémoire résidente (Python et JVM additionnés). "
        "Bridage : part du temps de la mesure pendant laquelle la sonde thermique de la machine "
        "avait abaissé le plafond CPU à 25 %. Le temps mur en souffre, le temps CPU beaucoup moins.",
        "",
        "| Étape | Moteur | Mur (s) | Mur min-max (s) | Étape (s) | CPU (s) | RSS max (Mo) | Bridage max |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in resultat["synthese"]:
        lignes.append(
            f"| {r['etape']} | {r['moteur']} | {r['mur_s']:.1f} | {r['mur_min_s']:.0f}-{r['mur_max_s']:.0f} "
            f"| {r['etape_s']:.1f} | {r['cpu_s']:.0f} | {r['rss_max_mo']} | {_pct(r['part_bridee_max'])} |")
    totaux: dict[str, dict] = {}
    for r in resultat["synthese"]:
        t = totaux.setdefault(r["moteur"], {"mur_s": 0.0, "etape_s": 0.0, "cpu_s": 0.0, "rss_max_mo": 0})
        for k in ("mur_s", "etape_s", "cpu_s"):
            t[k] += r[k]
        t["rss_max_mo"] = max(t["rss_max_mo"], r["rss_max_mo"])
    for moteur, t in totaux.items():
        lignes.append(f"| total | {moteur} | {t['mur_s']:.1f} | | {t['etape_s']:.1f} | {t['cpu_s']:.0f} "
                      f"| {t['rss_max_mo']} | |")
    return "\n".join(lignes) + "\n"


def main(engines: list[str], repetitions: int = 1) -> int:
    resultat = {"machine": machine(), "repetitions": repetitions, "mesures": []}
    sortie_json = config.DOCS_DIR / "benchmark.json"
    config.DOCS_DIR.mkdir(exist_ok=True)
    # Les moteurs alternent à chaque passage pour subir des conditions comparables.
    for passage in range(1, repetitions + 1):
        for engine in engines:
            for etape in ETAPES:
                mesure = mesurer(engine, etape)
                mesure["passage"] = passage
                print(f"passage {passage}  {engine:<7} {etape:<7} mur {mesure['mur_s']:>7.1f} s  "
                      f"CPU {mesure['cpu_s']:>7.1f} s  RSS max {mesure['rss_max_mo']:>6} Mo  "
                      f"bridage {_pct(mesure['part_bridee'])}", flush=True)
                resultat["mesures"].append(mesure)
                # Sauvegarde au fil de l'eau : une coupure ne perd pas les mesures faites.
                resultat["synthese"] = synthetiser(resultat["mesures"], engines)
                sortie_json.write_text(json.dumps(resultat, indent=2, ensure_ascii=False))
    (config.DOCS_DIR / "benchmark.md").write_text(rapport_markdown(resultat))
    print("résultats : docs/benchmark.md, docs/benchmark.json")
    return 0


if __name__ == "__main__":
    sys.exit(main(list(config.ENGINES)))
