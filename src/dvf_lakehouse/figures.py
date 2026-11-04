"""Figures PNG (matplotlib, backend Agg) écrites dans docs/."""

from __future__ import annotations

import json
import sys

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

from dvf_lakehouse import config  # noqa: E402
from dvf_lakehouse.duckdb_pipeline.connection import connecter, lire_parquet  # noqa: E402
from dvf_lakehouse.duckdb_pipeline.silver import SILVER_HIVE  # noqa: E402

# Palette catégorielle (ordre fixe) et rampe séquentielle bleue.
BLEU, ORANGE, VERT_EAU = "#2a78d6", "#eb6834", "#1baf7a"
TEXTE, TEXTE_2, GRILLE, FOND = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
RAMPE_BLEUE = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
COULEUR_PERIMETRE = {"Bordeaux": BLEU, "Bordeaux Métropole": ORANGE, "France": VERT_EAU}
COULEUR_TYPE = {"Appartement": BLEU, "Maison": ORANGE}
SOURCE = "Source : DVF géolocalisées (DGFiP, Etalab), juillet 2021 à juin 2026. Traitement dvf-lakehouse."


def _style() -> None:
    plt.rcParams.update({
        "figure.facecolor": FOND, "axes.facecolor": FOND, "savefig.facecolor": FOND,
        "font.size": 10, "axes.titlesize": 11, "axes.titleweight": "bold", "axes.titlelocation": "left",
        "axes.edgecolor": TEXTE_2, "axes.labelcolor": TEXTE_2, "xtick.color": TEXTE_2, "ytick.color": TEXTE_2,
        "text.color": TEXTE, "axes.spines.top": False, "axes.spines.right": False,
        "axes.grid": True, "grid.color": GRILLE, "grid.linewidth": 0.8, "axes.axisbelow": True,
        "legend.frameon": False, "lines.linewidth": 2,
    })


def _milliers(x, _pos=None) -> str:
    return f"{x:,.0f}".replace(",", " ")


def _source(fig) -> None:
    fig.text(0.01, 0.01, SOURCE, fontsize=7.5, color=TEXTE_2, ha="left", va="bottom")


def prix_bordeaux_france(con, ch) -> None:
    df = con.execute(f"""
        SELECT perimetre, semestre, type_local, nb_ventes, prix_m2_median
        FROM {lire_parquet(ch.gold('marche_semestre'))} ORDER BY semestre
    """).df()
    semestres = sorted(df["semestre"].unique())
    x = np.arange(len(semestres))
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.6), sharex=True)
    for ax, type_local in zip(axes, config.TYPES_LOGEMENT):
        for perimetre in ("Bordeaux", "Bordeaux Métropole", "France"):
            s = df[(df.perimetre == perimetre) & (df.type_local == type_local)].set_index("semestre")
            y = s.reindex(semestres)["prix_m2_median"].to_numpy()
            couleur = COULEUR_PERIMETRE[perimetre]
            ax.plot(x, y, color=couleur, marker="o", markersize=4, label=perimetre)
            ax.annotate(f"{y[-1]:,.0f} €".replace(",", " "), (x[-1], y[-1]), xytext=(6, 0),
                        textcoords="offset points", va="center", fontsize=8.5, color=TEXTE)
        ax.set_title(f"{type_local}s : prix médian au m²")
        ax.set_xticks(x, [s.replace("-", "\n") for s in semestres], fontsize=8)
        ax.yaxis.set_major_formatter(_milliers)
        ax.set_ylim(0, None)
        ax.set_xlim(-0.4, len(semestres) - 0.2)
        ax.set_ylabel("€/m²")
        ax.grid(axis="x", visible=False)
    axes[0].legend(loc="lower left", ncols=3, fontsize=8.5)
    fig.suptitle("Bordeaux, Bordeaux Métropole et France, par semestre", x=0.01, ha="left", fontsize=12,
                 fontweight="bold")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    _source(fig)
    fig.savefig(config.DOCS_DIR / "prix_m2_bordeaux_vs_france.png", dpi=150)
    plt.close(fig)


def carte_metropole(con, ch) -> None:
    codes = ", ".join(f"'{c}'" for c in config.BORDEAUX_METROPOLE)
    df = con.execute(f"""
        SELECT code_commune, longitude, latitude, prix_m2
        FROM {lire_parquet(ch.silver_ventes, SILVER_HIVE)}
        WHERE code_commune IN ({codes}) AND latitude IS NOT NULL
    """).df()
    # Écarte les quelques points géocodés loin de la métropole.
    df = df[df.longitude.between(-0.85, -0.40) & df.latitude.between(44.70, 45.03)]
    lat0 = np.deg2rad(df.latitude.mean())
    fig, ax = plt.subplots(figsize=(8.6, 9.2))
    carte = LinearSegmentedColormap.from_list("bleus", RAMPE_BLEUE)
    hb = ax.hexbin(df.longitude, df.latitude, C=df.prix_m2, reduce_C_function=np.median, gridsize=70,
                   mincnt=5, cmap=carte, vmin=2500, vmax=6000, linewidths=0.2, edgecolors=FOND)
    ax.set_aspect(1 / np.cos(lat0))
    barre = fig.colorbar(hb, ax=ax, orientation="horizontal", shrink=0.7, pad=0.07, aspect=40)
    barre.set_label("prix médian au m² des ventes de l'hexagone (€)", color=TEXTE_2)
    barre.ax.xaxis.set_major_formatter(_milliers)
    barre.outline.set_visible(False)
    centres = df.groupby("code_commune")[["longitude", "latitude"]].median()
    for code, (lon, lat) in centres.iterrows():
        ax.annotate(config.BORDEAUX_METROPOLE[code], (lon, lat), fontsize=7, ha="center", va="center",
                    color=TEXTE, bbox={"boxstyle": "round,pad=0.15", "fc": FOND, "ec": "none", "alpha": 0.7})
    ax.set_title(f"Bordeaux Métropole : prix au m², {_milliers(len(df))} ventes")
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")
    ax.grid(False)
    fig.text(0.01, 0.035, "Appartements et maisons, juillet 2021 à juin 2026. Hexagones de 5 ventes au moins.",
             fontsize=8, color=TEXTE_2)
    _source(fig)
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.savefig(config.DOCS_DIR / "carte_bordeaux_metropole.png", dpi=150)
    plt.close(fig)


def volumes(con, ch) -> None:
    mois = con.execute(f"""
        SELECT annee, mois, type_local, nb_ventes FROM {lire_parquet(ch.gold('marche_france_mois'))}
        ORDER BY annee, mois
    """).df()
    annees = mois.groupby(["annee", "type_local"])["nb_ventes"].sum().unstack()
    fig, (haut, bas) = plt.subplots(2, 1, figsize=(10, 7.6), gridspec_kw={"height_ratios": [1.1, 1]})

    x = np.arange(len(annees))
    bas_pile = np.zeros(len(annees))
    for type_local in config.TYPES_LOGEMENT:
        valeurs = annees[type_local].to_numpy()
        for i, annee in enumerate(annees.index):
            partielle = annee in config.ANNEES_PARTIELLES
            haut.bar(x[i], valeurs[i], bottom=bas_pile[i], width=0.62, color=COULEUR_TYPE[type_local],
                     alpha=0.45 if partielle else 1, edgecolor=FOND, linewidth=2,
                     label=type_local if i == 1 else None)
        bas_pile += valeurs
    for i, annee in enumerate(annees.index):
        etiquette = _milliers(bas_pile[i])
        if annee in config.ANNEES_PARTIELLES:
            etiquette += f"\n({config.ANNEES_PARTIELLES[annee]})"
        haut.annotate(etiquette, (x[i], bas_pile[i]), xytext=(0, 4), textcoords="offset points",
                      ha="center", va="bottom", fontsize=8.5)
    haut.set_xticks(x, [str(a) for a in annees.index])
    haut.yaxis.set_major_formatter(_milliers)
    haut.set_ylim(0, bas_pile.max() * 1.18)
    haut.set_title("Ventes retenues par année (un seul logement, après filtres)")
    haut.legend(loc="upper right", ncols=2)
    haut.grid(axis="x", visible=False)

    for type_local in config.TYPES_LOGEMENT:
        s = mois[mois.type_local == type_local]
        dates = np.array([np.datetime64(f"{a}-{m:02d}") for a, m in zip(s.annee, s.mois)])
        bas.plot(dates, s.nb_ventes, color=COULEUR_TYPE[type_local], label=type_local)
        decalage = 5 if type_local == "Maison" else -5
        bas.annotate(type_local, (dates[-1], s.nb_ventes.iloc[-1]), xytext=(6, decalage),
                     textcoords="offset points", va="center", fontsize=8.5)
    bas.set_title("Ventes retenues par mois")
    bas.yaxis.set_major_formatter(_milliers)
    bas.set_ylim(0, None)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    _source(fig)
    fig.savefig(config.DOCS_DIR / "volumes_ventes.png", dpi=150)
    plt.close(fig)


def benchmark() -> None:
    chemin = config.DOCS_DIR / "benchmark.json"
    if not chemin.exists():
        return
    synthese = json.loads(chemin.read_text())["synthese"]
    etapes = ["bronze", "silver", "gold"]
    panneaux = (
        ("mur_s", "Temps mur par étape (s)", lambda v: f"{v:.0f} s", 1),
        ("cpu_s", "Temps CPU consommé (s)", lambda v: f"{v:.0f} s", 1),
        ("rss_max_mo", "Pic de mémoire résidente (Go)", lambda v: f"{v:.1f} Go".replace(".", ","), 1 / 1024),
    )
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.6))
    y = np.arange(len(etapes))
    for k, (moteur, couleur, nom) in enumerate((("duckdb", BLEU, "DuckDB"), ("spark", ORANGE, "Spark"))):
        lignes = {r["etape"]: r for r in synthese if r["moteur"] == moteur}
        if len(lignes) < len(etapes):
            continue
        decalage = -0.2 if k == 0 else 0.2
        for ax, (cle, _titre, fmt, echelle) in zip(axes, panneaux):
            valeurs = [lignes[e][cle] * echelle for e in etapes]
            ax.barh(y + decalage, valeurs, height=0.36, color=couleur, label=nom)
            for yi, v in zip(y + decalage, valeurs):
                ax.annotate(fmt(v), (v, yi), xytext=(3, 0), textcoords="offset points", va="center", fontsize=8)
    for ax, (_cle, titre, _fmt, _e) in zip(axes, panneaux):
        ax.set_yticks(y, etapes)
        ax.invert_yaxis()
        ax.set_title(titre)
        ax.grid(axis="y", visible=False)
        ax.margins(x=0.22)
    axes[0].legend(loc="lower right")
    fig.tight_layout()
    fig.savefig(config.DOCS_DIR / "benchmark.png", dpi=150)
    plt.close(fig)


def main(engine: str = "duckdb") -> int:
    _style()
    config.DOCS_DIR.mkdir(exist_ok=True)
    ch = config.chemins(engine)
    con = connecter()
    prix_bordeaux_france(con, ch)
    carte_metropole(con, ch)
    volumes(con, ch)
    benchmark()
    con.close()
    print("figures écrites dans docs/")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "duckdb"))
