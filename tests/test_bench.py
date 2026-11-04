from __future__ import annotations

from datetime import datetime

from dvf_lakehouse.bench import part_bridee, synthetiser

JOURNAL = """\
2026-11-04 12:43:00 88C load=2.10 ralenti=0
2026-11-04 12:43:05 91C load=2.20 ralenti=0
12:43:10 RALENTI 94C
2026-11-04 12:43:10 94C load=2.90 ralenti=1
2026-11-04 12:43:15 85C load=2.80 ralenti=1
12:43:25 NORMAL 68C
2026-11-04 12:43:25 68C load=2.50 ralenti=0
2026-11-04 12:43:30 66C load=2.40 ralenti=0
"""


def test_part_bridee(tmp_path, monkeypatch):
    journal = tmp_path / "temps.log"
    journal.write_text(JOURNAL)
    monkeypatch.setenv("DVF_JOURNAL_PAUSES", str(journal))
    # 6 relevés entre 12:43:00 et 12:43:30, dont 2 bridés.
    assert part_bridee(datetime(2026, 11, 4, 12, 43, 0), datetime(2026, 11, 4, 12, 43, 30)) == round(2 / 6, 3)
    assert part_bridee(datetime(2026, 11, 4, 12, 43, 25), datetime(2026, 11, 4, 12, 43, 30)) == 0.0
    monkeypatch.delenv("DVF_JOURNAL_PAUSES")
    assert part_bridee(datetime(2026, 11, 4), datetime(2026, 11, 5)) is None


def test_synthese_mediane_et_pic():
    mesures = [
        {"moteur": "duckdb", "etape": e, "mur_s": m, "etape_s": m - 1, "cpu_s": 2 * m, "rss_max_mo": r,
         "part_bridee": 0.0}
        for e in ("bronze", "silver", "gold") for m, r in ((10.0, 100), (20.0, 300))
    ]
    s = {r["etape"]: r for r in synthetiser(mesures, ["duckdb"])}
    assert s["bronze"]["mur_s"] == 15.0
    assert s["bronze"]["cpu_s"] == 30.0
    assert s["bronze"]["rss_max_mo"] == 300
    assert (s["gold"]["mur_min_s"], s["gold"]["mur_max_s"]) == (10.0, 20.0)
