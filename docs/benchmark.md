# Benchmark Spark / DuckDB

Mesuré le 2026-11-04 11:43 UTC sur 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz (8 cœurs logiques, 15,3 Go de RAM).

- Spark 4.2.1 (`local[2]`, driver 3g, 16 partitions de shuffle), openjdk version "21.0.13" 2026-10-20
- DuckDB 1.5.6 (`threads=2`, `memory_limit='2.5GB'`)
- Python 3.12.14, 2 passage(s) par étape, valeurs médianes
- Processus plafonnés par cgroup (portée systemd) à 1 cœur et 5 Go

Mur : durée du processus, démarrage du moteur compris. Étape : traitement seul. CPU : secondes de processeur consommées par l'arbre de processus. RSS max : pic de mémoire résidente (Python et JVM additionnés). Bridage : part du temps de la mesure pendant laquelle la sonde thermique de la machine avait abaissé le plafond CPU à 25 %. Le temps mur en souffre, le temps CPU beaucoup moins.

| Étape | Moteur | Mur (s) | Mur min-max (s) | Étape (s) | CPU (s) | RSS max (Mo) | Bridage max |
|---|---|---:|---:|---:|---:|---:|---:|
| bronze | spark | 359.8 | 352-368 | 348.3 | 340 | 3320 | 4 % |
| silver | spark | 525.8 | 440-612 | 515.0 | 498 | 3666 | 7 % |
| gold | spark | 111.6 | 90-134 | 101.3 | 108 | 2812 | 0 % |
| bronze | duckdb | 109.7 | 94-125 | 109.4 | 101 | 2924 | 16 % |
| silver | duckdb | 136.4 | 113-159 | 135.9 | 132 | 2942 | 0 % |
| gold | duckdb | 7.6 | 6-9 | 7.2 | 7 | 571 | 0 % |
| total | spark | 997.2 | | 964.6 | 947 | 3666 | |
| total | duckdb | 253.7 | | 252.5 | 240 | 2942 | |
