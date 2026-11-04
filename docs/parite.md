# Parité Spark / DuckDB

Sorties des deux moteurs sur le jeu complet, jointes sur leur clé. Types de colonnes, entiers et chaînes : égalité stricte. Flottants : tolérance relative 1e-9.

| Table | Lignes | Résultat |
|---|---:|---|
| gold.prix_commune_annee | 115 691 | identique (flottants identiques au bit près) |
| gold.marche_france_mois | 120 | identique (flottants identiques au bit près) |
| gold.marche_annee | 36 | identique (flottants identiques au bit près) |
| gold.marche_semestre | 60 | identique (flottants identiques au bit près) |
| gold.bordeaux_metropole | 316 | identique (flottants identiques au bit près) |
| silver.ventes | 4 138 268 | identique (écart relatif max 2,9e-10 sur score_mad) |
| silver.mutations | 7 302 732 | identique (écart relatif max 2,9e-10 sur score_mad) |
