# Contrôles qualité (spark)

49 contrôles bloquants réussis sur 49, 9 indicateurs. Durée : 47 s.

Généré par `make quality ENGINE=spark`.

## Bronze

| Contrôle | Statut | Valeur | Attendu |
|---|---|---|---|
| lignes bronze = lignes des CSV source | ok | 20 065 182 | = 20 065 182 |
| lignes par année | info | 2021 : 2 573 111<br>2022 : 4 676 488<br>2023 : 3 819 188<br>2024 : 3 505 340<br>2025 : 3 793 113<br>2026 : 1 697 942 |  |
| départements | info | 97 |  |
| id_mutation jamais nul | ok | 0 | = 0 |
| date_mutation jamais nul | ok | 0 | = 0 |
| nature_mutation jamais nul | ok | 0 | = 0 |
| code_commune jamais nul | ok | 0 | = 0 |
| code_departement jamais nul | ok | 0 | = 0 |
| id_parcelle jamais nul | ok | 0 | = 0 |
| partition annee = année de date_mutation | ok | 0 | = 0 |
| une seule date et un seul département par mutation | ok | 0 | = 0 |
| code_type_local cohérent avec type_local | ok | 0 | = 0 |
| valeur_fonciere nulle (% des lignes) | info | 0,923 |  |
| coordonnées absentes (% des lignes) | info | 2,85 |  |
| coordonnées hors de l'emprise du territoire (lignes) | info | 16 |  |

## Silver

| Contrôle | Statut | Valeur | Attendu |
|---|---|---|---|
| une ligne par mutation du bronze | ok | 7 302 732 | = 7 302 732 |
| id_mutation unique (mutations) | ok | 0 | = 0 |
| id_mutation unique (ventes) | ok | 0 | = 0 |
| ventes = mutations sans motif d'exclusion | ok | 4 138 268 | = 4 138 268 |
| motifs connus | ok | 0 | = 0 |
| ventes : date_mutation jamais nul | ok | 0 | = 0 |
| ventes : annee jamais nul | ok | 0 | = 0 |
| ventes : code_commune jamais nul | ok | 0 | = 0 |
| ventes : nom_commune jamais nul | ok | 0 | = 0 |
| ventes : type_local jamais nul | ok | 0 | = 0 |
| ventes : valeur_fonciere jamais nul | ok | 0 | = 0 |
| ventes : surface_reelle_bati jamais nul | ok | 0 | = 0 |
| ventes : prix_m2 jamais nul | ok | 0 | = 0 |
| ventes : type Appartement ou Maison | ok | 0 | = 0 |
| ventes : surface entre 9 et 1000 m² | ok | 0 | = 0 |
| ventes : valeur >= 1000 € | ok | 0 | = 0 |
| ventes : prix entre 100 et 30000 €/m² | ok | 0 | = 0 |
| ventes : score MAD <= 3.5 | ok | 0 | = 0 |
| ventes : prix_m2 = valeur / surface | ok | 0 | = 0 |
| mutations par motif d'exclusion | info | vente_retenue : 4 138 268<br>sans_logement : 1 757 377<br>nature_mutation : 505 198<br>local_activite : 383 057<br>plusieurs_logements : 223 917<br>valeur_absente : 168 362<br>prix_aberrant : 100 436<br>prix_m2_hors_bornes : 12 772<br>plusieurs_dispositions : 6 481<br>valeur_hors_bornes : 4 791<br>surface_hors_bornes : 1 754<br>surface_absente : 319 |  |
| référence du filtre MAD (ventes) | info | commune : 3 866 370<br>departement : 271 898 |  |
| ventes sans coordonnées (%) | info | 0,821 |  |

## Gold

| Contrôle | Statut | Valeur | Attendu |
|---|---|---|---|
| somme des ventes mensuelles = ventes silver | ok | 4 138 268 | = 4 138 268 |
| somme des ventes annuelles France = ventes silver | ok | 4 138 268 | = 4 138 268 |
| ventes annuelles Bordeaux Métropole = ventes silver des 28 communes | ok | 57 862 | = 57 862 |
| prix_commune_annee : clé unique (code_commune, annee, type_local) | ok | 0 | = 0 |
| prix_commune_annee : q1 <= médiane <= q3 | ok | 0 | = 0 |
| prix_commune_annee : médiane dans les bornes de prix | ok | 0 | = 0 |
| marche_france_mois : clé unique (annee, mois, type_local) | ok | 0 | = 0 |
| marche_france_mois : médiane dans les bornes de prix | ok | 0 | = 0 |
| marche_annee : clé unique (perimetre, annee, type_local) | ok | 0 | = 0 |
| marche_annee : q1 <= médiane <= q3 | ok | 0 | = 0 |
| marche_annee : médiane dans les bornes de prix | ok | 0 | = 0 |
| marche_semestre : clé unique (perimetre, semestre, type_local) | ok | 0 | = 0 |
| marche_semestre : q1 <= médiane <= q3 | ok | 0 | = 0 |
| marche_semestre : médiane dans les bornes de prix | ok | 0 | = 0 |
| bordeaux_metropole : clé unique (code_commune, annee, type_local) | ok | 0 | = 0 |
| bordeaux_metropole : q1 <= médiane <= q3 | ok | 0 | = 0 |
| bordeaux_metropole : médiane dans les bornes de prix | ok | 0 | = 0 |
| prix_commune_annee : au moins 5 ventes par ligne | ok | 0 | = 0 |
| lignes par table | info | prix_commune_annee : 115 691<br>marche_france_mois : 120<br>marche_annee : 36<br>marche_semestre : 60<br>bordeaux_metropole : 316 |  |
| les 28 communes de Bordeaux Métropole ont des ventes | ok | 28 | = 28 |
| noms des communes de la métropole conformes aux codes INSEE | ok | aucun | = aucun |
