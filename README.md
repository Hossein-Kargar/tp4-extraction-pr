# TP4 - Extraction et Preparation des Donnees

Ce depot contient le travail de preparation des donnees pour le TP4 de MGL804.

## Contenu

- `tp4_guide_steps.py`
  - Reproduit les etapes 1.1, 1.2 et 2.1 du guide.
  - Charge les tables Parquet AIDev depuis Hugging Face.
  - Met les tables en cache local dans `data_cache/`.
  - Produit des sorties intermediaires dans `outputs/`.

- `extraction_pr.py`
  - Script base sur `group_6.csv` pour produire un export Excel de commentaires deja prepares.

- `TP4_MGL804_Lab_Guide.ipynb`
  - Notebook du guide du laboratoire.

## Etapes couvertes

- `1.1` Filtrer les PRs agentiques
- `1.2` Joindre `Comments -> Reviews -> Pull Requests`
- `2.1` Filtrer les projets `Java` et `Python`

## Resultats produits par `tp4_guide_steps.py`

Fichiers generes dans `outputs/` :

- `df_agentic.parquet`
- `df_comments_agentic.parquet`
- `df_prs_java_python.parquet`
- `tp4_guide_steps.xlsx`

## Jointures utilisees

1. `pr_review_comments_v2.pull_request_review_id -> pr_reviews.id`
2. `pr_reviews.pr_id -> pull_request.id`
3. `pull_request.repo_id -> repository.id`

## Agents IA retenus

- `OpenAI_Codex`
- `Copilot`
- `Devin`
- `Cursor`
- `Claude_Code`

## Commande principale

```powershell
.\.venv\Scripts\python.exe tp4_guide_steps.py
```

## Notes de partage

- Les gros fichiers generes (`data_cache/`, `outputs/`, `*.parquet`, `*.xlsx`) sont ignores par Git.
- Pour partager les resultats, privilegier OneDrive/Google Drive.
- Pour partager la methode, utiliser ce depot GitHub.
