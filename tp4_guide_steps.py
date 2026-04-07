from pathlib import Path
import re

import pandas as pd
from openpyxl.utils import get_column_letter


HF_TABLES = {
    "pull_request": "hf://datasets/hao-li/AIDev/pull_request.parquet",
    "pr_review_comments_v2": "hf://datasets/hao-li/AIDev/pr_review_comments_v2.parquet",
    "pr_reviews": "hf://datasets/hao-li/AIDev/pr_reviews.parquet",
    "repository": "hf://datasets/hao-li/AIDev/repository.parquet",
}

AGENTS = ["OpenAI_Codex", "Copilot", "Devin", "Cursor", "Claude_Code"]
CACHE_DIR = Path("data_cache")
OUTPUT_DIR = Path("outputs")
OUTPUT_XLSX = OUTPUT_DIR / "tp4_guide_steps.xlsx"
ILLEGAL_EXCEL_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")


def load_table(name: str) -> pd.DataFrame:
    """Load a table from local cache when available, otherwise from Hugging Face."""
    CACHE_DIR.mkdir(exist_ok=True)
    cache_path = CACHE_DIR / f"{name}.parquet"

    if cache_path.exists():
        print(f"[cache] Chargement local de {name}: {cache_path}")
        return pd.read_parquet(cache_path)

    print(f"[remote] Chargement distant de {name}: {HF_TABLES[name]}")
    df = pd.read_parquet(HF_TABLES[name])
    df.to_parquet(cache_path, index=False)
    print(f"[cache] Sauvegarde locale de {name}: {cache_path}")
    return df


def summarize_table(name: str, df: pd.DataFrame) -> None:
    """Print lightweight diagnostics to understand a table before joining it."""
    print("\n" + "=" * 80)
    print(f"{name}: {len(df):,} lignes, {len(df.columns)} colonnes")
    print(f"Colonnes: {list(df.columns)}")

    key_candidates = [col for col in df.columns if col == "id" or col.endswith("_id")]
    if key_candidates:
        print(f"Colonnes de jointure possibles: {key_candidates}")

    preview = df.head(3)
    if not preview.empty:
        print("Apercu:")
        preview_text = preview.to_string(index=False)
        print(preview_text.encode("cp1252", errors="replace").decode("cp1252"))


def find_first_column(df: pd.DataFrame, candidates: list[str], label: str) -> str:
    """Pick the first existing column among a set of likely names."""
    for candidate in candidates:
        if candidate in df.columns:
            return candidate

    raise KeyError(f"Impossible de trouver la colonne pour {label}. Candidates: {candidates}")


def build_agentic_prs(df_prs: pd.DataFrame) -> pd.DataFrame:
    """Guide step 1.1: keep only PRs opened by the five known AI agents."""
    agent_col = find_first_column(df_prs, ["agent"], "agent")

    print("\n" + "-" * 80)
    print("Etape 1.1 - Filtrer les PRs agentiques")
    unique_agents = sorted(df_prs[agent_col].dropna().astype(str).unique().tolist())
    print(f"Valeurs uniques de {agent_col}: {unique_agents}")

    df_agentic = df_prs[df_prs[agent_col].isin(AGENTS)].copy()
    print("\nNombre de PRs par agent:")
    print(df_agentic[agent_col].value_counts(dropna=False).to_string())
    print(f"Total PRs agentiques: {len(df_agentic):,}")
    return df_agentic


def build_comments_agentic(
    df_review_comments: pd.DataFrame,
    df_reviews: pd.DataFrame,
    df_agentic: pd.DataFrame,
) -> pd.DataFrame:
    """Guide step 1.2: comments -> reviews -> pull requests."""
    comment_review_id_col = find_first_column(
        df_review_comments,
        ["pull_request_review_id"],
        "pull_request_review_id dans les commentaires",
    )
    review_id_col = find_first_column(df_reviews, ["id"], "id dans les reviews")
    review_pr_id_col = find_first_column(df_reviews, ["pr_id"], "pr_id dans les reviews")
    pr_id_col = find_first_column(df_agentic, ["id"], "id dans les pull requests")

    print("\n" + "-" * 80)
    print("Etape 1.2 - Joindre Comments -> Reviews -> Pull Requests")
    print(
        "Jointure A: df_review_comments"
        f".merge(df_reviews, left_on='{comment_review_id_col}', right_on='{review_id_col}', how='left')"
    )

    df_comment_review = df_review_comments.merge(
        df_reviews,
        left_on=comment_review_id_col,
        right_on=review_id_col,
        how="left",
        suffixes=("_comment", "_review"),
    )
    print(f"Resultat A: {df_comment_review.shape}")

    print(
        "Jointure B: resultat_A"
        f".merge(df_agentic, left_on='{review_pr_id_col}', right_on='{pr_id_col}', how='inner')"
    )

    df_comments = df_comment_review.merge(
        df_agentic,
        left_on=review_pr_id_col,
        right_on=pr_id_col,
        how="inner",
        suffixes=("_review", "_pr"),
    )
    print(f"Resultat final: {df_comments.shape}")

    author_col = find_first_column(
        df_comments,
        ["user_type", "user_type_comment", "user_type_inline", "author_association"],
        "type d'auteur",
    )
    print(f"\nDistribution de la colonne auteur ({author_col}):")
    print(df_comments[author_col].astype(str).value_counts(dropna=False).head(20).to_string())

    return df_comments


def build_java_python_prs(df_agentic: pd.DataFrame, df_repos: pd.DataFrame) -> pd.DataFrame:
    """Guide step 2.1: keep only Java and Python repositories."""
    repo_language_col = find_first_column(df_repos, ["language"], "language dans repository")
    repo_id_col = find_first_column(df_repos, ["id"], "id dans repository")
    repo_name_col = find_first_column(df_repos, ["full_name"], "full_name dans repository")
    pr_repo_col = find_first_column(
        df_agentic,
        ["repo_id", "base_repo_id", "repository_id"],
        "repo_id dans pull_request",
    )
    agent_col = find_first_column(df_agentic, ["agent"], "agent")

    print("\n" + "-" * 80)
    print("Etape 2.1 - Filtrer les projets Java et Python")
    print("Top langages dans repository:")
    print(df_repos[repo_language_col].value_counts(dropna=False).head(10).to_string())

    df_repos_jp = df_repos[df_repos[repo_language_col].isin(["Java", "Python"])].copy()
    print(f"\nDepots Java/Python: {len(df_repos_jp):,}")

    df_prs_jp = df_agentic.merge(
        df_repos_jp[[repo_id_col, repo_language_col, repo_name_col]],
        left_on=pr_repo_col,
        right_on=repo_id_col,
        how="inner",
        suffixes=("_pr", "_repo"),
    )

    print(f"PRs agentiques en Java/Python: {len(df_prs_jp):,}")
    print("\nTableau croise language x agent:")
    print(pd.crosstab(df_prs_jp[repo_language_col], df_prs_jp[agent_col]).to_string())

    return df_prs_jp


def save_outputs(
    df_agentic: pd.DataFrame,
    df_comments: pd.DataFrame,
    df_prs_jp: pd.DataFrame,
) -> None:
    """Persist the intermediate outputs so later notebook steps run faster."""
    OUTPUT_DIR.mkdir(exist_ok=True)

    agentic_path = OUTPUT_DIR / "df_agentic.parquet"
    comments_path = OUTPUT_DIR / "df_comments_agentic.parquet"
    prs_jp_path = OUTPUT_DIR / "df_prs_java_python.parquet"

    df_agentic.to_parquet(agentic_path, index=False)
    df_comments.to_parquet(comments_path, index=False)
    df_prs_jp.to_parquet(prs_jp_path, index=False)

    print("\n" + "-" * 80)
    print("Fichiers sauvegardes:")
    print(f"- {agentic_path.resolve()}")
    print(f"- {comments_path.resolve()}")
    print(f"- {prs_jp_path.resolve()}")


def autosize_worksheet(worksheet, dataframe: pd.DataFrame) -> None:
    """Adjust column widths to keep the workbook readable."""
    for idx, column in enumerate(dataframe.columns, start=1):
        values = [str(column)] + dataframe[column].fillna("").astype(str).tolist()
        width = min(max(len(value) for value in values) + 2, 80)
        worksheet.column_dimensions[get_column_letter(idx)].width = width
    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions


def clean_excel_string(value: object) -> object:
    """Remove control chars and enforce Excel's cell-length limit for strings."""
    if not isinstance(value, str):
        return value

    cleaned = ILLEGAL_EXCEL_RE.sub("", value)
    if len(cleaned) > 32767:
        cleaned = cleaned[:32767]
    return cleaned


def sanitize_for_excel(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Prepare a dataframe for safe Excel export."""
    df = dataframe.copy()
    object_columns = df.select_dtypes(include=["object", "string"]).columns
    for column in object_columns:
        df[column] = df[column].map(clean_excel_string)
    return df


def write_excel(
    df_agentic: pd.DataFrame,
    df_comments: pd.DataFrame,
    df_prs_jp: pd.DataFrame,
) -> Path:
    """Export the main guide outputs to a single Excel workbook."""
    OUTPUT_DIR.mkdir(exist_ok=True)

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        sheets = {
            "PRs_Agentiques": df_agentic,
            "Comments_Reviews_PRs": df_comments,
            "PRs_Java_Python": df_prs_jp,
        }
        for sheet_name, dataframe in sheets.items():
            safe_dataframe = sanitize_for_excel(dataframe)
            safe_dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
            autosize_worksheet(writer.sheets[sheet_name], safe_dataframe)

    return OUTPUT_XLSX


def main() -> None:
    print("Chargement des tables utiles pour les etapes 1.1, 1.2 et 2.1...")
    df_prs = load_table("pull_request")
    df_review_comments = load_table("pr_review_comments_v2")
    df_reviews = load_table("pr_reviews")
    df_repos = load_table("repository")

    print("\nExploration rapide des tables")
    summarize_table("pull_request", df_prs)
    summarize_table("pr_review_comments_v2", df_review_comments)
    summarize_table("pr_reviews", df_reviews)
    summarize_table("repository", df_repos)

    df_agentic = build_agentic_prs(df_prs)
    df_comments = build_comments_agentic(df_review_comments, df_reviews, df_agentic)
    df_prs_jp = build_java_python_prs(df_agentic, df_repos)

    save_outputs(df_agentic, df_comments, df_prs_jp)
    output_xlsx = write_excel(df_agentic, df_comments, df_prs_jp)
    print(f"- {output_xlsx.resolve()}")


if __name__ == "__main__":
    main()
