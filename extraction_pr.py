from pathlib import Path
from datetime import datetime

import pandas as pd
from openpyxl.utils import get_column_letter


INPUT_CSV = Path("group_6.csv")
OUTPUT_XLSX = Path("extractions_pr.xlsx")

KEYWORD_TO_CATEGORY = {
    "Add*": "Changement structurel",
    "Chang*": "Changement structurel",
    "Chang* the name": "Nommage / lisibilite",
    "Cleanup": "Code mort / nettoyage",
    "Clean* up": "Code mort / nettoyage",
    "Code clarity": "Nommage / lisibilite",
    "Code clean*": "Code mort / nettoyage",
    "Code organization": "Organisation du code",
    "Code review": "Autre",
    "Clean code": "Code mort / nettoyage",
    "Creat*": "Changement structurel",
    "Customiz*": "Remplacement / migration",
    "Easier to maintain": "Nommage / lisibilite",
    "Encapsulat*": "Organisation du code",
    "Enhanc*": "Autre",
    "Extend*": "Changement structurel",
    "Extract*": "Changement structurel",
    "Fix*": "Autre",
    "Inlin*": "Changement structurel",
    "Improv*": "Autre",
    "Improv* code quality": "Code mort / nettoyage",
    "Introduc*": "Changement structurel",
    "Merg*": "Organisation du code",
    "Modif*": "Autre",
    "Modulariz*": "Organisation du code",
    "Migrat*": "Remplacement / migration",
    "Mov*": "Organisation du code",
    "Organiz*": "Organisation du code",
    "Polish*": "Nommage / lisibilite",
    "Reduc*": "Duplication / simplification",
    "Refactor*": "Changement structurel",
    "Refin*": "Nommage / lisibilite",
    "Remov*": "Code mort / nettoyage",
    "Remov* redundant code": "Duplication / simplification",
    "Renam*": "Nommage / lisibilite",
    "Remov* unused dependencies": "Code mort / nettoyage",
    "Reorganiz*": "Organisation du code",
    "Replac*": "Remplacement / migration",
    "Restructur*": "Organisation du code",
    "Rework*": "Changement structurel",
    "Rewrit*": "Changement structurel",
    "Simplif*": "Duplication / simplification",
    "Split*": "Changement structurel",
}

KEYWORD_TO_REFACTORING_TYPE = {
    "Add*": "Ajout / extension",
    "Chang*": "Modification / restructuration",
    "Chang* the name": "Renommage",
    "Cleanup": "Nettoyage de code",
    "Clean* up": "Nettoyage de code",
    "Code clarity": "Amelioration de lisibilite",
    "Code clean*": "Nettoyage de code",
    "Code organization": "Reorganisation du code",
    "Code review": "Autre / a verifier",
    "Clean code": "Nettoyage de code",
    "Creat*": "Creation / extraction",
    "Customiz*": "Remplacement / adaptation",
    "Easier to maintain": "Amelioration de maintenabilite",
    "Encapsulat*": "Encapsulation",
    "Enhanc*": "Amelioration",
    "Extend*": "Extension",
    "Extract*": "Extraction",
    "Fix*": "Correction",
    "Inlin*": "Inline",
    "Improv*": "Amelioration",
    "Improv* code quality": "Amelioration de qualite",
    "Introduc*": "Introduction de structure",
    "Merg*": "Fusion",
    "Modif*": "Modification",
    "Modulariz*": "Modularisation",
    "Migrat*": "Migration",
    "Mov*": "Deplacement",
    "Organiz*": "Organisation",
    "Polish*": "Polissage / finition",
    "Reduc*": "Reduction / simplification",
    "Refactor*": "Refactorisation",
    "Refin*": "Raffinement",
    "Remov*": "Suppression",
    "Remov* redundant code": "Suppression de duplication",
    "Renam*": "Renommage",
    "Remov* unused dependencies": "Suppression de dependances inutiles",
    "Reorganiz*": "Reorganisation",
    "Replac*": "Remplacement",
    "Restructur*": "Restructuration",
    "Rework*": "Reecriture / rework",
    "Rewrit*": "Reecriture",
    "Simplif*": "Simplification",
    "Split*": "Decoupage / split",
}


def unique_join(values: pd.Series, sep: str = "\n") -> str:
    """Join non-empty unique values while preserving their first-seen order."""
    cleaned = []
    seen = set()

    for value in values.fillna("").astype(str):
        text = value.strip()
        if not text or text in seen:
            continue
        cleaned.append(text)
        seen.add(text)

    return sep.join(cleaned)


def count_matches(values: pd.Series, expected: str) -> int:
    """Count case-insensitive matches for a categorical column."""
    normalized = values.fillna("").astype(str).str.strip().str.casefold()
    return int((normalized == expected.casefold()).sum())


def extract_repo_full_name(url: str) -> str:
    """Extract owner/repo from a GitHub PR URL."""
    parts = str(url).strip().split("/")
    if len(parts) >= 5 and "github.com" in parts[2]:
        return f"{parts[3]}/{parts[4]}"
    return ""


def extract_pr_number(url: str) -> str:
    """Extract the PR number from a GitHub PR URL."""
    parts = str(url).strip().rstrip("/").split("/")
    if parts and parts[-2:-1] == ["pull"]:
        return parts[-1]
    if "pull" in parts:
        idx = parts.index("pull")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return ""


def split_keywords(text: str) -> list[str]:
    """Split the semicolon-separated keyword field into cleaned labels."""
    return [part.strip() for part in str(text).split(";") if part.strip()]


def classify_keywords(text: str) -> str:
    """Map one or more keyword labels to a compact refactoring category."""
    categories = []
    for keyword in split_keywords(text):
        category = KEYWORD_TO_CATEGORY.get(keyword, "Autre")
        if category not in categories:
            categories.append(category)
    return " | ".join(categories) if categories else "Aucune"


def classify_refactoring_types(text: str) -> str:
    """Map one or more keyword labels to a readable refactoring type."""
    types = []
    for keyword in split_keywords(text):
        refactoring_type = KEYWORD_TO_REFACTORING_TYPE.get(keyword, "Autre / a verifier")
        if refactoring_type not in types:
            types.append(refactoring_type)
    return " | ".join(types) if types else "Aucun type detecte"


def prepare_comment_sheet(df_comments: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns that make the CSV easier to analyze."""
    df = df_comments.copy()
    df["repo_full_name"] = df["html_url"].apply(extract_repo_full_name)
    df["pr_number"] = df["html_url"].apply(extract_pr_number)
    df["is_human_comment"] = df["user_type_inline"].astype(str).str.strip().str.casefold().eq("user")
    df["is_bot_comment"] = df["user_type_inline"].astype(str).str.strip().str.casefold().eq("bot")
    df["primary_keyword"] = df["matched_keywords"].apply(lambda text: split_keywords(text)[0] if split_keywords(text) else "")
    df["refactoring_category"] = df["matched_keywords"].apply(classify_keywords)
    df["refactoring_type"] = df["matched_keywords"].apply(classify_refactoring_types)
    return df


def build_pr_sheet(df_comments: pd.DataFrame) -> pd.DataFrame:
    """Aggregate comment-level rows into a more readable PR-level view."""
    grouped = df_comments.groupby("pr_id", sort=False)

    df_prs = grouped.agg(
        agent=("agent", "first"),
        repo_full_name=("repo_full_name", "first"),
        pr_number=("pr_number", "first"),
        html_url=("html_url", "first"),
        total_comments=("id_inline", "count"),
        human_comments=("user_type_inline", lambda s: count_matches(s, "User")),
        bot_comments=("user_type_inline", lambda s: count_matches(s, "Bot")),
        unique_authors=("user_inline", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
        authors=("user_inline", unique_join),
        user_types=("user_type_inline", unique_join),
        unique_files=("path", lambda s: s.fillna("").astype(str).str.strip().replace("", pd.NA).dropna().nunique()),
        files=("path", unique_join),
        comments_with_keywords=("matched_keywords", lambda s: s.fillna("").astype(str).str.strip().ne("").sum()),
        matched_keywords=("matched_keywords", unique_join),
        categories=("refactoring_category", unique_join),
        type_refactoring=("refactoring_type", unique_join),
    ).reset_index()

    # Keep a compact but readable comment preview per PR.
    preview_rows = df_comments.copy()
    preview_rows["comment_preview"] = (
        "["
        + preview_rows["user_inline"].fillna("").astype(str).str.strip()
        + "] "
        + preview_rows["path"].fillna("").astype(str).str.strip()
        + ": "
        + preview_rows["body_inline"].fillna("").astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    )

    preview_by_pr = (
        preview_rows.groupby("pr_id", sort=False)["comment_preview"]
        .apply(unique_join)
        .rename("comment_previews")
        .reset_index()
    )

    df_prs = df_prs.merge(preview_by_pr, on="pr_id", how="left")
    df_prs["has_keyword_match"] = df_prs["comments_with_keywords"] > 0
    df_prs["pct_human_comments"] = (df_prs["human_comments"] / df_prs["total_comments"] * 100).round(1)
    df_prs["pct_bot_comments"] = (df_prs["bot_comments"] / df_prs["total_comments"] * 100).round(1)

    preferred_order = [
        "pr_id",
        "agent",
        "repo_full_name",
        "pr_number",
        "html_url",
        "total_comments",
        "human_comments",
        "bot_comments",
        "pct_human_comments",
        "pct_bot_comments",
        "comments_with_keywords",
        "has_keyword_match",
        "unique_authors",
        "authors",
        "user_types",
        "unique_files",
        "files",
        "matched_keywords",
        "categories",
        "type_refactoring",
        "comment_previews",
    ]
    return df_prs[preferred_order].sort_values(["agent", "pr_id"]).reset_index(drop=True)


def build_summary_sheet(df_comments: pd.DataFrame, df_prs: pd.DataFrame) -> pd.DataFrame:
    """Create a compact summary block that is easy to scan in Excel."""
    rows = [
        {"section": "Global", "metric": "Nombre de PR uniques", "value": int(df_prs["pr_id"].nunique())},
        {"section": "Global", "metric": "Nombre de commentaires", "value": int(len(df_comments))},
        {
            "section": "Global",
            "metric": "Commentaires avec mot-cle detecte",
            "value": int(df_comments["matched_keywords"].fillna("").astype(str).str.strip().ne("").sum()),
        },
        {"section": "Auteurs", "metric": "Commentaires humains", "value": count_matches(df_comments["user_type_inline"], "User")},
        {"section": "Auteurs", "metric": "Commentaires bots", "value": count_matches(df_comments["user_type_inline"], "Bot")},
    ]

    by_agent = (
        df_prs.groupby("agent", dropna=False)
        .agg(pr_count=("pr_id", "count"), comment_count=("total_comments", "sum"))
        .reset_index()
    )
    for _, row in by_agent.iterrows():
        rows.append({"section": "Par agent", "metric": f"{row['agent']} - PR", "value": int(row["pr_count"])})
        rows.append({"section": "Par agent", "metric": f"{row['agent']} - commentaires", "value": int(row["comment_count"])})

    top_keywords = (
        df_comments["matched_keywords"]
        .fillna("")
        .astype(str)
        .str.split(";")
        .explode()
        .str.strip()
    )
    top_keywords = top_keywords[top_keywords.ne("")]
    for keyword, count in top_keywords.value_counts().head(10).items():
        rows.append({"section": "Top mots-cles", "metric": keyword, "value": int(count)})

    return pd.DataFrame(rows)


def build_agent_sheet(df_prs: pd.DataFrame) -> pd.DataFrame:
    """Agent-level aggregated view for quick comparison."""
    df_agent = (
        df_prs.groupby("agent", dropna=False)
        .agg(
            pr_count=("pr_id", "count"),
            total_comments=("total_comments", "sum"),
            human_comments=("human_comments", "sum"),
            bot_comments=("bot_comments", "sum"),
            comments_with_keywords=("comments_with_keywords", "sum"),
        )
        .reset_index()
    )
    df_agent["avg_comments_per_pr"] = (df_agent["total_comments"] / df_agent["pr_count"]).round(2)
    df_agent["pct_human_comments"] = (df_agent["human_comments"] / df_agent["total_comments"] * 100).round(1)
    df_agent["pct_bot_comments"] = (df_agent["bot_comments"] / df_agent["total_comments"] * 100).round(1)
    return df_agent.sort_values("pr_count", ascending=False).reset_index(drop=True)


def build_part1_analysis_sheet(df_comments: pd.DataFrame, df_prs: pd.DataFrame, df_agents: pd.DataFrame) -> pd.DataFrame:
    """Create a flat worksheet with the main Part 1 indicators."""
    rows = []

    rows.extend(
        [
            {"section": "Vue globale", "dimension": "PR uniques", "value": int(df_prs["pr_id"].nunique())},
            {"section": "Vue globale", "dimension": "Commentaires", "value": int(len(df_comments))},
            {
                "section": "Vue globale",
                "dimension": "Commentaires humains (%)",
                "value": round(df_comments["is_human_comment"].mean() * 100, 1),
            },
            {
                "section": "Vue globale",
                "dimension": "Commentaires bots (%)",
                "value": round(df_comments["is_bot_comment"].mean() * 100, 1),
            },
            {
                "section": "Vue globale",
                "dimension": "PR avec au moins un mot-cle (%)",
                "value": round(df_prs["has_keyword_match"].mean() * 100, 1),
            },
        ]
    )

    for _, row in df_agents.iterrows():
        rows.extend(
            [
                {"section": "Par agent", "dimension": f"{row['agent']} - PR", "value": int(row["pr_count"])},
                {"section": "Par agent", "dimension": f"{row['agent']} - commentaires", "value": int(row["total_comments"])},
                {"section": "Par agent", "dimension": f"{row['agent']} - % humains", "value": float(row["pct_human_comments"])},
                {"section": "Par agent", "dimension": f"{row['agent']} - moyenne comm./PR", "value": float(row["avg_comments_per_pr"])},
            ]
        )

    category_counts = (
        df_comments.loc[df_comments["refactoring_category"].ne("Aucune"), "refactoring_category"]
        .astype(str)
        .str.split(r"\s+\|\s+")
        .explode()
        .value_counts()
    )
    for category, count in category_counts.items():
        rows.append({"section": "Categories", "dimension": category, "value": int(count)})

    return pd.DataFrame(rows)


def build_category_sheet(df_comments: pd.DataFrame) -> pd.DataFrame:
    """Cross-tab categories x agents using the derived keyword categories."""
    exploded = df_comments.copy()
    exploded["refactoring_category"] = exploded["refactoring_category"].astype(str).str.split(r"\s+\|\s+")
    exploded = exploded.explode("refactoring_category").reset_index(drop=True)
    exploded = exploded[exploded["refactoring_category"].notna() & exploded["refactoring_category"].ne("Aucune")]

    if exploded.empty:
        return pd.DataFrame(columns=["refactoring_category"])

    category_agent = (
        pd.crosstab(exploded["refactoring_category"], exploded["agent"])
        .reset_index()
        .sort_values("refactoring_category")
        .reset_index(drop=True)
    )
    return category_agent


def build_pr_types_sheet(df_prs: pd.DataFrame) -> pd.DataFrame:
    """Dedicated per-PR export focused on refactoring type."""
    columns = [
        "pr_id",
        "agent",
        "repo_full_name",
        "pr_number",
        "html_url",
        "has_keyword_match",
        "matched_keywords",
        "categories",
        "type_refactoring",
        "comment_previews",
    ]
    return df_prs[columns].sort_values(["agent", "pr_id"]).reset_index(drop=True)


def autosize_worksheet(worksheet, dataframe: pd.DataFrame) -> None:
    """Adjust column widths to keep the workbook readable."""
    for idx, column in enumerate(dataframe.columns, start=1):
        values = [str(column)] + dataframe[column].fillna("").astype(str).tolist()
        width = min(max(len(value) for value in values) + 2, 80)
        worksheet.column_dimensions[get_column_letter(idx)].width = width
    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = worksheet.dimensions


def write_workbook(output_path: Path, sheets: dict[str, pd.DataFrame]) -> Path:
    """Write the workbook, falling back to a timestamped filename if needed."""
    candidate_paths = [output_path]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate_paths.append(output_path.with_name(f"{output_path.stem}_{timestamp}{output_path.suffix}"))

    last_error = None
    for candidate in candidate_paths:
        try:
            with pd.ExcelWriter(candidate, engine="openpyxl") as writer:
                for sheet_name, dataframe in sheets.items():
                    dataframe.to_excel(writer, sheet_name=sheet_name, index=False)
                    autosize_worksheet(writer.sheets[sheet_name], dataframe)
            return candidate
        except PermissionError as exc:
            last_error = exc
            continue

    raise last_error


def main() -> None:
    df_comments = pd.read_csv(INPUT_CSV, dtype=str, keep_default_na=False)
    df_comments = df_comments.fillna("")
    df_comments = prepare_comment_sheet(df_comments)
    df_comments = df_comments.sort_values(["agent", "pr_id", "id_inline"]).reset_index(drop=True)

    df_prs = build_pr_sheet(df_comments)
    df_summary = build_summary_sheet(df_comments, df_prs)
    df_agents = build_agent_sheet(df_prs)
    df_part1 = build_part1_analysis_sheet(df_comments, df_prs, df_agents)
    df_categories = build_category_sheet(df_comments)
    df_pr_types = build_pr_types_sheet(df_prs)

    output_file = write_workbook(
        OUTPUT_XLSX,
        {
            "PRs": df_prs,
            "Commentaires": df_comments,
            "Resume": df_summary,
            "Agents": df_agents,
            "Analyse_P1": df_part1,
            "Categories": df_categories,
            "Types_PR": df_pr_types,
        },
    )

    print(f"Fichier cree : {output_file.resolve()}")
    print(f"Feuille PRs           : {len(df_prs)} PR uniques")
    print(f"Feuille Commentaires  : {len(df_comments)} commentaires")
    print(f"Feuille Resume        : {len(df_summary)} indicateurs")
    print(f"Feuille Agents        : {len(df_agents)} agents")
    print(f"Feuille Analyse_P1    : {len(df_part1)} indicateurs")
    print(f"Feuille Categories    : {len(df_categories)} categories")
    print(f"Feuille Types_PR      : {len(df_pr_types)} PR typées")


if __name__ == "__main__":
    main()
