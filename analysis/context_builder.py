# analysis/context_builder.py

import pandas as pd
import logging
import json
from datetime import datetime, timedelta
import pytz

from db.database import get_connection
from analysis.entity_extraction import (
    get_entities_for_article,
    get_entities_for_category,
    get_related_entities,
)

logger = logging.getLogger(__name__)


def get_exemplar_groups(category, limit=3, db_path="db/news.db"):
    """
    Retrieve exemplar groups for a given category.
    Returns a dictionary with exemplar information.
    """
    conn = get_connection(db_path)
    query = """
        SELECT
            eg.exemplar_id,
            eg.pattern_name,
            eg.pattern_description,
            eg.success_score
        FROM exemplar_groups eg
        WHERE eg.category = ?
        ORDER BY eg.success_score DESC
        LIMIT ?
    """
    exemplars_df = pd.read_sql_query(query, conn, params=(category, limit))

    result = []
    for _, exemplar in exemplars_df.iterrows():
        exemplar_id = exemplar["exemplar_id"]

        # Get articles for this exemplar
        article_query = """
            SELECT
                a.id AS article_id,
                a.title,
                a.content
            FROM articles a
            JOIN exemplar_articles ea ON a.id = ea.article_id
            WHERE ea.exemplar_id = ?
            LIMIT 3
        """
        articles_df = pd.read_sql_query(article_query, conn, params=(exemplar_id,))

        articles = []
        for _, article in articles_df.iterrows():
            # Get summary instead of full content
            content = article["content"]
            if content and len(content) > 500:
                content = content[:500] + "..."

            articles.append(
                {
                    "article_id": article["article_id"],
                    "title": article["title"],
                    "content": content,
                }
            )

        result.append(
            {
                "exemplar_id": exemplar_id,
                "pattern_name": exemplar["pattern_name"],
                "pattern_description": exemplar["pattern_description"],
                "success_score": exemplar["success_score"],
                "articles": articles,
            }
        )

    conn.close()
    return result


def get_recent_category_groups(category, days=7, limit=5, db_path="db/news.db"):
    """
    Get recent successful groupings within a category.
    Returns a list of dictionaries with group information.
    """
    conn = get_connection(db_path)
    cutoff_time = (datetime.now(pytz.UTC) - timedelta(days=days)).isoformat()

    query = """
        SELECT
            tg.group_id,
            tg.main_topic,
            tg.sub_topic,
            tg.group_label,
            tg.description,
            tg.consistency_score,
            COUNT(tgm.article_id) AS article_count
        FROM two_phase_article_groups tg
        JOIN two_phase_article_group_memberships tgm ON tg.group_id = tgm.group_id
        WHERE tg.main_topic = ?
          AND tg.created_at >= ?
          AND tg.consistency_score >= 0.7
        GROUP BY tg.group_id
        ORDER BY tg.consistency_score DESC, article_count DESC
        LIMIT ?
    """
    groups_df = pd.read_sql_query(query, conn, params=(category, cutoff_time, limit))

    result = []
    for _, group in groups_df.iterrows():
        group_id = group["group_id"]

        # Get example articles for this group
        article_query = """
            SELECT
                a.id AS article_id,
                a.title
            FROM articles a
            JOIN two_phase_article_group_memberships tgm ON a.id = tgm.article_id
            WHERE tgm.group_id = ?
            LIMIT 3
        """
        articles_df = pd.read_sql_query(article_query, conn, params=(group_id,))

        # Get key entities for this group
        entity_query = """
            SELECT
                e.entity_id,
                e.entity_name,
                e.entity_type,
                ge.relevance_score
            FROM entity_profiles e
            JOIN group_entities ge ON e.entity_id = ge.entity_id
            WHERE ge.group_id = ?
            ORDER BY ge.relevance_score DESC
            LIMIT 5
        """
        entities_df = pd.read_sql_query(entity_query, conn, params=(group_id,))

        result.append(
            {
                "group_id": group_id,
                "main_topic": group["main_topic"],
                "sub_topic": group["sub_topic"],
                "group_label": group["group_label"],
                "description": group["description"],
                "consistency_score": group["consistency_score"],
                "article_count": group["article_count"],
                "example_articles": articles_df.to_dict(orient="records"),
                "key_entities": entities_df.to_dict(orient="records"),
            }
        )

    conn.close()
    return result


def build_grouping_context(articles_dict, category, api_key=None, db_path="db/news.db"):
    """
    Build rich context for grouping decisions.

    Args:
        articles_dict: Dictionary of {article_id: article_content}
        category: The main category to focus on
        api_key: API key for LLM (not used here but kept for consistency)
        db_path: Path to the database

    Returns:
        A context object with information to help the LLM make better grouping decisions
    """
    # Get key entities for all articles in the batch
    all_entities = {}
    for article_id in articles_dict.keys():
        entities_df = get_entities_for_article(article_id, db_path=db_path)
        all_entities[article_id] = entities_df.to_dict(orient="records")

    # Get the most relevant entities for this category
    category_entities = get_entities_for_category(category, limit=10, db_path=db_path)

    # Get exemplar groups for this category
    exemplars = get_exemplar_groups(category, limit=3, db_path=db_path)

    # Get recent successful groupings
    recent_groups = get_recent_category_groups(
        category, days=7, limit=5, db_path=db_path
    )

    # Build the context object
    context = {
        "category": category,
        "timestamp": datetime.now().isoformat(),
        "article_count": len(articles_dict),
        "article_entities": all_entities,
        "category_entities": category_entities.to_dict(orient="records"),
        "exemplars": exemplars,
        "recent_groups": recent_groups,
    }

    return context


def format_context_for_prompt(context, max_length=4000):
    """
    Format the context object into a string to include in an LLM prompt.
    Limits the length to avoid excessively large prompts.
    """
    category = context["category"]

    output = [
        f"CONTEXT FOR GROUPING ARTICLES IN CATEGORY: {category}\n",
        f"Number of articles to group: {context['article_count']}\n",
    ]

    # Add key entities for the category
    if context["category_entities"]:
        output.append("\nKEY ENTITIES IN THIS CATEGORY:")
        for entity in context["category_entities"][:5]:  # Limit to top 5
            output.append(
                f"- {entity['entity_name']} ({entity['entity_type']}): Mentioned in {entity['article_count']} articles"
            )

    # Add exemplars
    if context["exemplars"]:
        output.append("\nEXEMPLAR GROUPING PATTERNS:")
        for ex in context["exemplars"]:
            output.append(f"- {ex['pattern_name']}: {ex['pattern_description']}")
            if ex["articles"]:
                output.append("  Example articles:")
                for art in ex["articles"][:2]:  # Limit to 2 examples
                    output.append(f"  * {art['title']}")

    # Add recent successful groups
    if context["recent_groups"]:
        output.append("\nRECENT SUCCESSFUL GROUPINGS:")
        for group in context["recent_groups"][:3]:  # Limit to top 3
            output.append(
                f"- {group['group_label']} (Score: {group['consistency_score']:.2f})"
            )
            if group["key_entities"]:
                entities_str = ", ".join(
                    [e["entity_name"] for e in group["key_entities"][:3]]
                )
                output.append(f"  Key entities: {entities_str}")

    result = "\n".join(output)

    if len(result) > max_length:
        # Truncate if too long
        return result[:max_length] + "...\n[Context truncated due to length]"

    return result
