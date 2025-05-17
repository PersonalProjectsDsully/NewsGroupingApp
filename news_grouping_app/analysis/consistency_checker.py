# analysis/consistency_checker.py

import logging
import json
import pandas as pd
import re
from datetime import datetime, timedelta
import pytz

from news_grouping_app.db.database import get_connection, create_exemplar, add_article_to_exemplar
from news_grouping_app.llm_calls import call_gpt_api
from news_grouping_app.utils import chunk_summaries, MAX_TOKEN_CHUNK
from news_grouping_app.analysis.context_builder import build_grouping_context, format_context_for_prompt
from news_grouping_app.analysis.entity_extraction import get_entities_for_article

logger = logging.getLogger(__name__)
MODEL = "o3-mini"  # or whichever model you prefer


def get_recent_groups_for_category(category, days=30, db_path="db/news.db"):
    """
    Get successful groupings from the past for a specific category.
    Returns a DataFrame with group information.
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
            GROUP_CONCAT(tgm.article_id) AS article_ids,
            COUNT(tgm.article_id) AS article_count
        FROM two_phase_article_groups tg
        JOIN two_phase_article_group_memberships tgm ON tg.group_id = tgm.group_id
        WHERE tg.main_topic = ?
            AND strftime('%s', tg.created_at) >= strftime('%s', ?)
            AND tg.consistency_score >= 0.7
        GROUP BY tg.group_id
        ORDER BY tg.consistency_score DESC, article_count DESC
        LIMIT 10
    """

    df = pd.read_sql_query(query, conn, params=(category, cutoff_time))

    # Process article_ids
    if not df.empty:
        df["article_ids"] = df["article_ids"].apply(
            lambda x: [int(aid) for aid in x.split(",") if aid and aid.isdigit()]
        )

    conn.close()
    return df


def evaluate_group_consistency(group_data, api_key, db_path="db/news.db"):
    """
    Evaluate the consistency of a newly formed group.
    Returns a dict with consistency score and feedback.
    """
    category = group_data.get("main_topic", "")
    group_label = group_data.get("group_label", "")
    description = group_data.get("description", "")
    article_ids = group_data.get("articles", [])

    if not category or not article_ids:
        return {
            "consistency_score": 0.5,  # Default neutral score
            "feedback": "Insufficient data to evaluate consistency",
        }

    # Get historical groups for comparison
    historical_groups = get_recent_groups_for_category(
        category, days=30, db_path=db_path
    )

    # Get article summaries
    conn = get_connection(db_path)
    article_data = {}

    placeholders = ",".join("?" for _ in article_ids)
    query = f"""
        SELECT
            id AS article_id,
            title,
            content
        FROM articles
        WHERE id IN ({placeholders})
    """

    articles_df = pd.read_sql_query(query, conn, params=article_ids)
    conn.close()

    # Create summaries dict for context building
    summaries_dict = {}
    for _, row in articles_df.iterrows():
        art_id = row["article_id"]
        content = f"{row['title']} - {row['content']}"
        summaries_dict[art_id] = content

        # Also store in article_data for the evaluation prompt
        article_data[art_id] = {
            "title": row["title"],
            "content": (
                row["content"][:500] + "..."
                if len(row["content"]) > 500
                else row["content"]
            ),
        }

    # Build context
    context = build_grouping_context(summaries_dict, category, api_key, db_path)

    # Prepare historical group data for the prompt
    historical_data = []
    for _, group in historical_groups.iterrows():
        historical_data.append(
            {
                "group_label": group["group_label"],
                "description": group["description"],
                "consistency_score": group["consistency_score"],
                "article_count": group["article_count"],
            }
        )

    # IMPROVED CONSISTENCY EVALUATION PROMPT
    prompt = (
        f"Evaluate the consistency of this newly formed article group in the '{category}' category.\n\n"
        f"GROUP INFORMATION:\n"
        f"Group Label: {group_label}\n"
        f"Description: {description}\n"
        f"Category: {category}\n"
        f"Article Count: {len(article_ids)}\n\n"
        f"ARTICLES IN THIS GROUP:\n"
    )

    # Add articles with their entity data
    for art_id, data in article_data.items():
        # Get entities for this article
        entities_df = get_entities_for_article(art_id, db_path=db_path)
        entity_text = ""
        if not entities_df.empty:
            top_entities = entities_df.sort_values(
                "relevance_score", ascending=False
            ).head(5)
            entity_text = "\nKey entities: " + ", ".join(
                f"{row['entity_name']} ({row['entity_type']})"
                for _, row in top_entities.iterrows()
            )

        prompt += f"Article {art_id}: {data['title']}{entity_text}\n"

    # Add historical group data
    if historical_data:
        prompt += "\nRECENT SUCCESSFUL GROUPS IN THIS CATEGORY:\n"
        for group in historical_data:
            prompt += (
                f"- {group['group_label']} (Score: {group['consistency_score']}): "
                f"{group['description'][:100]}...\n"
            )

    # IMPROVED EVALUATION CRITERIA
    prompt += (
        "\nIMPORTANT EVALUATION GUIDELINES:\n"
        "1. Focus on the CORE EVENT/INCIDENT: Do all articles cover the same fundamental event, even if they emphasize different aspects?\n"
        "2. Look beyond narrative style: Different publishers may frame the same news differently (e.g., focusing on the company's apology vs. user frustration).\n"
        "3. Consider shared entities: Articles sharing key companies, products, or individuals are likely covering the same story.\n"
        "4. Temporal relationships matter: Articles may cover different stages of the same evolving story (announcement, reaction, aftermath).\n"
        "5. Look for causal relationships: An article about 'impact of X' belongs with articles about 'X happening'.\n\n"
        "Examples of articles that SHOULD be grouped together despite different emphasis:\n"
        "- 'Microsoft apologizes for removing VS Code extensions' + 'VS Code developers frustrated after removal of extensions'\n"
        "- 'CloudProvider A suffers outage' + 'Financial impacts of yesterday's major cloud disruption'\n\n"
        "Please evaluate the group, focusing on whether the articles cover the same core event/incident rather than just surface similarities.\n\n"
        "Return only JSON with this format:\n"
        "{\n"
        '  "consistency_score": 0.1-1.0,\n'
        '  "feedback": "Detailed feedback about group consistency",\n'
        '  "recommended_changes": {\n'
        '    "remove_articles": [article_ids that don\'t fit],\n'
        '    "suggested_label": "Better label if needed",\n'
        '    "suggested_description": "Better description if needed"\n'
        "  },\n"
        '  "exemplar_worthy": true/false\n'
        "}\n"
    )

    messages = [
        {
            "role": "system",
            "content": (
                "You are an expert evaluator of article groupings. Your goal is to ensure "
                "that article groups are consistent and coherent. Articles describing the same core incident "
                "should be grouped together, even when they emphasize different aspects or use different language."
            ),
        },
        {"role": "user", "content": prompt},
    ]

    response = call_gpt_api(messages, api_key)
    if not response:
        logger.warning(
            f"No response from GPT for consistency evaluation of group '{group_label}'"
        )
        return {
            "consistency_score": 0.5,  # Default neutral score
            "feedback": "Could not evaluate consistency due to API error",
        }

    # Parse the response
    cleaned = response.strip().strip("```")
    cleaned = re.sub(r"^json\s+", "", cleaned, flags=re.IGNORECASE)
    try:
        data = json.loads(cleaned)

        # If this group is worthy of being an exemplar, save it
        if (
            data.get("exemplar_worthy", False)
            and data.get("consistency_score", 0) >= 0.8
        ):
            try:
                exemplar_id = create_exemplar(
                    category=category,
                    pattern_name=f"Exemplar: {group_label}",
                    pattern_description=description,
                    success_score=data.get("consistency_score", 0.8),
                    db_path=db_path,
                )

                # Add articles to the exemplar
                for art_id in article_ids:
                    add_article_to_exemplar(exemplar_id, art_id, db_path=db_path)

                logger.info(
                    f"Created exemplar {exemplar_id} for group with label '{group_label}'"
                )
            except Exception as e:
                logger.error(f"Error creating exemplar for group '{group_label}': {e}")

        return data
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing consistency evaluation JSON: {e}\n{cleaned}")
        return {
            "consistency_score": 0.5,  # Default neutral score
            "feedback": "Could not parse consistency evaluation",
        }
