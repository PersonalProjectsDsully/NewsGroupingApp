"""
utils.py

Contains generic helper functions (hashing, token counting, chunking, etc.)
and the CVE regex from the original code.
"""

import re
import hashlib

# Increase max token chunk size for faster processing
MAX_TOKEN_CHUNK = 100000  # Increased from 70k to 100k tokens
CVE_REGEX = r"\bCVE-\d{4}-\d{4,7}\b"


def approximate_tokens(text: str) -> int:
    """Roughly estimate tokens by counting words and multiplying by ~1.3."""
    return int(len(text.split()) * 1.3)


def chunk_summaries(summaries_dict, max_token_chunk=MAX_TOKEN_CHUNK):
    """
    Splits article summaries into chunks without exceeding max_token_chunk.
    If a single article alone exceeds max_token_chunk, yield it alone.

    Optimized to create larger, more efficient chunks.
    """
    current_chunk = {}
    current_tokens = 0

    # Sort articles by length (shortest first) to optimize packing
    sorted_items = sorted(summaries_dict.items(), key=lambda x: len(x[1]))

    for link, summary in sorted_items:
        tokens_for_article = approximate_tokens(summary)

        # If this single article exceeds chunk limit, yield it alone
        if tokens_for_article > max_token_chunk:
            # First yield any accumulated items
            if current_chunk:
                yield current_chunk
                current_chunk = {}
                current_tokens = 0
            # Then yield this large article alone
            yield {link: summary}
            continue

        # If adding this article would exceed the limit, yield current chunk first
        if current_tokens + tokens_for_article > max_token_chunk:
            if current_chunk:
                yield current_chunk
                current_chunk = {}
                current_tokens = 0

        # Add this article to the current chunk
        current_chunk[link] = summary
        current_tokens += tokens_for_article

    # Yield any remaining articles
    if current_chunk:
        yield current_chunk


def extract_cves(text: str):
    """Extract a set of unique CVE numbers from the provided text."""
    return set(re.findall(CVE_REGEX, text))
