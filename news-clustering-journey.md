# The Journey to News Article Clustering: A Technical Evolution

## Table of Contents
- [Overview](#overview)
- [1. Early Approaches: Direct LLM Comparison](#1-early-approaches-direct-llm-comparison)
- [2. Traditional Embedding + Clustering Approaches](#2-traditional-embedding--clustering-approaches)
  - [A. Doc2Vec Embeddings (Initial Attempt)](#a-doc2vec-embeddings-initial-attempt)
  - [B. Sentence Transformers with Various Clustering Algorithms](#b-sentence-transformers-with-various-clustering-algorithms)
  - [C. Semi-Supervised Affinity Propagation](#c-semi-supervised-affinity-propagation)
- [3. Deep Learning Approaches](#3-deep-learning-approaches)
- [4. LLM-Based Direct Clustering](#4-llm-based-direct-clustering)
- [5. Storage and Infrastructure Evolution](#5-storage-and-infrastructure-evolution)
- [6. Supporting Technologies and Tools](#6-supporting-technologies-and-tools)
- [7. Methods That Didn't Make the Final Cut](#7-methods-that-didnt-make-the-final-cut)
- [8. Key Insights from Testing](#8-key-insights-from-testing)
- [9. Final Solution: Article Signature-Based Grouping](#9-final-solution-article-signature-based-grouping)
- [10. Why This Journey Mattered](#10-why-this-journey-mattered)

## Overview

This document chronicles the extensive journey of developing an effective news article clustering system. What started as a simple clustering problem evolved into a sophisticated hybrid approach combining multiple AI technologies, custom algorithms, and deep understanding of news content structure.

## 1. Early Approaches: Direct LLM Comparison

### The Naive Beginning
- **Approach**: Direct pairwise comparison of articles using llama3.1:8b
- **Method**: Each article was compared to every other article directly through LLM prompts
- **Process**: 
  1. Take article A and article B, ask LLM "How similar are these articles?" 
  2. Receive similarity score (0.0-1.0)
  3. If similarity > threshold, group them together
  4. When article C arrives, compare it to both A and B individually
  5. Continue this process for all articles
- **Result**: O(n²) complexity - ~250,000 API calls for 700 articles
- **Learning**: Fundamental scalability issues with pure LLM comparison

```python
# Actual implementation approach
for i, article_a in enumerate(articles):
    for j, article_b in enumerate(articles[i+1:], i+1):
        similarity_score = llm_compare(article_a, article_b)
        if similarity_score > 0.7:  # threshold
            group_articles(article_a, article_b)

# When new article arrives:
for existing_article in all_articles:
    similarity = llm_compare(new_article, existing_article)
    # Decide grouping based on similarity
```

**Why it failed**: With 700 articles, this required (700 × 699) / 2 = 244,650 individual LLM API calls. Each new article added required comparing it to all existing articles, making the approach completely unscalable for real-time news processing.

## 2. Traditional Embedding + Clustering Approaches

### A. Doc2Vec Embeddings (Initial Attempt)
- **Technology**: Doc2Vec neural network
- **Issue**: Lacked semantic understanding compared to modern transformers
- **Performance**: Poor clustering quality on news articles
- **Verdict**: Outdated technology for this use case

### B. Sentence Transformers with Various Clustering Algorithms

#### Embedding Models Tested:
1. `all-MiniLM-L6-v2` (384 dimensions)
2. `all-mpnet-base-v2` (768 dimensions)
3. `paraphrase-multilingual:latest` (via Ollama)
4. `nomic-embed-text:latest` (via Ollama)
5. `gte-large-en-v1.5` (Qwen model)

#### Clustering Algorithms Explored:

**1. DBSCAN (Density-Based Spatial Clustering)**
```python
def cluster_dbscan(embeddings, eps=1.2, min_samples=3):
    dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='cosine')
    labels = dbscan.fit_predict(embeddings)
    return labels
```
- **Parameters tested**: eps: [0.3, 0.4, 0.7, 1.2], min_samples: [3, 5]
- **Issues**: Parameter sensitivity, too many noise points
- **Result**: Inconsistent and unreliable

**2. HDBSCAN (Hierarchical DBSCAN)**
- **Advantages**: Better density handling, automatic cluster detection
- **Challenges**: Still required significant tuning
- **Performance**: Better than DBSCAN but still lacking

**3. Affinity Propagation** ⭐ Best Traditional Method
```python
def cluster_affinity(embeddings, damping=0.9, preference=None):
    distances = pairwise_distances(embeddings, metric='cosine')
    similarity = -distances
    ap = AffinityPropagation(
        random_state=42,
        damping=damping,
        preference=preference,
        affinity='precomputed'
    )
    ap_labels = ap.fit_predict(similarity)
    return ap_labels
```
- **Key Strengths**: 
  - No predefined cluster count
  - Message passing architecture
  - Best results among traditional methods
- **Parameters**: damping (0.5-1.0), preference (cluster control)

**4. Other Algorithms Tested**:
- K-Medoids: Poor with overlapping topics
- K-means: Sensitive to initialization
- Agglomerative Clustering: Computationally intensive
- Spectral Clustering: Poor on news data
- Gaussian Mixture Models: Failed on article relationships
- Custom Graph-based Clustering: Failed to produce meaningful clusters

### C. Semi-Supervised Affinity Propagation
```python
def cluster_affinity_semisupervised(embeddings, df, damping=0.9, preference=None):
    # Modify similarity matrix with constraints
    # BIG_POS (2.0): Force same-labeled articles together
    # BIG_NEG (-2.0): Force different-labeled articles apart
    # BOOST_DIAG (2.0): Make labeled articles exemplars
```
- **Innovation**: Incorporated human constraints
- **Challenge**: Required manual labeling effort
- **Outcome**: Improved accuracy but not scalable

## 3. Deep Learning Approaches

### Deep Embedded Clustering (DEC)
**Architecture**:
1. TF-IDF vectorization
2. Deep Autoencoder for dimensionality reduction
3. Custom ClusteringLayer for soft assignments
4. End-to-end training (reconstruction + clustering loss)

**Visualization**: UMAP and Plotly

**Challenges**:
- Complex implementation
- Sensitive hyperparameters
- Extensive training requirements
- Overkill for the task

**Verdict**: Theoretically elegant but practically complex

## 4. LLM-Based Direct Clustering

### A. Single-Step Grouping
```python
def generate_grouping(summaries_dict, api_key):
    # Single prompt to group articles
    # Returns: main_topic, sub_topic, group_label, articles
```
**Limitation**: 70,000 token constraint

### B. Two-Phase Grouping
**Phase 1**: Generate topic labels
**Phase 2**: Assign articles to labels

**Improvement**: More structured but still limited by tokens

### C. Chunked Processing
```python
def chunk_summaries(summaries_dict, max_token_chunk=70000):
    # Break articles into manageable chunks
    # Approximate tokens as words * 1.3
```
**Solution**: Necessary for large datasets but inconsistent across chunks

## 5. Storage and Infrastructure Evolution

### Embedding Storage Evolution:
1. **TEXT/JSON Approach**: Inefficient string storage
2. **BLOB Storage**: Binary numpy serialization ✓
3. **Vector Database Considerations**: pgvector, FAISS, Pinecone

### Ollama Integration Issues:
- **Problem**: Direct HTTP POST returned empty responses
- **Solution**: Official Ollama Python library
```python
import ollama
embedding = ollama.embeddings(model='model_name', prompt=text)
```

## 6. Supporting Technologies and Tools

### NLP and Feature Extraction:
- **spaCy**: Named Entity Recognition
- **KeyBERT**: Keyword extraction (considered)
- **c-TF-IDF**: Topic representation in BERTopic
- **BeautifulSoup**: HTML cleaning

### Visualization and Analysis:
- **UMAP**: Dimensionality reduction
- **Plotly**: Interactive visualizations
- **NetworkX**: Graph analysis
- **Matplotlib**: Static visualizations

### Database and Storage:
- **SQLite**: Primary database
- **Pandas**: Data manipulation
- **xlsxwriter**: Excel export

### Time and Scheduling:
- **PyTZ**: Timezone conversions
- **Python datetime**: Time filtering
- **Scheduled execution**: 15-minute intervals

## 7. Methods That Didn't Make the Final Cut

### Topic Modeling:
- LDA (Latent Dirichlet Allocation)
- NMF (Non-negative Matrix Factorization)
- BERTopic
- Top2Vec

**Why rejected**: Focused on topics rather than article relationships

### Alternative Embeddings:
- NewsBERT (news-specific)
- Universal Sentence Encoder
- Custom fine-tuned models

**Why rejected**: Resource intensive, marginal improvements

### Advanced Clustering:
- Mean Shift
- OPTICS
- Hierarchical clustering with Ward linkage

**Why rejected**: Similar issues to other density-based methods

## 8. Key Insights from Testing

### Why Traditional Clustering Failed:
1. High-dimensional embeddings don't form natural clusters
2. News articles have overlapping topics
3. Distance metrics miss nuanced relationships
4. Hard clustering doesn't capture article nuances

### Why Affinity Propagation Was Best:
1. No predefined cluster count
2. Message passing captures relationships
3. Works well with cosine similarity
4. Handles varying cluster sizes

### Limitations of Best Traditional Method:
1. Pure mathematical similarity insufficient
2. Can't distinguish contextual differences
3. No semantic understanding
4. No temporal relationship consideration

## 9. Final Solution: Article Signature-Based Grouping

### Core Innovation: Article Signatures
```python
def generate_article_signature(article_id: int, db_path: str = "db/news.db") -> Dict[str, Any]:
    signature = {
        "article_id": article_id,
        "published_date": published_date_str,
        "source": source,
        "primary_entities": primary_entities,  # With relevance scores
        "companies": companies,
        "cves": cves,
        "technologies": technologies,
        "products": products,
        "references": references,
        "events": events,
        "quotes": quotes,
        "author": author
    }
    return signature
```

### Multi-Dimensional Similarity:
```python
# Weighted composite scoring
weights = {
    "entity_similarity": 0.40,
    "company_similarity": 0.25,
    "cve_similarity": 0.15,
    "event_similarity": 0.10,
}
```

### Dynamic Thresholds:
```python
DYNAMIC_THRESHOLD_RULES = {
    'base': 0.40,
    'category_adjust': {
        'Cybersecurity & Data Privacy': +0.05,
        'Artificial Intelligence & Machine Learning': +0.03,
        'Other': -0.03
    },
    'size_adjust': {
        'breakpoints': [1, 5, 10],
        'adjustments': [+0.05, 0.0, -0.03, -0.05]
    }
}
```

### LLM Assistance for Edge Cases:
- Ambiguity zone detection
- Semantic understanding for difficult cases
- Context-aware new group creation

## 10. Why This Journey Mattered

### Lessons Learned:
1. **Pure mathematical approaches insufficient**: News requires semantic understanding
2. **Embeddings alone aren't enough**: Structured features crucial
3. **Hybrid approaches win**: Combine best of traditional and AI methods
4. **Context matters**: Temporal, source, and entity relationships critical
5. **Dynamic adaptation essential**: One-size-fits-all fails

### Evolution Summary:
1. **Naive LLM comparison** → Too expensive
2. **Traditional clustering** → Affinity Propagation best, but limited
3. **Deep learning** → Too complex
4. **Direct LLM clustering** → Token limitations
5. **Hybrid signature-based** → Optimal solution

### Final Architecture Success Factors:
- Structured feature extraction
- Multi-dimensional similarity scoring
- Dynamic threshold adaptation
- LLM intelligence for edge cases
- Context-aware decision making
- Continuous learning and adaptation

This journey from simple clustering to sophisticated hybrid AI demonstrates the importance of iterative development and willingness to combine multiple approaches to solve complex real-world problems.
