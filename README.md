# News Article Clustering System

A real-time news aggregation and clustering system that automatically groups related tech articles using AI-powered analysis.

[My Hosted Demo](https://dsecuritynews.com/)


## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the System](#running-the-system)
- [Docker Deployment](#docker-deployment)
- [Available Categories](#available-categories)
- [Scrapers](#scrapers)
- [API Endpoints](#api-endpoints)
- [Frontend Structure](#frontend-structure)
- [Analysis Pipeline](#analysis-pipeline)
- [Database Schema](#database-schema)
- [Troubleshooting](#troubleshooting)

## Overview

This system scrapes news articles from various tech sources, analyzes them using OpenAI's language models, and groups related articles into topics. It provides a modern React-based web interface for browsing categorized news clusters.

## Architecture

The system consists of:
- **Backend**: Python Flask API with data collection and analysis pipelines
- **Frontend**: React application with Tailwind CSS
- **Database**: SQLite for data storage
- **Analysis**: GPT-based entity extraction, article grouping, and trending analysis

## Prerequisites

### Required Software
- Python 3.9+
- Node.js 18+
- Docker (optional, for containerized deployment)

### Required API Keys
- OpenAI API key
- Note with the sources I used, in April this used ~280 million tokens.

![image](https://github.com/user-attachments/assets/0ab15c5e-41b3-424b-9343-78714be08b8b)

## Installation

### Local Development

1. **Clone the repository**
```bash
git clone https://github.com/PersonalProjectsDsully/NewsGroupingApp.git
cd news_app
```

2. **Set up Python environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. **Set up frontend**
```bash
cd frontend
npm install
npm run build
cd ..
```

4. **Create database directory**
```bash
mkdir -p db
```

5. **Set environment variables**
```bash
export OPENAI_API_KEY="your_openai_api_key_here"
```

## Configuration

### Models
The system uses different OpenAI models for various tasks. Configure them in your code as needed:
- Main grouping: `o3-mini`
- Entity extraction: `gpt-4o`
- Trending analysis: `o3-mini`

### Time Zone
Default timezone is US Eastern Time. This can be configured in the application code.

## Running the System

### Local Development

1. **Start the backend scraper and analysis pipeline**
```bash
python main.py
```

2. **Start the Flask API server**
```bash
python app.py
```

The web interface will be available at `http://localhost:8501`

### Docker Deployment

1. **Build the Docker image**
```bash
docker-compose build
```

2. **Run the container**
```bash
# With API key in docker-compose.yml
docker-compose up

# Or pass API key via environment variable
OPENAI_API_KEY=your_api_key_here docker-compose up
```

3. **Build and run in one command**
```bash
docker-compose up --build
```

## Available Categories

The system organizes articles into the following categories:
- Artificial Intelligence & Machine Learning
- Cybersecurity & Data Privacy
- Software Development & Open Source
- Enterprise Technology & Cloud Computing
- Business, Finance & Trade
- Consumer Technology & Gadgets
- Automotive, Space & Transportation
- Science & Environment
- Politics & Government
- Other

## Scrapers

The system includes scrapers for multiple tech news sources:
- BleepingComputer
- Krebs on Security
- NIST
- CyberScoop
- The Register
- Schneier on Security
- Hacker News
- Securelist
- Slashdot
- Sophos
- TechCrunch
- Neowin
- TechRadar
- Dark Reading

Each scraper is implemented as a Python module in the `scrapers/` directory.

## API Endpoints

### Article Groups
- `GET /api/home_groups` - Get top groups for all categories
- `GET /api/category_groups?category=<name>&hours=<number>` - Get groups for specific category
- `GET /api/<category>_groups` - Category-specific endpoints (e.g., `/api/ai_machine_learning_groups`)

### Trending & Analytics
- `GET /api/trending?category=<name>&hours=<number>` - Get trending topics
- `GET /api/trending_entities?hours=<number>&limit=<number>` - Get trending entities
- `GET /api/category_entities?category=<name>&limit=<number>` - Get entities for a category

### CVE Data
- `GET /api/cve_table?hours=<number>` - Get CVE mentions and details

### Debugging & Testing
- `GET /api/debug/date_format` - Check date formatting
- `GET /api/prompt_tester/articles` - Get recent articles for testing
- `POST /api/prompt_tester/test_prompt` - Test grouping prompts

## Frontend Structure

The React frontend is built with:
- React Router for navigation
- Tailwind CSS for styling
- Radix UI components
- Lucide React icons

### Key Components
- `App.js` - Main routing setup
- `pages/` - Page components for each category
- `components/` - Reusable UI components

## Analysis Pipeline

The system performs three main analysis phases:

1. **Entity Extraction**: Identifies companies, people, technologies, and CVEs
2. **Article Grouping**: Uses two-phase similarity analysis to group related articles
3. **Trending Analysis**: Identifies trending topics and emerging patterns

## Database Schema

### Key Tables
- `articles` - Scraped articles with metadata
- `two_phase_article_groups` - Article group definitions
- `two_phase_article_group_memberships` - Article-to-group mappings
- `entity_profiles` - Extracted entities
- `article_entities` - Article-to-entity relationships
- `trending_groups` - Trending topic definitions
- `article_cves` - CVE mentions in articles
- `cve_info` - Detailed CVE information

## AI Models & Assistants

### Claude 3.7 (Anthropic)
- **Primary Use**: Frontend development and system architecture visualization
- **Key Contributions**:
  - Developed the entire frontend interface
  - Created Mermaid diagrams for data flow and system architecture
  - Designed the category-based navigation system
  - Implemented responsive CSS with dark mode support

### OpenAI o1-pro
- **Primary Use**: Core algorithm development and architecture design
- **Note**: Heavily used throughout the project for complex problem-solving

### OpenAI GPT-4o
- **Primary Use**: Code generation and implementation

### OpenAI o3-mini (High)
- **Primary Use**: Production runtime decisions
- **Key Contributions**:
  - Powers the production article grouping decisions
  - Handles ambiguous case resolution
  - Generates group labels and descriptions
  - Performs entity extraction in production
  - Makes real-time clustering decisions

### Google Gemini Models (via AI Studio)
- **Primary Use**: Testing with long context windows
- **Note**: Utilized for processing large code updates and modifications

## Development Tools

### Repomix (VS Code Extension)
- **Purpose**: Code organization and export
- **Key Features**:
  - Exports entire codebase to a single file
  - Preserves directory structure
  - Facilitates sharing complete context with AI assistants
  - Automatically excludes unnecessary files
- **Why It Was Essential**:
  - Enabled comprehensive code reviews with AI
  - Allowed full project context sharing
  - Simplified cross-file refactoring discussions

## Troubleshooting

### Common Issues

1. **Database Lock Errors**
   - Solution: The system handles database locking by using proper cursor management
   - Try increasing the database timeout if issues persist

2. **Date Format Inconsistencies**
   - The system includes date migration to standardize to ISO format
   - Check `/api/debug/date_format` for date validation

3. **API Key Issues**
   - Ensure OPENAI_API_KEY is set in environment
   - For Docker, check docker-compose.yml or pass via command line

4. **Frontend Build Issues**
   - Ensure all npm dependencies are installed
   - Check for proper Node.js version (18+)
   - Frontend build output should be in `frontend_build/` directory

### Logs
Monitor application logs for detailed error information:
```bash
# For local development
python main.py  # Check console output
python app.py   # Check Flask server logs

# For Docker
docker-compose logs -f
```

### Known Problems

- Mitre API doesn't always have information on CVEs, NIST Could be used as a backup, though this is not implemented in prod.
- Date Filtering is not perfect when converting dates to EST. 
- Converting dates to standard format works the majority of the time, if multiple dates are scraped from the fields, it will not work properly.
- When article links are updated, it does not detect that it is a duplicate article.
- Companies will ban you from scraping there websites, just take note of this.

## Contributing

Feel free to submit issues or pull requests to improve the system.
