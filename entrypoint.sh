#!/bin/bash
# Complete entrypoint.sh for news application

echo "Starting application..."

# Check if API key is provided
if [ -z "$OPENAI_API_KEY" ]; then
  echo "WARNING: OPENAI_API_KEY environment variable is not set."
  echo "The application may not function correctly without it."
else
  echo "API key is configured."
fi

# Ensure database directory exists
mkdir -p /app/db
echo "Database directory created/verified"

# Step 1: Create database schema
echo "Setting up database schema..."
python -c "from news_grouping_app.db.database import setup_database; setup_database()"
echo "Database schema setup complete"

# Step 2: Start scrapers in the background
echo "Starting scrapers in the background..."
python -m news_grouping_app.main &
SCRAPER_PID=$!
echo "Scrapers started with PID: $SCRAPER_PID"

# Step 3: Start Flask app in the foreground
echo "Starting Flask app in the foreground..."
python -m news_grouping_app.app

# If Flask app exits, kill the scraper process
kill $SCRAPER_PID
