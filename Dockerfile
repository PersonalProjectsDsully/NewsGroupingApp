# ---------------------------------------------------
# 1) Build the React front end
# ---------------------------------------------------
FROM node:18 AS frontend-builder

WORKDIR /app

# Copy package files and install dependencies
COPY frontend/package*.json ./
# Install all dependencies defined in package.json and package-lock.json
RUN npm ci > /app/build_log.txt 2>&1

# Copy the entire frontend source code
COPY frontend/ ./

# Build the React application
RUN npm run build --verbose


# ---------------------------------------------------
# 2) Build the Python backend image
# ---------------------------------------------------
FROM python:3.9-slim

WORKDIR /app

# Copy Python dependencies and install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the *built React files* from the builder stage
COPY --from=frontend-builder /app/build /app/frontend_build

# Copy the rest of the backend code (scrapers, analysis, etc.)
COPY . /app

# Make the entrypoint script executable
RUN chmod +x /app/entrypoint.sh

# Create directory for the database if it doesn't exist
RUN mkdir -p /app/db

# The API key will be passed as an environment variable at runtime
# We don't hardcode it here for security reasons
ENV OPENAI_API_KEY=""

# Expose Flask port (8501)
EXPOSE 8501

# Use the entrypoint script directly with sh
ENTRYPOINT ["/bin/sh", "/app/entrypoint.sh"]
