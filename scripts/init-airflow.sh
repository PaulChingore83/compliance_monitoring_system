#!/bin/bash
# Initialize Airflow database and create admin user

echo "Initializing Airflow database..."
airflow db init

echo "Creating admin user..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

echo "Setting up connections and variables..."

# Set GitHub configuration (using environment variables or defaults)
airflow connections add 'github_default' \
    --conn-type 'http' \
    --conn-host 'https://api.github.com' \
    --conn-login '$GITHUB_ACCESS_TOKEN' \
    --conn-extra '{"headers": {"Accept": "application/vnd.github.v3+json"}}'

# Set default variables
airflow variables set GITHUB_OWNER "Scytale-exercise"
airflow variables set SNOWFLAKE_ENABLED "false"

echo "Airflow initialization complete!"