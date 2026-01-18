# GitHub PR Compliance Monitoring System

A production-ready ETL pipeline for monitoring GitHub pull request compliance across organization repositories.

## Project Overview

This project provides an automated way to monitor whether merged GitHub pull requests comply with key engineering controls, focusing on **code review approval** and **status check success** across all repositories for a given GitHub user or organization. An Apache Airflow DAG (`github_pr_compliance`) orchestrates a daily ETL pipeline that:
- **Extracts** merged PRs and their metadata, reviews, status checks, and commits from the GitHub API, handling pagination, rate limits, and concurrency.
- **Transforms** the raw JSON into structured compliance metrics, deriving flags such as `code_review_passed`, `status_checks_passed`, and `is_compliant`, plus review/check/commit counts.
- **Loads** the results as Parquet files to local storage (partitioned by repository) and can optionally push the same metrics into Snowflake for analytics and reporting.

The system runs inside Docker using Airflow Webserver + Scheduler + Postgres, with configuration controlled via Airflow Variables (e.g. `GITHUB_OWNER`, `GITHUB_ACCESS_TOKEN`, `SNOWFLAKE_ENABLED`) and standard Airflow Connections. This approach keeps the pipeline **observable, reproducible, and easy to extend** with additional compliance rules or destinations while minimizing operational overhead.



## Features

- **Complete ETL Pipeline**: Extract → Transform → Load with Apache Airflow
- **GitHub API Integration**: Handles pagination, rate limits, and concurrent requests
- **Compliance Validation**: 
  - Code review compliance (at least one approved review)
  - Status check compliance (all checks passed)
- **Production Features**:
  - Comprehensive error handling and logging
  - Exponential backoff retry logic
  - Type hints and data validation
  - Docker containerization
  - Snowflake integration (optional)

## Architecture
Extract (GitHub API) → Transform (Pandas) → Load (Parquet/Snowflake)

### Architecture diagram

```mermaid
flowchart LR
  subgraph GH[GitHub]
    API[GitHub API]
  end

  subgraph AF[Apache Airflow]
    DAG[github_pr_compliance DAG]
    T1[extract_data]
    T2[transform_data]
    T3[load_data]
    DAG --> T1 --> T2 --> T3
  end

  subgraph ST[Storage]
    RAW[data/raw<br/>raw_pr_data_YYYYMMDD_HHMMSS.json]
    PROC[data/processed<br/>pr_compliance_YYYYMMDD_HHMMSS.parquet]
    FINAL[data/processed<br/>final_pr_compliance_YYYYMMDD_HHMMSS.parquet]
    SF[(Snowflake<br/>(optional))]
  end

  API --> T1
  T1 --> RAW
  RAW --> T2
  T2 --> PROC
  PROC --> T3
  T3 --> FINAL
  T3 -. enabled when SNOWFLAKE_ENABLED=true .-> SF
```

## Setup


- **Prerequisites**
  - **Docker & Docker Compose**: Install the latest versions for your OS. [Docker](https://www.docker.com/products/docker-desktop/) (or a free equivalent like [Rancher](https://rancherdesktop.io/))
  - **GitHub Personal Access Token**: With read access to the repositories you want to monitor.
  - **(Optional) Snowflake account**: If you want to load metrics into Snowflake.

- **Clone the repository**

## Step 1: Configuration Setup
Configure Environment Variables
bash

# Create .env file from example
cp .env.example .env

# Edit .env with your preferred editor
nano .env  # or code .env, vim .env, etc.

- **Minimum .env configuration:**

*env*

# Required: GitHub Configuration
GITHUB_ACCESS_TOKEN=ghp_your_token_here  # Optional but recommended
GITHUB_OWNER=home-assistant

# Optional: Snowflake (set to false if not using)
SNOWFLAKE_ENABLED=false

# Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_GID=50000
FERNET_KEY=generate key

Get GitHub Token (Optional but Recommended)

    Go to https://github.com/settings/tokens

    Click "Generate new token" → "Generate new token (classic)"

    Add note: "Airflow Compliance Pipeline"

    Select scopes: repo (full control of private repositories) or public_repo (for public only)

    Copy the token (starts with ghp_)

    Add to .env file

**Step 2: Build and Start Docker Containers**
*bash*

# Build the Docker images
docker-compose build

# Start all services in detached mode
docker-compose up -d

# Check if services are running
docker-compose ps

Expected output:
text

Name                          Command              State           Ports
--------------------------------------------------------------------------------
github-compliance-airflow-scheduler-1   /entrypoint.sh scheduler       Up      8080/tcp
github-compliance-airflow-webserver-1   /entrypoint.sh webserver       Up      0.0.0.0:8080->8080/tcp
github-compliance-postgres-1            docker-entrypoint.sh postgres  Up      5432/tcp

**Step 4: Verify Airflow is Running**
*bash*

# Check logs to ensure Airflow initialized
docker-compose logs airflow-webserver --tail=50

# Or check directly via curl
curl http://localhost:8080/health

Expected: Should return JSON with "status": "healthy"
Step 6: Access Airflow Web UI

    Open browser and go to: http://localhost:8080

    Login credentials:

        Username: admin

        Password: admin

```bash
git clone <this-repo-url>
cd "compliance monitoring system"
```

- **Environment variables**
  - **`GITHUB_ACCESS_TOKEN`**: Export this before starting Docker so it is available to Airflow init scripts:

```bash
export GITHUB_ACCESS_TOKEN="ghp_XXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

  - **(Optional) Snowflake configuration**: If you plan to enable Snowflake loading, you will later configure the Snowflake connection in the Airflow UI.

## Running with Docker / Airflow

- **1. Start Airflow database and create admin user**

```bash
docker compose up airflow-init
```

This will:
- Initialize the Airflow metadata database
- Create an admin user with:
  - **Username**: `admin`
  - **Password**: `admin`

- **2. Start the webserver and scheduler**

```bash
docker compose up -d airflow-webserver airflow-scheduler
```

# 1. Clean slate
docker-compose down -v
docker system prune -a -f

# 2. Use simple docker-compose.yml from step 3
# 3. Start
docker-compose up -d

# 4. Wait longer (Airflow can take time)
echo "Waiting 90 seconds..."
sleep 90

# 5. Check
docker-compose ps

# 6. Check logs



# 7. Try direct access to container
docker-compose exec airflow curl http://localhost:8080/health || echo "Not running inside container"

# 8. Check if process is running
docker-compose exec airflow ps aux | grep webserver

# If Airflow is running, reset the admin user
docker-compose exec airflow airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
   # This overwrites existing user

- **3. Access the Airflow UI**
  - Open `http://localhost:8080` in your browser.
  - Log in with **admin / admin**.

- **4. Configure GitHub and optional Snowflake in Airflow**
  - **Airflow Variables** (Admin → Variables):
    - **`GITHUB_OWNER`**: GitHub org/user to scan (default: `Scytale-exercise`).
    - **`GITHUB_ACCESS_TOKEN`**: Your GitHub PAT (if not provided via the init script).
    - **`SNOWFLAKE_ENABLED`**: `"false"` (default) or `"true"` to enable Snowflake loading.
  - **GitHub Connection (optional)**:
    - Create a connection named `github_default` (type `HTTP`) pointing to `https://api.github.com` with your token if you want to use it in future operators.

- **5. Trigger the DAG**
  - In the Airflow UI, locate the DAG named **`github_pr_compliance`**.
  - Turn it **On** to run on its schedule (daily at midnight UTC) or click **Trigger DAG** to run immediately.

## Outputs

- **Raw data**: Saved as JSON under `/opt/airflow/data/raw` (mapped to the local `data/raw/` directory).
- **Transformed metrics**: Saved as Parquet under `/opt/airflow/data/processed` (local `data/processed/`).
- **Logs**: Written under `/opt/airflow/logs` (local `logs/`).
- **(Optional) Snowflake**: When `SNOWFLAKE_ENABLED="true"`, transformed data is loaded into the configured Snowflake table.

### Sample Output Files

See the [`samples/`](samples/) directory for example output files:
- **`sample_raw_pr_data.json`**: Example raw JSON output from the extract step
- **`sample_pr_compliance.csv`**: Example transformed data structure (CSV representation of Parquet format)
- **`samples/README.md`**: Detailed documentation of output file formats and compliance rules