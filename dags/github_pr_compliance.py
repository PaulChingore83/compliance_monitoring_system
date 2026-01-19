"""
GitHub PR Compliance Monitoring DAG
Daily ETL pipeline for monitoring PR compliance across organization repositories
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, Any
import logging

# Add plugins directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Try to import with error handling
try:
    from github_etl.extractor import GitHubExtractor
    from github_etl.transformer import PRTransformer
    from github_etl.loader import DataLoader
except ImportError as e:
    # Fallback: Try absolute import
    try:
        sys.path.append('/opt/airflow/plugins')
        from github_etl.extractor import GitHubExtractor
        from github_etl.transformer import PRTransformer
        from github_etl.loader import DataLoader
    except ImportError:
        # Define placeholder classes if import fails (for DAG parsing)
        class GitHubExtractor:
            def __init__(self, **kwargs):
                pass
        
        class PRTransformer:
            def __init__(self, **kwargs):
                pass
        
        class DataLoader:
            def __init__(self, **kwargs):
                pass
        
        logging.warning(f"Using placeholder classes due to import error: {e}")

# Default arguments
default_args: Dict[str, Any] = {
    'owner': 'scytale',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# DAG definition
with DAG(
    dag_id='github_pr_compliance',
    default_args=default_args,
    description='ETL pipeline for GitHub PR compliance monitoring',
    schedule_interval='0 0 * * *',  # Daily at midnight
    catchup=False,
    tags=['github', 'compliance', 'etl'],
) as dag:
    
    def extract_data(**context) -> str:
        """Extract task: Fetch data from GitHub API"""
        logger = logging.getLogger(__name__)
        logger.info("Starting extraction task")
        
        try:
            # Get configuration
            repo_owner = Variable.get("GITHUB_OWNER", default_var="home-assistant")
            access_token = Variable.get("GITHUB_ACCESS_TOKEN", default_var="")
            
            # Initialize extractor
            extractor = GitHubExtractor(
                repo_owner=repo_owner,
                access_token=access_token
            )
            
            # Fetch all PR data
            raw_data_path = extractor.extract_all_prs()
            
            logger.info(f"Extraction completed. Data saved to: {raw_data_path}")
            return raw_data_path
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
    
    def transform_data(**context) -> str:
        """Transform task: Process and validate PR data"""
        logger = logging.getLogger(__name__)
        logger.info("Starting transformation task")
        
        try:
            # Get extraction output
            ti = context['ti']
            raw_data_path = ti.xcom_pull(task_ids='extract_data')
            
            # Initialize transformer
            transformer = PRTransformer()
            
            # Transform data
            transformed_data_path = transformer.transform(raw_data_path)
            
            # Generate summary statistics
            stats = transformer.get_summary_statistics()
            logger.info(f"Transformation summary: {stats}")
            
            logger.info(f"Transformation completed. Data saved to: {transformed_data_path}")
            return transformed_data_path
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def load_data(**context) -> None:
        """Load task: Save transformed data to storage"""
        logger = logging.getLogger(__name__)
        logger.info("Starting load task")
        
        try:
            # Get transformation output
            ti = context['ti']
            transformed_data_path = ti.xcom_pull(task_ids='transform_data')
            
            # Initialize loader
            loader = DataLoader()
            
            # Load to local storage (Parquet)
            local_path = loader.save_to_parquet(transformed_data_path)
            
            # Optional: Load to Snowflake
            snowflake_enabled = Variable.get("SNOWFLAKE_ENABLED", default_var="false")
            if snowflake_enabled.lower() == "true":
                loader.load_to_snowflake(transformed_data_path)
                logger.info("Data loaded to Snowflake")
            
            logger.info(f"Load completed. Local file: {local_path}")
            
        except Exception as e:
            logger.error(f"Load failed: {str(e)}")
            raise
    
    # Define tasks
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
        execution_timeout=timedelta(minutes=15),
    )
    
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
        execution_timeout=timedelta(minutes=10),
    )
    
    # Set task dependencies
    extract_task >> transform_task >> load_task