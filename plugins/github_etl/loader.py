"""
Data Loader for saving transformed data to various storage systems
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger(__name__)

class DataLoader:
    """Load transformed data to storage systems"""
    
    def __init__(
        self,
        local_output_dir: str = "/opt/airflow/data/processed",
        snowflake_config: Optional[Dict[str, Any]] = None
    ):
        self.local_output_dir = Path(local_output_dir)
        self.local_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Snowflake configuration
        self.snowflake_config = snowflake_config or {}
    
    def save_to_parquet(self, input_file: str, 
                       additional_metadata: Optional[Dict[str, Any]] = None) -> str:
        """Save DataFrame to Parquet with timestamp and metadata"""
        try:
            # Read input file
            if input_file.endswith('.parquet'):
                df = pd.read_parquet(input_file)
            elif input_file.endswith('.json'):
                df = pd.read_json(input_file)
            else:
                raise ValueError(f"Unsupported file format: {input_file}")
            
            # Add metadata columns
            df['_loaded_at'] = datetime.now()
            df['_file_source'] = input_file
            
            if additional_metadata:
                for key, value in additional_metadata.items():
                    df[f'_meta_{key}'] = value
            
            # Save to new file with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.local_output_dir / f"final_pr_compliance_{timestamp}.parquet"
            
            # Save with partitioning by repository
            df.to_parquet(
                output_file,
                index=False,
                partition_cols=['repository'] if 'repository' in df.columns else None
            )
            
            logger.info(f"Saved {len(df)} records to {output_file}")
            return str(output_file)
            
        except Exception as e:
            logger.error(f"Failed to save to Parquet: {str(e)}")
            raise
    
    def _get_snowflake_connection(self):
        """Create Snowflake connection"""
        try:
            conn = snowflake.connector.connect(
                user=self.snowflake_config.get('user'),
                password=self.snowflake_config.get('password'),
                account=self.snowflake_config.get('account'),
                warehouse=self.snowflake_config.get('warehouse'),
                database=self.snowflake_config.get('database'),
                schema=self.snowflake_config.get('schema'),
                role=self.snowflake_config.get('role', 'PUBLIC')
            )
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise
    
    def load_to_snowflake(self, input_file: str, 
                         table_name: str = "PR_COMPLIANCE_METRICS") -> None:
        """Load data to Snowflake (optional)"""
        if not self.snowflake_config:
            logger.warning("Snowflake configuration not provided. Skipping Snowflake load.")
            return
        
        try:
            # Read data
            df = pd.read_parquet(input_file)
            
            # Add load metadata
            df['_snowflake_loaded_at'] = datetime.now()
            
            # Connect to Snowflake
            conn = self._get_snowflake_connection()
            
            # Create table if not exists
            with conn.cursor() as cur:
                cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    pr_number NUMBER,
                    pr_title VARCHAR,
                    author VARCHAR,
                    repository VARCHAR,
                    merged_at TIMESTAMP_NTZ,
                    code_review_passed BOOLEAN,
                    status_checks_passed BOOLEAN,
                    is_compliant BOOLEAN,
                    review_count NUMBER,
                    approved_review_count NUMBER,
                    status_check_count NUMBER,
                    commit_count NUMBER,
                    _snowflake_loaded_at TIMESTAMP_NTZ
                )
                """)
            
            # Write data to Snowflake
            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name,
                schema=self.snowflake_config.get('schema', 'PUBLIC'),
                database=self.snowflake_config.get('database'),
                auto_create_table=False,
                overwrite=False
            )
            
            if success:
                logger.info(f"Successfully loaded {nrows} rows to Snowflake table {table_name}")
            else:
                logger.error("Failed to load data to Snowflake")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Failed to load to Snowflake: {str(e)}")
            raise
    
    def generate_report(self, input_file: str, 
                       report_type: str = "html") -> str:
        """Generate compliance report"""
        try:
            df = pd.read_parquet(input_file)
            
            if report_type == "html":
                report = self._generate_html_report(df)
            elif report_type == "json":
                report = self._generate_json_report(df)
            else:
                raise ValueError(f"Unsupported report type: {report_type}")
            
            # Save report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = self.local_output_dir / f"compliance_report_{timestamp}.{report_type}"
            
            with open(report_file, 'w') as f:
                f.write(report)
            
            logger.info(f"Generated {report_type} report: {report_file}")
            return str(report_file)
            
        except Exception as e:
            logger.error(f"Failed to generate report: {str(e)}")
            raise
    
    def _generate_html_report(self, df: pd.DataFrame) -> str:
        """Generate HTML compliance report"""
        total_prs = len(df)
        compliant_prs = df['is_compliant'].sum()
        compliance_rate = (compliant_prs / total_prs * 100) if total_prs > 0 else 0
        
        # Top repositories by compliance
        repo_compliance = df.groupby('repository').agg({
            'is_compliant': ['count', 'sum']
        }).round(2)
        repo_compliance.columns = ['total_prs', 'compliant_prs']
        repo_compliance['compliance_rate'] = (repo_compliance['compliant_prs'] / repo_compliance['total_prs'] * 100).round(2)
        repo_compliance = repo_compliance.sort_values('compliance_rate', ascending=False)
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>GitHub PR Compliance Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                .summary {{ background: #f5f5f5; padding: 20px; border-radius: 5px; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .compliant {{ color: green; }}
                .non-compliant {{ color: red; }}
            </style>
        </head>
        <body>
            <h1>GitHub PR Compliance Report</h1>
            <div class="summary">
                <h2>Summary</h2>
                <p><strong>Total PRs Analyzed:</strong> {total_prs}</p>
                <p><strong>Compliant PRs:</strong> {compliant_prs}</p>
                <p><strong>Compliance Rate:</strong> {compliance_rate:.2f}%</p>
                <p><strong>Generated At:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            
            <h2>Repository Compliance</h2>
            {repo_compliance.to_html(classes='compliance-table')}
        </body>
        </html>
        """
        
        return html
    
    def _generate_json_report(self, df: pd.DataFrame) -> str:
        """Generate JSON compliance report"""
        import json
        
        report_data = {
            'summary': {
                'total_prs': len(df),
                'compliant_prs': int(df['is_compliant'].sum()),
                'compliance_rate': round((df['is_compliant'].sum() / len(df) * 100), 2) if len(df) > 0 else 0,
                'generated_at': datetime.now().isoformat()
            },
            'repository_stats': df.groupby('repository').agg({
                'is_compliant': ['count', 'sum']
            }).to_dict(),
            'violations': {
                'code_review': int((~df['code_review_passed']).sum()),
                'status_checks': int((~df['status_checks_passed']).sum())
            }
        }
        
        return json.dumps(report_data, indent=2)