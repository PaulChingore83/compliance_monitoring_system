"""
Data Transformer for PR Compliance Validation
"""

import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
import pandas as pd
from pydantic import BaseModel, validator
from enum import Enum

logger = logging.getLogger(__name__)

class ReviewState(str, Enum):
    APPROVED = "APPROVED"
    CHANGES_REQUESTED = "CHANGES_REQUESTED"
    COMMENTED = "COMMENTED"

class StatusCheckConclusion(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"
    NEUTRAL = "neutral"

class PRComplianceSchema(BaseModel):
    """Schema for transformed PR compliance data"""
    pr_number: int
    pr_title: str
    author: str
    repository: str
    merged_at: datetime
    code_review_passed: bool
    status_checks_passed: bool
    is_compliant: bool
    
    @validator('is_compliant')
    def validate_compliance(cls, v, values):
        """Ensure is_compliant is consistent with other fields"""
        code_review_passed = values.get('code_review_passed', False)
        status_checks_passed = values.get('status_checks_passed', False)
        expected = code_review_passed and status_checks_passed
        if v != expected:
            logger.warning(f"is_compliant mismatch for PR {values.get('pr_number')}. Correcting.")
        return expected

class PRTransformer:
    """Transform raw PR data into compliance metrics"""
    
    def __init__(self, output_dir: str = "/opt/airflow/data/processed"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_raw_data(self, file_path: str) -> List[Dict[str, Any]]:
        """Load raw JSON data from file"""
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def validate_pr_data(self, pr_data: Dict[str, Any]) -> bool:
        """Validate PR data structure"""
        required_fields = ['pr_metadata', 'reviews', 'status_checks', 'commits']
        if not all(field in pr_data for field in required_fields):
            return False
        
        metadata = pr_data['pr_metadata']
        if not all(k in metadata for k in ['number', 'title', 'author', 'merged_at', 'repository']):
            return False
        
        return True
    
    def check_code_review_compliance(self, reviews: List[Dict]) -> bool:
        """Check if PR has at least one approved review"""
        if not reviews:
            return False
        
        approved_reviews = [
            review for review in reviews 
            if review.get('state') == ReviewState.APPROVED.value
        ]
        
        return len(approved_reviews) > 0
    
    def check_status_checks_compliance(self, status_checks: Dict) -> bool:
        """Check if all required status checks passed"""
        if not status_checks or not status_checks.get('statuses'):
            return False
        
        required_conclusions = {StatusCheckConclusion.SUCCESS.value}
        failed_conclusions = {
            StatusCheckConclusion.FAILURE.value,
            StatusCheckConclusion.CANCELLED.value
        }
        
        # Check if any check failed
        for check in status_checks['statuses']:
            conclusion = check.get('conclusion')
            if conclusion in failed_conclusions:
                return False
        
        # Check if all required checks succeeded
        all_checks = [check.get('conclusion') for check in status_checks['statuses']]
        successful_checks = [c for c in all_checks if c == StatusCheckConclusion.SUCCESS.value]
        
        # If there are required checks, ensure all passed
        if all_checks:
            return len(successful_checks) == len(all_checks)
        
        return False
    
    def transform_pr(self, pr_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform single PR data into compliance metrics"""
        try:
            if not self.validate_pr_data(pr_data):
                logger.warning(f"Skipping invalid PR data: {pr_data.get('pr_metadata', {}).get('number', 'unknown')}")
                return None
            
            metadata = pr_data['pr_metadata']
            reviews = pr_data['reviews']
            status_checks = pr_data['status_checks']
            
            # Calculate compliance metrics
            code_review_passed = self.check_code_review_compliance(reviews)
            status_checks_passed = self.check_status_checks_compliance(status_checks)
            is_compliant = code_review_passed and status_checks_passed
            
            return {
                'pr_number': metadata['number'],
                'pr_title': metadata['title'],
                'author': metadata['author']['login'],
                'repository': metadata['repository'],
                'merged_at': metadata['merged_at'],
                'code_review_passed': code_review_passed,
                'status_checks_passed': status_checks_passed,
                'is_compliant': is_compliant,
                'review_count': len(reviews),
                'approved_review_count': len([r for r in reviews if r.get('state') == ReviewState.APPROVED.value]),
                'status_check_count': len(status_checks.get('statuses', [])),
                'commit_count': len(pr_data.get('commits', []))
            }
            
        except Exception as e:
            logger.error(f"Failed to transform PR data: {str(e)}")
            return None
    
    def transform(self, input_file: str) -> str:
        """Transform all PR data from raw JSON to compliance metrics"""
        logger.info(f"Starting transformation of {input_file}")
        
        try:
            # Load raw data
            raw_data = self.load_raw_data(input_file)
            
            # Transform each PR
            transformed_data = []
            for pr_data in raw_data:
                transformed_pr = self.transform_pr(pr_data)
                if transformed_pr:
                    transformed_data.append(transformed_pr)
            
            # Create DataFrame
            df = pd.DataFrame(transformed_data)
            
            # Validate with schema
            for _, row in df.iterrows():
                try:
                    PRComplianceSchema(**row.to_dict())
                except Exception as e:
                    logger.warning(f"Schema validation failed for PR {row.get('pr_number')}: {str(e)}")
            
            # Save to Parquet
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.output_dir / f"pr_compliance_{timestamp}.parquet"
            
            df.to_parquet(output_file, index=False)
            
            logger.info(f"Transformed {len(transformed_data)} PRs. Saved to {output_file}")
            return str(output_file)
            
        except Exception as e:
            logger.error(f"Transformation failed: {str(e)}")
            raise
    
    def get_summary_statistics(self, df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """Generate summary statistics from transformed data"""
        try:
            if df is None:
                # Find latest parquet file
                parquet_files = list(self.output_dir.glob("*.parquet"))
                if not parquet_files:
                    return {}
                
                latest_file = max(parquet_files, key=lambda x: x.stat().st_mtime)
                df = pd.read_parquet(latest_file)
            
            if df.empty:
                return {}
            
            total_prs = len(df)
            compliant_prs = df['is_compliant'].sum()
            compliance_rate = (compliant_prs / total_prs * 100) if total_prs > 0 else 0
            
            # Violations by repository
            violations_by_repo = df[df['is_compliant'] == False].groupby('repository').size().to_dict()
            
            # Breakdown of violation types
            review_violations = df[~df['code_review_passed']].shape[0]
            check_violations = df[~df['status_checks_passed']].shape[0]
            
            stats = {
                'total_prs': total_prs,
                'compliant_prs': compliant_prs,
                'compliance_rate': round(compliance_rate, 2),
                'violations_by_repository': violations_by_repo,
                'review_violations': review_violations,
                'check_violations': check_violations,
                'generated_at': datetime.now().isoformat()
            }
            
            # Log summary
            logger.info(f"""
            === Compliance Summary ===
            Total PRs: {total_prs}
            Compliant PRs: {compliant_prs}
            Compliance Rate: {compliance_rate:.2f}%
            Review Violations: {review_violations}
            Check Violations: {check_violations}
            ===========================
            """)
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to generate statistics: {str(e)}")
            return {}