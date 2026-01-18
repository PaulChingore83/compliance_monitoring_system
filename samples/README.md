# Sample Output Files

This directory contains sample output files demonstrating the structure and format of data produced by the GitHub PR Compliance Monitoring System.

## Files

### `sample_raw_pr_data.json`
**Location in pipeline**: `data/raw/raw_pr_data_YYYYMMDD_HHMMSS.json`

This file contains the raw extracted data from the GitHub API. Each entry represents a merged pull request with:
- **pr_metadata**: Basic PR information (number, title, author, repository, merge date, branches)
- **reviews**: Array of PR reviews (including state: APPROVED, COMMENTED, CHANGES_REQUESTED)
- **status_checks**: Status check results (CI/CD checks, linting, security scans, etc.)
- **commits**: Array of commits included in the PR

**Example entries**:
- PR #42: Compliant PR (has approved review + all status checks passed)
- PR #43: Non-compliant PR (no reviews, but status checks passed)
- PR #44: Non-compliant PR (has approved review, but status checks failed)

### `sample_pr_compliance.csv`
**Location in pipeline**: `data/processed/pr_compliance_YYYYMMDD_HHMMSS.parquet`

This CSV file represents the structure of the transformed Parquet files. The actual output is in Parquet format for efficient storage and querying.

**Columns**:
- `pr_number`: GitHub PR number
- `pr_title`: Title of the pull request
- `author`: GitHub username of the PR author
- `repository`: Repository name
- `merged_at`: ISO 8601 timestamp of when the PR was merged
- `code_review_passed`: Boolean indicating if at least one approved review exists
- `status_checks_passed`: Boolean indicating if all status checks passed
- `is_compliant`: Boolean indicating overall compliance (both code_review_passed AND status_checks_passed must be True)
- `review_count`: Total number of reviews
- `approved_review_count`: Number of approved reviews
- `status_check_count`: Total number of status checks
- `commit_count`: Number of commits in the PR

**Note**: The actual Parquet files are binary and partitioned by repository. To view Parquet files, use:
```python
import pandas as pd
df = pd.read_parquet('data/processed/pr_compliance_YYYYMMDD_HHMMSS.parquet')
print(df.head())
```

## Compliance Rules

A PR is considered **compliant** if:
1. ✅ **Code Review Compliance**: At least one review with state `APPROVED`
2. ✅ **Status Check Compliance**: All status checks have conclusion `success` (no failures or cancellations)

Both conditions must be met for `is_compliant` to be `True`.

## Final Output Files

After the load step, the data is saved to:
- `data/processed/final_pr_compliance_YYYYMMDD_HHMMSS.parquet/` (partitioned by repository)
- Optionally loaded to Snowflake table `PR_COMPLIANCE_METRICS` (if `SNOWFLAKE_ENABLED=true`)
