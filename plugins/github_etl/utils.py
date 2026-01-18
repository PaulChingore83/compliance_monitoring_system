"""
Utility functions for logging, configuration, and error handling
"""

import logging
import sys
from typing import Dict, Any
from datetime import datetime
from pathlib import Path
import json

def setup_logging(log_dir: str = "/opt/airflow/data/logs") -> logging.Logger:
    """Setup comprehensive logging configuration"""
    log_dir_path = Path(log_dir)
    log_dir_path.mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger('github_etl')
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # File handler
    timestamp = datetime.now().strftime("%Y%m%d")
    log_file = log_dir_path / f"github_etl_{timestamp}.log"
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def load_config(config_file: str = "/opt/airflow/config/settings.json") -> Dict[str, Any]:
    """Load configuration from JSON file"""
    config_path = Path(config_file)
    
    if not config_path.exists():
        return {}
    
    with open(config_path, 'r') as f:
        return json.load(f)

def format_error_message(error: Exception, context: str = "") -> str:
    """Format error messages for consistent logging"""
    error_info = {
        'error_type': type(error).__name__,
        'error_message': str(error),
        'context': context,
        'timestamp': datetime.now().isoformat()
    }
    
    return json.dumps(error_info)

def validate_github_token(token: str) -> bool:
    """Validate GitHub access token format"""
    if not token:
        return False
    
    # GitHub tokens typically start with 'ghp_' (classic) or 'github_pat_' (fine-grained)
    return token.startswith('ghp_') or token.startswith('github_pat_')

def calculate_execution_time(start_time: datetime) -> str:
    """Calculate and format execution time"""
    end_time = datetime.now()
    duration = end_time - start_time
    
    hours, remainder = divmod(duration.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"