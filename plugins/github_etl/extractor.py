"""
GitHub API Extractor with pagination, rate limiting, and error handling
"""

import asyncio
import aiohttp
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
from pathlib import Path
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests

logger = logging.getLogger(__name__)

class GitHubExtractor:
    """Extract PR data from GitHub API with proper rate limiting and error handling"""
    
    BASE_URL = "https://api.github.com"
    
    def __init__(
        self,
        repo_owner: str = "home-assistant",
        access_token: Optional[str] = None,
        max_concurrent_requests: int = 10,
        output_dir: str = "/opt/airflow/data/raw"
    ):
        self.repo_owner = repo_owner
        self.access_token = access_token
        self.max_concurrent_requests = max_concurrent_requests
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Headers for authentication
        self.headers = {"Accept": "application/vnd.github.v3+json"}
        if access_token:
            self.headers["Authorization"] = f"token {access_token}"
    
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, aiohttp.ClientError))
    )
    async def _make_request_async(self, session: aiohttp.ClientSession, url: str) -> Optional[Dict]:
        """Make async HTTP request with rate limiting"""
        try:
            async with session.get(url, headers=self.headers) as response:
                # Check rate limits
                remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
                reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                
                if remaining == 0:
                    sleep_time = max(reset_time - time.time(), 0) + 1
                    logger.warning(f"Rate limit reached. Sleeping for {sleep_time} seconds")
                    await asyncio.sleep(sleep_time)
                    return await self._make_request_async(session, url)
                
                if response.status == 200:
                    return await response.json()
                elif response.status == 404:
                    logger.warning(f"Resource not found: {url}")
                    return None
                else:
                    response.raise_for_status()
                    
        except aiohttp.ClientError as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            raise
    
    def _make_request_sync(self, url: str) -> Optional[Dict]:
        """Make synchronous HTTP request"""
        try:
            response = requests.get(url, headers=self.headers)
            
            # Handle rate limits
            remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
            reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
            
            if remaining == 0:
                sleep_time = max(reset_time - time.time(), 0) + 1
                logger.warning(f"Rate limit reached. Sleeping for {sleep_time} seconds")
                time.sleep(sleep_time)
                return self._make_request_sync(url)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logger.warning(f"Resource not found: {url}")
                return None
            else:
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {str(e)}")
            raise
    
    async def _fetch_pr_details(self, session: aiohttp.ClientSession, repo: str, pr_number: int) -> Dict[str, Any]:
        """Fetch detailed PR information including reviews, status checks, and commits"""
        base_url = f"{self.BASE_URL}/repos/{self.repo_owner}/{repo}"
        
        # Fetch PR metadata
        pr_url = f"{base_url}/pulls/{pr_number}"
        pr_data = await self._make_request_async(session, pr_url)
        
        if not pr_data or pr_data.get('state') != 'closed' or not pr_data.get('merged_at'):
            return {}
        
        # Fetch reviews
        reviews_url = f"{pr_url}/reviews"
        reviews_data = await self._make_request_async(session, reviews_url) or []
        
        # Fetch status checks
        status_url = f"{base_url}/commits/{pr_data['head']['sha']}/status"
        status_data = await self._make_request_async(session, status_url) or {}
        
        # Fetch commits
        commits_url = f"{pr_url}/commits"
        commits_data = await self._make_request_async(session, commits_url) or []
        
        return {
            'pr_metadata': {
                'number': pr_data['number'],
                'title': pr_data['title'],
                'state': pr_data['state'],
                'merged_at': pr_data['merged_at'],
                'author': {
                    'login': pr_data['user']['login'],
                    'id': pr_data['user']['id']
                },
                'base_branch': pr_data['base']['ref'],
                'head_branch': pr_data['head']['ref'],
                'repository': repo
            },
            'reviews': reviews_data,
            'status_checks': status_data,
            'commits': commits_data
        }
    
    async def _fetch_repository_prs(self, session: aiohttp.ClientSession, repo: str) -> List[Dict]:
        """Fetch all closed PRs from a repository"""
        prs_url = f"{self.BASE_URL}/repos/{self.repo_owner}/{repo}/pulls"
        params = {'state': 'closed', 'per_page': 100}
        
        all_prs = []
        page = 1
        
        while True:
            params['page'] = page
            url = f"{prs_url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}"
            
            prs_page = await self._make_request_async(session, url)
            if not prs_page:
                break
            
            # Filter for merged PRs only
            merged_prs = [pr for pr in prs_page if pr.get('merged_at')]
            
            # Fetch details for each merged PR concurrently
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            
            async def fetch_with_semaphore(pr_num: int):
                async with semaphore:
                    return await self._fetch_pr_details(session, repo, pr_num)
            
            tasks = [fetch_with_semaphore(pr['number']) for pr in merged_prs]
            detailed_prs = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out empty results and exceptions
            valid_prs = [pr for pr in detailed_prs if pr and not isinstance(pr, Exception)]
            all_prs.extend(valid_prs)
            
            if len(prs_page) < 100:
                break
            
            page += 1
        
        return all_prs
    
    def get_repositories(self) -> List[str]:
        """Fetch all repositories for the organization/user"""
        url = f"{self.BASE_URL}/users/{self.repo_owner}/repos"
        params = {'per_page': 100}
        
        repos = []
        page = 1
        
        while True:
            params['page'] = page
            response = self._make_request_sync(f"{url}?{'&'.join([f'{k}={v}' for k, v in params.items()])}")
            
            if not response:
                break
            
            repos.extend([repo['name'] for repo in response])
            
            if len(response) < 100:
                break
            
            page += 1
        
        logger.info(f"Found {len(repos)} repositories")
        return repos
    
    async def extract_all_prs_async(self) -> List[Dict]:
        """Extract all PRs from all repositories asynchronously"""
        repositories = self.get_repositories()
        
        async with aiohttp.ClientSession() as session:
            all_pr_data = []
            
            for repo in repositories:
                logger.info(f"Processing repository: {repo}")
                try:
                    repo_prs = await self._fetch_repository_prs(session, repo)
                    all_pr_data.extend(repo_prs)
                    logger.info(f"Found {len(repo_prs)} merged PRs in {repo}")
                except Exception as e:
                    logger.error(f"Failed to process repository {repo}: {str(e)}")
                    continue
            
            return all_pr_data
    
    def extract_all_prs(self) -> str:
        """Main extraction method - runs async extraction"""
        logger.info("Starting GitHub PR extraction")
        
        try:
            # Run async extraction
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            all_pr_data = loop.run_until_complete(self.extract_all_prs_async())
            
            # Filter out empty results
            filtered_data = [pr for pr in all_pr_data if pr]
            
            # Save to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = self.output_dir / f"raw_pr_data_{timestamp}.json"
            
            with open(output_file, 'w') as f:
                json.dump(filtered_data, f, indent=2, default=str)
            
            logger.info(f"Extracted {len(filtered_data)} PRs. Saved to {output_file}")
            return str(output_file)
            
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            raise
        finally:
            loop.close()