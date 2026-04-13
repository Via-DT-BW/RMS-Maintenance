"""
GitHub Service Module

Handles all interactions with the GitHub API for creating and managing Issues.
"""

import os
import logging
import requests
from typing import Optional, List

# Configure logging
logger = logging.getLogger(__name__)

# GitHub API configuration
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
GITHUB_API_BASE = "https://api.github.com"


def create_issue(repo: str, title: str, description: str, assignees: Optional[List[str]] = None) -> Optional[int]:
    """
    Create a new GitHub Issue.

    Args:
        repo: Repository in format 'owner/repo'
        title: Issue title
        description: Issue description (body)
        assignees: List of GitHub usernames to assign

    Returns:
        Issue number if successful, None otherwise
    """
    if not GITHUB_TOKEN:
        logger.error("GITHUB_TOKEN not set in environment variables")
        return None

    if not repo or '/' not in repo:
        logger.error(f"Invalid repository format: {repo}")
        return None

    url = f"{GITHUB_API_BASE}/repos/{repo}/issues"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    payload = {
        "title": title,
        "body": description
    }

    if assignees:
        # Filter out empty/None assignees
        valid_assignees = [a for a in assignees if a and a.strip()]
        if valid_assignees:
            payload["assignees"] = valid_assignees

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 201:
            issue_data = response.json()
            issue_number = issue_data.get('number')
            logger.info(f"GitHub Issue created: {repo}#{issue_number} - {title}")
            return issue_number
        else:
            logger.error(f"Failed to create GitHub Issue: {response.status_code} - {response.text}")
            return None
    except requests.RequestException as e:
        logger.error(f"Request error creating GitHub Issue: {str(e)}")
        return None


def close_issue(repo: str, issue_number: int) -> bool:
    """
    Close a GitHub Issue.

    Args:
        repo: Repository in format 'owner/repo'
        issue_number: Issue number to close

    Returns:
        True if successful, False otherwise
    """
    if not GITHUB_TOKEN:
        logger.error("GITHUB_TOKEN not set in environment variables")
        return False

    if not repo or '/' not in repo:
        logger.error(f"Invalid repository format: {repo}")
        return False

    url = f"{GITHUB_API_BASE}/repos/{repo}/issues/{issue_number}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    payload = {
        "state": "closed"
    }

    try:
        response = requests.patch(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            logger.info(f"GitHub Issue closed: {repo}#{issue_number}")
            return True
        else:
            logger.error(f"Failed to close GitHub Issue: {response.status_code} - {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"Request error closing GitHub Issue: {str(e)}")
        return False


def get_user_github_usernames(project_responsible_json: str, cursor) -> List[str]:
    """
    Get GitHub usernames for all responsible users of a project.

    Args:
        project_responsible_json: JSON string containing list of usernames, or delimited string (; or ,)
        cursor: Database cursor to query user GitHub usernames

    Returns:
        List of GitHub usernames (only those with github_username set)
    """
    assignees = []

    if not project_responsible_json:
        return assignees

    try:
        import json
        responsible_list = []
        
        # Try to parse as JSON first
        try:
            responsible_list = json.loads(project_responsible_json)
        except json.JSONDecodeError:
            # If not JSON, support semicolon and comma separated strings
            logger.info(f"[DEBUG] Not valid JSON, trying delimited format: {project_responsible_json}")
            normalized = str(project_responsible_json).replace(';', ',')
            responsible_list = [r.strip() for r in normalized.split(',') if r.strip()]
        
        # Ensure it's a list
        if not isinstance(responsible_list, list):
            responsible_list = [responsible_list]

        logger.info(f"[DEBUG] Parsed responsible list: {responsible_list}")

        for responsible_value in responsible_list:
            if not responsible_value:
                continue
            try:
                normalized_value = str(responsible_value).strip()
                cursor.execute(
                    """
                    SELECT TOP 1 github_username
                    FROM users
                    WHERE github_username IS NOT NULL
                      AND (
                        username = ?
                        OR name = ?
                        OR github_username = ?
                      )
                    """,
                    (normalized_value, normalized_value, normalized_value)
                )
                user_row = cursor.fetchone()
                if user_row and user_row[0]:
                    logger.info(f"[DEBUG] Found github_username for {normalized_value}: {user_row[0]}")
                    assignees.append(user_row[0].strip())
                else:
                    logger.info(f"[DEBUG] No github_username found for responsible value {normalized_value}")
            except Exception as e:
                logger.error(f"Error fetching github_username for responsible value {responsible_value}: {str(e)}")
                continue

    except Exception as e:
        logger.error(f"Unexpected error in get_user_github_usernames: {str(e)}", exc_info=True)

    # Remove duplicates while preserving order
    unique_assignees = list(dict.fromkeys([a for a in assignees if a]))
    logger.info(f"[DEBUG] Final assignees list: {unique_assignees}")
    return unique_assignees


def get_project_github_repo(cursor, project_id: int) -> Optional[str]:
    """
    Get the GitHub repository for a project.

    Args:
        cursor: Database cursor
        project_id: Project ID

    Returns:
        Repository string 'owner/repo' or None if not set
    """
    try:
        cursor.execute(
            "SELECT github_repo FROM projects WHERE id = ?",
            (project_id,)
        )
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
        return None
    except Exception as e:
        logger.error(f"Error fetching github_repo for project {project_id}: {str(e)}")
        return None
