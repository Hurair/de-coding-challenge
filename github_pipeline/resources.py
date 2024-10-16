from datetime import datetime
from typing import Any, Union
from urllib.parse import urljoin

import requests

from dagster import ConfigurableResource, get_dagster_logger


class GitHubAPIResource(ConfigurableResource):
    """Custom Dagster resource for the GitHub REST API.

    Args:
        - github_token (str | None, optional): \
            GitHub token for authentication. If no token is set, the API calls will be without authentication.
        - host (str, optional): \
            Host address of the Contentful Management API. Defaults to 'https://api.github.com'.
    """

    github_token: str | None = None
    """GitHub token for authentication. If no token is set, the API calls will be without authentication."""

    host: str = "https://api.github.com"
    """Host of the GitHub REST API."""

    def _get_headers(self) -> dict[str, str]:
        """
           Build the headers for GitHub API requests, including authentication if a token is available.
           Returns:
               dict[str, str]: A dictionary of headers for GitHub API requests.
        """
        headers = {"Accept": "application/vnd.github+json"}
        if self.github_token:
            headers["Authorization"] = f"Bearer {self.github_token}"
        return headers

    @staticmethod
    def get_next_link(link_header: str) -> Union[str, None]:
        """
        Extract the 'next' link from the GitHub Link header for paginated responses.
        Args:
            link_header (str): The 'Link' header from a GitHub API response.
        Returns:
            str | None: The URL of the next page if available, otherwise None.
        """
        links = [link.strip() for link in link_header.split(",")]
        for link in links:
            if 'rel="next"' in link:
                return link[link.find("<") + 1:link.find(">")]
        return None

    def execute_request(self, method: str, path: str, params: dict[str, Any] = None,
                        json: Any = None) -> requests.Response:
        """
        Execute an HTTP request to the GitHub REST API.

        Args:
            method (str): The HTTP method to use (e.g., 'GET', 'POST').
            path (str): The API endpoint path (relative to the host).
            params (dict[str, Any], optional): URL query parameters to include in the request.
            json (Any, optional): JSON payload for the request body, if applicable.

        Returns:
            requests.Response: The HTTP response object from the GitHub API.

        Raises:
            requests.HTTPError: If the API response contains an HTTP error status code.
        """
        url = urljoin(self.host, path)
        headers = self._get_headers()
        try:
            response = requests.request(method, url, headers=headers, params=params, json=json)
            get_dagster_logger().info(f"Request to {response.url}")
            response.raise_for_status()
        except requests.HTTPError as e:
            get_dagster_logger().exception(f"HTTPError {e} for {url}")
            raise e
        return response

    def fetch_repo_details(self, owner: str, repo: str) -> dict[str, Any]:
        """
        Fetch metadata, issues, and pull requests for a GitHub repository.

        This method retrieves detailed metadata for a repository, including the number of stars, forks,
        watchers, releases, open/closed issues, and pull requests. It also fetches information on releases
        and calculates average days to close issues and pull requests.

        Args:
            owner (str): The owner or organization name of the repository.
            repo (str): The name of the repository.

        Returns:
            dict[str, Any]: A dictionary containing repository metadata and aggregated information
                            about issues, pull requests, and releases.

        Example return format:
        {
            "html_url": "https://github.com/owner/repo",
            "stars": 123,
            "forks": 45,
            "watchers": 10,
            "releases": 5,
            "open_issues": 20,
            "closed_issues": 15,
            "avg_days_until_issue_was_closed": 3.2,
            "open_prs": 5,
            "closed_prs": 10,
            "avg_days_until_pr_was_closed": 2.5,
        }
        """

        def api_request(path: str, params: dict[str, Any] = None) -> list[dict[str, Any]]:
            """
            Helper function to fetch paginated results from GitHub API.

            Args:
                path (str): The API endpoint path.
                params (dict[str, Any], optional): URL query parameters.

            Returns:
                list[dict[str, Any]]: A list of results from the API.
            """

            results = []
            while path:
                response = self.execute_request("GET", path, params)
                results.extend(response.json())
                link_header = response.headers.get("Link")
                path = self.get_next_link(link_header) if link_header else None
            return results

        def calculate_average_days(items: list[dict[str, Any]]) -> float:
            """
            Calculate the average number of days between 'created_at' and 'closed_at' for closed issues/PRs.

            Args:
                items (list[dict[str, Any]]): List of issues or pull requests.

            Returns:
                float: The average number of days from creation to closure, or 0 if no items are closed.
            """
            total_days, count = 0, 0
            for item in items:
                if item.get("closed_at"):
                    total_days += (datetime.strptime(item["closed_at"], "%Y-%m-%dT%H:%M:%SZ") -
                                   datetime.strptime(item["created_at"], "%Y-%m-%dT%H:%M:%SZ")).days
                    count += 1
            return round(total_days / count if count else 0, 1)

        # Fetch metadata i.e stars, forks, watchers.
        repo_metadata = self.execute_request("GET", f"/repos/{owner}/{repo}").json()

        # Fetch all issues (open and closed) along with pull requests.
        issues_and_prs = api_request(f"/repos/{owner}/{repo}/issues", params={"state": "all", "per_page": 100})

        # Separate open and closed issues (no "pull_request" field) and PRs (contain "pull_request" field)
        open_issues = [item for item in issues_and_prs if "pull_request" not in item and item["state"] == "open"]
        closed_issues = [item for item in issues_and_prs if "pull_request" not in item and item["state"] == "closed"]
        open_prs = [item for item in issues_and_prs if "pull_request" in item and item["state"] == "open"]
        closed_prs = [item for item in issues_and_prs if "pull_request" in item and item["state"] == "closed"]

        # Fetch releases
        releases = api_request(f"/repos/{owner}/{repo}/releases", params={"per_page": 100})

        return {
            "html_url": repo_metadata.get("html_url"),
            "stars": repo_metadata.get("stargazers_count", 0),
            "forks": repo_metadata.get("forks_count", 0),
            "watchers": repo_metadata.get("subscribers_count", 0),
            "releases": len(releases) if releases else 0,
            "open_issues": len(open_issues),
            "closed_issues": len(closed_issues),
            "avg_days_until_issue_was_closed": calculate_average_days(closed_issues),
            "open_prs": len(open_prs),
            "closed_prs": len(closed_prs),
            "avg_days_until_pr_was_closed": calculate_average_days(closed_prs),
        }
