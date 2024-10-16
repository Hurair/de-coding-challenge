import pytest
import requests

from github_pipeline.resources import GitHubAPIResource

# Sample mock data to simulate GitHub API responses
mock_repo_metadata = {
    "html_url": "https://api.github.com/repos/test_owner/test_repo",
    "stargazers_count": 100,
    "forks_count": 20,
    "subscribers_count": 5,
}

mock_issues_and_prs = [
    {"state": "open", "pull_request": {}, "created_at": "2023-01-01T00:00:00Z"},
    {"state": "closed", "pull_request": {}, "created_at": "2023-01-01T00:00:00Z", "closed_at": "2023-01-05T00:00:00Z"},
    {"state": "open", "created_at": "2023-01-01T00:00:00Z"},
    {"state": "closed", "created_at": "2023-01-01T00:00:00Z", "closed_at": "2023-01-03T00:00:00Z"},
]

mock_releases = [{"id": 1}, {"id": 2}, {"id": 3}]


@pytest.fixture
def github_api_resource():
    """
    Fixture for initializing the GitHubAPIResource object.

    This fixture sets up an instance of the `GitHubAPIResource`
    class with a mock GitHub token for testing purposes. It can
    be used in tests that require interaction with the GitHub API
    through this resource.

    Returns:
        GitHubAPIResource: An instance of the GitHubAPIResource
        initialized with a mock token.
    """
    return GitHubAPIResource(github_token="mock_token")


def test_fetch_repo_details(requests_mock, github_api_resource):
    """
    Test fetching repository details from GitHub using mocked responses.

    This test verifies that the `fetch_repo_details` method of
    the `GitHubAPIResource` correctly retrieves and processes
    repository metadata, issues, pull requests, and releases
    from the GitHub API. The API responses are mocked to simulate
    the expected output.

    Args:
        requests_mock (requests_mock.Mocker): The requests-mock fixture
        to mock API responses.
        github_api_resource (GitHubAPIResource): The fixture providing
        an instance of GitHubAPIResource.

    Asserts:
        The test checks that the returned repository details match
        the expected values based on the mocked responses.
    """
    # Arrange
    # Mock the API responses
    repo_url = "https://api.github.com/repos/test_owner/test_repo"
    issues_url = "https://api.github.com/repos/test_owner/test_repo/issues"
    releases_url = "https://api.github.com/repos/test_owner/test_repo/releases"

    # Mock GitHub API response for repo metadata
    requests_mock.get(repo_url, json=mock_repo_metadata, status_code=200)

    # Mock GitHub API response for issues and pull requests
    requests_mock.get(issues_url, json=mock_issues_and_prs, status_code=200)

    # Mock GitHub API response for releases
    requests_mock.get(releases_url, json=mock_releases, status_code=200)

    # Act
    # Call the fetch_repo_details method
    repo_details = github_api_resource.fetch_repo_details("test_owner", "test_repo")

    # Assert
    # Validate the output
    assert repo_details["html_url"] == "https://api.github.com/repos/test_owner/test_repo"
    assert repo_details["stars"] == 100
    assert repo_details["forks"] == 20
    assert repo_details["watchers"] == 5
    assert repo_details["releases"] == 3
    assert repo_details["open_issues"] == 1
    assert repo_details["closed_issues"] == 1
    assert repo_details["avg_days_until_issue_was_closed"] == 2.0
    assert repo_details["open_prs"] == 1
    assert repo_details["closed_prs"] == 1
    assert repo_details["avg_days_until_pr_was_closed"] == 4.0


def test_execute_request_raises_http_error(requests_mock, github_api_resource):
    """
    Test that an HTTP error is raised and handled properly.

    This test verifies that the `execute_request` method of the
    `GitHubAPIResource` raises an `HTTPError` when an unsuccessful
    HTTP response is received from the GitHub API. It simulates
    a 404 Not Found error for a specific repository.

    Args:
        requests_mock (requests_mock.Mocker): The requests-mock fixture
        to mock API responses.
        github_api_resource (GitHubAPIResource): The fixture providing
        an instance of GitHubAPIResource.

    Asserts:
        The test ensures that an `HTTPError` is raised when the
        execute_request method is called for a nonexistent repository.
    """
    # Arrange
    # Ensure the correct URL is mocked
    repo_url = "https://api.github.com/repos/test_owner/test_repo"

    # Simulate an error from the GitHub API (404 Not Found)
    requests_mock.get(repo_url, status_code=404)

    # Act & Assert
    # Ensure that the HTTPError is raised when calling execute_request
    with pytest.raises(requests.HTTPError):
        github_api_resource.execute_request("GET", "/repos/test_owner/test_repo")
