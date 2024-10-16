import pytest

from dagster import build_op_context
from github_pipeline.assets import create_repo_metadata_asset, repo_report


@pytest.fixture
def mock_github_api(mocker):
    """Fixture for mocking the GitHub API Resource using pytest-mock.

        This fixture provides a mocked instance of the GitHub API Resource,
        pre-configured to return a set of mock repository metadata when
        the `fetch_repo_details` method is called. This allows tests to
        validate functionality without making actual API calls.

        Returns:
            mock: A mocked GitHub API resource.
        """
    github_api = mocker.Mock()
    github_api.fetch_repo_details.return_value = {
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
    return github_api

def test_repo_metadata(mock_github_api):
    """Test the repository metadata asset with a mocked GitHub API.

    This test verifies that the `create_repo_metadata_asset` function
    correctly retrieves and returns repository metadata from the mocked
    GitHub API. It checks that the API was called with the expected
    parameters and that the returned metadata contains the expected fields.

    Args:
        mock_github_api (mock): A mocked instance of the GitHub API Resource.
    """
    # Arrange
    asset_function = create_repo_metadata_asset(
        owner="mock_owner",
        repo="mock_repo",
        key_prefix=["mock", "prefix"],
        name="mock_repo_metadata"
    )
    context = build_op_context()
    # Act
    metadata = asset_function(context, mock_github_api)

    # Assert
    # Check if the GitHub API fetch method was called with the correct parameters
    mock_github_api.fetch_repo_details.assert_called_once_with(owner="mock_owner", repo="mock_repo")

    # Check if the metadata contains the expected fields
    assert metadata["html_url"] == "https://github.com/owner/repo"
    assert metadata["stars"] == 123



def test_repo_report(mocker):
    """
    Test the repository report generation with mocked data.

    This test evaluates the `repo_report` function, ensuring that it
    correctly generates a markdown report based on the provided
    repository data for multiple repositories. It mocks the
    `create_markdown_report` function to verify that it is called
    with the expected arguments and checks that the returned report
    content matches the expected output.

    Args:
        mocker (mocker): The pytest-mock fixture for creating mock objects.
    """
    # Arrange
    delta_rs = {"name": "delta-rs", "stars": 100, "forks": 50}
    iceberg_python = {"name": "iceberg-python", "stars": 200, "forks": 100}
    hudi_rs = {"name": "hudi-rs", "stars": 150, "forks": 75}
    context = build_op_context()

    # Patch create_markdown_report where it's used (in github_pipeline.assets)
    mock_create_markdown_report = mocker.patch(
        "github_pipeline.assets.create_markdown_report", return_value="mock markdown report"
    )

    # Act
    # Call the repo_report function
    report = repo_report(context, delta_rs, iceberg_python, hudi_rs)

    # Assert
    # Check if the markdown report is generated (assert the function was called)
    mock_create_markdown_report.assert_called_once_with(
        context, {"delta-rs": delta_rs, "iceberg-python": iceberg_python, "hudi-rs": hudi_rs}
    )

    # Check if the report content is as expected
    assert report == "mock markdown report"
