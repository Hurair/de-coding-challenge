from typing import Any

import pandas as pd

from dagster import AssetExecutionContext, MetadataValue
from github_pipeline.resources import GitHubAPIResource


def create_markdown_report(context: AssetExecutionContext, report_data: dict[str, dict]) -> str:
    """
    Create a markdown report from the report data.
    This function converts the input dictionary containing report data into a markdown formatted
    string. The resulting markdown report is also added to the execution context's metadata.

    Args:
        context (AssetExecutionContext):
            The context for the asset execution.
        report_data (dict[str, dict]):
            A dictionary where keys are report categories and values are dictionaries containing
            the specific data to be reported.

    Returns:
        str:
            A markdown formatted report string.
    """
    # use pandas to convert dict with report data to markdown table
    df_report = pd.DataFrame.from_dict(report_data)
    df_report = df_report.drop('html_url', axis=0) # Droping the url row as its not required in the final report.
    md_report = df_report.to_markdown()

    context.add_output_metadata({"report": MetadataValue.md(md_report)})
    return md_report


def fetch_repo_metadata(context: AssetExecutionContext, github_api: GitHubAPIResource, owner: str, repo: str) -> dict[str, Any]:
    """
    Fetch metadata for a specified GitHub repository.
    This function retrieves the repository details using the provided GitHub API resource.

    Args:
        context (AssetExecutionContext):
            The context for the asset execution.
        github_api (GitHubAPIResource):
            The GitHub API resource used to make requests for repository data.
        owner (str):
            The owner or organization name of the repository.
        repo (str):
            The name of the repository.

    Returns:
        dict[str, Any]:
            A dictionary containing the repository metadata, or an empty dictionary if the fetch fails.

    Example return format:
    {
        "html_url": "https://github.com/owner/repo",
        "stars": 123,
        "forks": 45,
        ...
    }
    """

    try:
        repo_metadata = github_api.fetch_repo_details(owner=owner, repo=repo)
        context.add_output_metadata(
            metadata={
                "repo link": MetadataValue.url(repo_metadata.get("html_url")),
                "data preview": MetadataValue.json(repo_metadata),
            }
        )
        return repo_metadata
    except Exception as e:
        context.log.error(f"Failed to fetch metadata for {owner}/{repo}: {e}")
        return {}
