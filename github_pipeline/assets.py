from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    FreshnessPolicy,
    asset,
)
from github_pipeline.resources import GitHubAPIResource
from github_pipeline.utils import create_markdown_report, fetch_repo_metadata


def create_repo_metadata_asset(owner: str, repo: str, key_prefix: list[str], name: str,
                               freshness_minutes: int = 60 * 24) -> dict[str, Any]:
    """
        Creates a reusable asset function for fetching metadata for a GitHub repository.

        This function is a factory that generates an asset for Dagster's asset system. It defines an asset that
        fetches metadata from the GitHub API for the specified repository. The asset is decorated with Dagster's
        asset decorator and includes a freshness policy to control how often the asset is updated.

        Args:
            owner (str): The GitHub repository owner (organization or username).
            repo (str): The name of the repository for which metadata is being fetched.
            key_prefix (list[str]): A list of strings to use as a prefix for the asset's key in the asset graph.
            name (str): The name to be given to the asset.
            freshness_minutes (int, optional): The maximum freshness lag in minutes before the asset becomes stale.
                                               Defaults to 24 hours (60 * 24).

        Returns:
            dict[str, Any]: A dictionary containing the metadata fetched from the GitHub repository.
        """
    @asset(
        name=name,
        key_prefix=key_prefix,
        io_manager_key="json_io_manager",
        group_name="github",
        freshness_policy=FreshnessPolicy(maximum_lag_minutes=freshness_minutes),
    )
    def repo_metadata(context: AssetExecutionContext, github_api: GitHubAPIResource) -> dict[str, Any]:
        return fetch_repo_metadata(context, github_api, owner=owner, repo=repo)

    return repo_metadata


# Creating assets using the generic function
delta_rs_metadata = create_repo_metadata_asset(
    'delta-io',
    'delta-rs',
    ['stage', 'github', 'repositories', 'delta-io', 'delta-rs'],
    'delta-rs_repo_metadata'
)
iceberg_python_metadata = create_repo_metadata_asset(
    'apache',
    'iceberg-python',
    ['stage', 'github', 'repositories', 'apache', 'iceberg-python'],
    'iceberg-python_repo_metadata'
)
hudi_rs_metadata = create_repo_metadata_asset(
    'apache',
    'hudi-rs',
    ['stage', 'github', 'repositories', 'apache', 'hudi-rs'],
    'hudi-rs_repo_metadata'
)


@asset(
    key_prefix=['dm', 'reports'],
    ins={
        'delta_rs': AssetIn(AssetKey('delta-rs_repo_metadata')),
        'iceberg_python': AssetIn(AssetKey('iceberg-python_repo_metadata')),
        'hudi_rs': AssetIn(AssetKey('hudi-rs_repo_metadata')),
    },
    io_manager_key='md_io_manager',
    group_name='github',
)
def repo_report(
        context: AssetExecutionContext,
        delta_rs: dict[str, Any],
        iceberg_python: dict[str, Any],
        hudi_rs: dict[str, Any]) -> str:
    """
        Generate a markdown report comparing multiple GitHub repositories.

        This asset creates a comparative report for the provided GitHub repositories,
        including 'delta-rs', 'iceberg-python', and 'hudi-rs'. The metadata for each
        repository is passed as input, and the function generates a markdown-formatted
        report.

        Args:
            context (AssetExecutionContext): The execution context provided by Dagster.
            delta_rs (dict[str, Any]): Metadata for the 'delta-rs' repository, including details like stars, forks etc.
            iceberg_python (dict[str, Any]): Metadata for the 'iceberg-python' repository, similar to delta_rs.
            hudi_rs (dict[str, Any]): Metadata for the 'hudi-rs' repository, similar to delta_rs.

        Returns:
            str: A markdown-formatted string that compares the three GitHub repositories.
        """

    report_data = {
        'delta-rs': delta_rs,
        'iceberg-python': iceberg_python,
        'hudi-rs': hudi_rs,
    }
    return create_markdown_report(context, report_data)
