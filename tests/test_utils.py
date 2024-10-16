import pandas as pd

from dagster import AssetExecutionContext, MetadataValue
from github_pipeline.utils import create_markdown_report


def test_create_markdown_report(mocker):
    """
    Test the creation of a markdown report from report data.

    This test verifies the functionality of the `create_markdown_report`
    function, ensuring that it correctly converts a dictionary of report
    data into a markdown formatted string. It also checks that the
    appropriate metadata is added to the execution context.

    Args:
        mocker (pytest_mock.MockerFixture): The mocker fixture provided
        by pytest-mock to create mock objects and methods.

    Asserts:
        - The generated markdown report matches the expected markdown
          format based on the input data.
        - The `add_output_metadata` method of the AssetExecutionContext
          is called exactly once after creating the report.
        - The correct metadata containing the markdown report is added
          to the context.

    Example:
        This test will simulate the creation of a markdown report for
        two repositories with specified star and fork counts, then
        assert that the output markdown is correctly formatted and that
        the appropriate metadata is logged in the context.
    """
    # Arrange
    context = mocker.Mock(spec=AssetExecutionContext)
    report_data = {
        "delta-rs": {
            "html_url": "https://github.com/delta-io/delta-rs",
            "stars": 123,
            "forks": 45,
            "watchers": 10,
            "releases": 5,
            "open_issues": 20,
            "closed_issues": 15,
            "avg_days_until_issue_was_closed": 3.2,
            "open_prs": 5,
            "closed_prs": 10,
            "avg_days_until_pr_was_closed": 2.5,},
        "hudi-rs": {
            "html_url": "https://github.com/apache/hudi-rs",
            "stars": 11,
            "forks": 435,
            "watchers": 1245,
            "releases": 512,
            "open_issues": 325,
            "closed_issues": 111,
            "avg_days_until_issue_was_closed": 1.2,
            "open_prs": 5,
            "closed_prs": 120,
            "avg_days_until_pr_was_closed": 21.5,
        }
    }
    expected_md = pd.DataFrame.from_dict(report_data).drop("html_url", axis=0).to_markdown()

    # Act
    md_report = create_markdown_report(context, report_data)

    # Assert
    assert md_report.strip() == expected_md.strip()  # Check the markdown output
    context.add_output_metadata.assert_called_once()  # Check if metadata was added
    # Verify that the correct metadata is added
    context.add_output_metadata.assert_called_with({"report": MetadataValue.md(md_report)})
