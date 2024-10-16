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
        "delta-rs": {"stars": 2252, "forks": 416},
        "hudi-rs": {"stars": 400, "forks": 28}
    }
    expected_md = pd.DataFrame.from_dict(report_data).to_markdown()

    # Act
    md_report = create_markdown_report(context, report_data)

    # Assert
    assert md_report.strip() == expected_md.strip()  # Check the markdown output
    context.add_output_metadata.assert_called_once()  # Check if metadata was added
    # Verify that the correct metadata is added
    context.add_output_metadata.assert_called_with({"report": MetadataValue.md(md_report)})
