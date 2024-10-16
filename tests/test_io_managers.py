import json

import boto3
import pytest
from moto import mock_aws
from upath import UPath

from dagster import InputContext, OutputContext
from github_pipeline.io_managers import JsonObjectS3IOManager, TextObjectS3IOManager

# Mock constants
BUCKET_NAME = "test-bucket"
S3_PREFIX = "test-prefix"

@pytest.fixture
def mock_s3_client():
    """
    Fixture to create a mocked S3 client using Moto.

    This fixture sets up a mocked AWS environment with an S3 bucket
    created specifically for testing purposes. It ensures that
    S3 operations can be performed without affecting real AWS resources.

    Yields:
        boto3.client: A mocked S3 client.
    """
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET_NAME)
        yield s3

def test_jsonobject_s3_io_manager_load(mocker, mock_s3_client):
    """
    Test loading a JSON object from S3 using JsonObjectS3IOManager.

    This test verifies that the `JsonObjectS3IOManager` can successfully
    load a JSON object stored in S3. It mocks the S3 client to simulate
    the presence of a JSON file in S3 and checks that the loaded data
    matches the expected content.

   Args:
       mocker (mocker): The pytest-mock fixture for creating mock objects.
       mock_s3_client (boto3.client): The mocked S3 client.
   """
    # Arrange
    json_manager = JsonObjectS3IOManager(s3_bucket=BUCKET_NAME, s3_session=mock_s3_client, s3_prefix=S3_PREFIX)
    test_data = {"key": "value"}
    json_bytes = json.dumps(test_data).encode("utf-8")

    # Mock S3 file
    mock_s3_client.put_object(Bucket=BUCKET_NAME, Key=f"{S3_PREFIX}/test.json", Body=json_bytes)

    # Create a mock InputContext
    mock_context = mocker.Mock(spec=InputContext)
    path = UPath(f"{S3_PREFIX}/test.json")

    # Act
    loaded_data = json_manager.load_from_path(context=mock_context, path=path)

    # Assert
    assert loaded_data == test_data

def test_jsonobject_s3_io_manager_dump(mocker, mock_s3_client):
    """
    Test dumping a JSON object to S3 using JsonObjectS3IOManager.

    This test verifies that the `JsonObjectS3IOManager` can successfully
    dump a JSON object to S3. It checks that the expected JSON file
    is created in S3 and contains the correct data.

    Args:
        mocker (mocker): The pytest-mock fixture for creating mock objects.
        mock_s3_client (boto3.client): The mocked S3 client.
    """
    # Arrange
    json_manager = JsonObjectS3IOManager(s3_bucket=BUCKET_NAME, s3_session=mock_s3_client, s3_prefix=S3_PREFIX)
    test_data = {"key": "value"}

    # Create a mock OutputContext
    mock_context = mocker.Mock(spec=OutputContext)
    path = UPath(f"{S3_PREFIX}/test.json")

    # Act
    json_manager.dump_to_path(context=mock_context, obj=test_data, path=path)

    # Verify the content in S3
    response = mock_s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{S3_PREFIX}/test.json")
    loaded_data = json.load(response["Body"])

    # Assert
    assert loaded_data == test_data

def test_textobject_s3_io_manager_load(mocker, mock_s3_client):
    """
    Test loading a text object from S3 using TextObjectS3IOManager.

    This test verifies that the `TextObjectS3IOManager` can successfully
    load a text file stored in S3. It mocks the S3 client to simulate
    the presence of a text file in S3 and checks that the loaded text
    matches the expected content.

    Args:
        mocker (mocker): The pytest-mock fixture for creating mock objects.
        mock_s3_client (boto3.client): The mocked S3 client.
    """
    # Arrange
    text_manager = TextObjectS3IOManager(s3_bucket=BUCKET_NAME, s3_session=mock_s3_client, s3_prefix=S3_PREFIX)
    test_text = "Hello, world!"
    text_bytes = test_text.encode("utf-8")

    # Mock S3 file
    mock_s3_client.put_object(Bucket=BUCKET_NAME, Key=f"{S3_PREFIX}/test.txt", Body=text_bytes)

    # Create a mock InputContext
    mock_context = mocker.Mock(spec=InputContext)
    path = UPath(f"{S3_PREFIX}/test.txt")

    # Act
    loaded_text = text_manager.load_from_path(context=mock_context, path=path)

    # Assert
    assert loaded_text == test_text

def test_textobject_s3_io_manager_dump(mocker, mock_s3_client):
    """
    Test dumping a text object to S3 using TextObjectS3IOManager.

    This test verifies that the `TextObjectS3IOManager` can successfully
    dump a text object to S3. It checks that the expected text file is
    created in S3 and contains the correct data.

    Args:
        mocker (mocker): The pytest-mock fixture for creating mock objects.
        mock_s3_client (boto3.client): The mocked S3 client.
    """
    # Arrange
    text_manager = TextObjectS3IOManager(s3_bucket=BUCKET_NAME, s3_session=mock_s3_client, s3_prefix=S3_PREFIX)
    test_text = "Hello, world!"

    # Create a mock OutputContext
    mock_context = mocker.Mock(spec=OutputContext)
    path = UPath(f"{S3_PREFIX}/test.txt")

    # Act
    text_manager.dump_to_path(context=mock_context, obj=test_text, path=path)

    # Verify the content in S3
    response = mock_s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{S3_PREFIX}/test.txt")
    loaded_text = response["Body"].read().decode("utf-8")

    # Assert
    assert loaded_text == test_text
