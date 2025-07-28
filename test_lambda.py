import base64
import json
import pytest
import boto3
from moto import mock_s3
from pydantic import ValidationError

from lambdas.pdf_text_extractor.pdf_text_extractor_handler import (
    build_handler,
    extract_pdf_base64_from_json,
    extract_and_clean_text_from_pdf,
    PDFExtractionError,
    S3PathError,
)
from lambdas.pdf_text_extractor.s3_adapter import S3Adapter

TEST_BUCKET = "test-bucket"
TEST_PREFIX = "flattened"

# ---------------- Fixtures ---------------- #

@pytest.fixture(scope="module", autouse=True)
def setup_env():
    import os
    os.environ["BUCKET_NAME"] = TEST_BUCKET
    os.environ["PROCESSED_TEXT_PREFIX"] = TEST_PREFIX
    os.environ["LOG_LEVEL"] = "INFO"

@pytest.fixture
def s3_mock():
    with mock_s3():
        client = boto3.client("s3", region_name="ca-central-1")
        client.create_bucket(Bucket=TEST_BUCKET)
        yield client

@pytest.fixture
def s3_adapter(s3_mock):
    return S3Adapter(s3_mock)

@pytest.fixture
def valid_event():
    return {
        "metadata": {
            "unid": "12345",
            "region": "ON",
            "title": "MyTitle",
            "file_name": "12345__MyTitle.pdf",
            "url": "http://example.com",
            "subject": "Test",
            "category": "Cat",
            "section_name": "Sec",
            "subsection_name": "SubSec",
            "langue": "en",
            "status": "Published",
        },
        "raw_path": f"s3://{TEST_BUCKET}/inputs/test.json",
        "content_type": "pdf",
    }

@pytest.fixture
def sample_pdf_json():
    fake_pdf = base64.b64encode(b"Fake PDF content for testing\nAnother Line").decode("utf-8")
    return {
        "Body": {
            "content": [
                {
                    "contentType": "application/pdf",
                    "data": fake_pdf
                }
            ]
        }
    }

# ---------------- Tests ---------------- #

def test_extract_pdf_base64_success(sample_pdf_json):
    result = extract_pdf_base64_from_json(sample_pdf_json)
    assert isinstance(result, bytes)
    assert b"Fake PDF content" in result

def test_pdf_cleaning_logic():
    result = extract_and_clean_text_from_pdf(b"Line1\n\nLine2  \n   \nLine3")
    assert "Line1" in result and "Line2" in result and "Line3" in result
    assert "\n\n" not in result

def test_handler_valid_pdf(setup_env, s3_mock, s3_adapter, valid_event, sample_pdf_json):
    s3_mock.put_object(
        Bucket=TEST_BUCKET,
        Key="inputs/test.json",
        Body=json.dumps(sample_pdf_json).encode("utf-8")
    )
    handler = build_handler(s3_adapter)
    result = handler(valid_event, None)
    assert "content_path" in result
    assert result["content_type"] == "pdf"

def test_handler_skips_non_pdf(setup_env, s3_adapter, valid_event):
    valid_event["content_type"] = "html"
    handler = build_handler(s3_adapter)
    result = handler(valid_event, None)
    assert "content_path" not in result

def test_handler_invalid_raw_path(setup_env, s3_adapter, valid_event):
    valid_event["raw_path"] = "invalid-path"
    handler = build_handler(s3_adapter)
    with pytest.raises(S3PathError):
        handler(valid_event, None)

def test_handler_invalid_metadata_format(setup_env, s3_adapter):
    handler = build_handler(s3_adapter)
    with pytest.raises(ValidationError):
        handler({"foo": "bar"}, None)

def test_pdf_extraction_failure():
    with pytest.raises(PDFExtractionError):
        extract_and_clean_text_from_pdf(b"not-a-real-pdf")
