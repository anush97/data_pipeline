import base64
import json
import pytest
import boto3
from moto import mock_aws
from pydantic import ValidationError
from unittest.mock import patch
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
    with mock_aws():
        client = boto3.client("s3", region_name="ca-central-1")
        client.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "ca-central-1"},
        )
        yield client

@pytest.fixture
def s3_adapter(s3_mock):
    return S3Adapter(s3_mock)

@pytest.fixture
def valid_event():
    return {
        "metadata": {
            "@unid": "12345",
            "region": "ON",
            "Province": "Ontario",
            "res_title": "MyTitle",
            "file_name": "12345__MyTitle.pdf",
            "url": "http://example.com",
            "Subject": "Test",
            "CategoryV2": "Cat",
            "SectionNameV2": "Sec",
            "SubSectionNameV2": "SubSec",
            "Langue": "en",  # Corrected case for 'Langue'
            "Status": "Published",  # Corrected case for 'Status'
        },
        "raw_path": f"s3://{TEST_BUCKET}/inputs/test.json",
        "content_type": "pdf",
    }

@pytest.fixture
def sample_pdf_json():
    fake_pdf = base64.b64encode(b"Fake PDF content for testing\nAnother Line").decode(
        "utf-8"
    )
    return {"Body": {"content": [{"contentType": "application/pdf", "data": fake_pdf}]}}


# ---------------- Tests ---------------- #


def test_extract_pdf_base64_success(sample_pdf_json):
    result = extract_pdf_base64_from_json(sample_pdf_json)
    assert isinstance(result, bytes)


def test_pdf_cleaning_logic():
    with patch("textract.process") as mock_textract:
        mock_textract.return_value = b"Line1\n\nLine2  \n   \nLine3"
        result = extract_and_clean_text_from_pdf(b"Fake PDF content")
        assert "Line1" in result and "Line2" in result and "Line3" in result
        assert "\n\n" not in result


def test_handler_valid_pdf(setup_env, s3_mock, s3_adapter, valid_event, sample_pdf_json):
    # Upload the sample JSON to the mock S3 bucket
    s3_mock.put_object(
        Bucket=TEST_BUCKET,
        Key="inputs/test.json",
        Body=json.dumps(sample_pdf_json).encode("utf-8"),
    )

    # Build the handler
    handler = build_handler(s3_adapter)

    # Call the handler with the valid event
    result = handler(valid_event, None)

    # Assertions to verify the result
    assert "content_path" in result
    assert result["content_type"] == "pdf"
    assert result["statusCode"] == 200

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
Error-
(.venv) ~/build/ai-in-pl-agent-assist-qna-data-pipeline git:[feature/AAQA-1247-Create-Lambda-to-extract-text-from-a-PDF-content]
pytest tests/unit/pdf_text_extractor//test_pdf_text_extractor.py
================================================================================================== test session starts ==================================================================================================
platform linux -- Python 3.13.5, pytest-8.4.1, pluggy-1.6.0 -- /home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/bin/python
cachedir: .pytest_cache
rootdir: /home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline
configfile: pytest.ini
plugins: requests-mock-1.12.1, anyio-4.9.0, httpx-0.35.0, cov-6.2.1
collected 7 items                                                                                                                                                                                                       

tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_extract_pdf_base64_success PASSED                                                                                                                  [ 14%]
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_pdf_cleaning_logic PASSED                                                                                                                          [ 28%]
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_valid_pdf FAILED                                                                                                                           [ 42%]
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_skips_non_pdf PASSED                                                                                                                       [ 57%]
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_invalid_raw_path FAILED                                                                                                                    [ 71%]
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_invalid_metadata_format FAILED                                                                                                             [ 85%]
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_pdf_extraction_failure PASSED                                                                                                                      [100%]

======================================================================================================= FAILURES ========================================================================================================
________________________________________________________________________________________________ test_handler_valid_pdf _________________________________________________________________________________________________

setup_env = None, s3_mock = <botocore.client.S3 object at 0x7b380768cec0>, s3_adapter = <lambdas.pdf_text_extractor.s3_adapter.S3Adapter object at 0x7b3806772660>
valid_event = {'content_type': 'pdf', 'metadata': {'@unid': '12345', 'CategoryV2': 'Cat', 'Langue': 'en', 'Province': 'Ontario', ...}, 'raw_path': 's3://test-bucket/inputs/test.json'}
sample_pdf_json = {'Body': {'content': [{'contentType': 'application/pdf', 'data': 'RmFrZSBQREYgY29udGVudCBmb3IgdGVzdGluZwpBbm90aGVyIExpbmU='}]}}

    def test_handler_valid_pdf(setup_env, s3_mock, s3_adapter, valid_event, sample_pdf_json):
        # Upload the sample JSON to the mock S3 bucket
        s3_mock.put_object(
            Bucket=TEST_BUCKET,
            Key="inputs/test.json",
            Body=json.dumps(sample_pdf_json).encode("utf-8"),
        )
    
        # Build the handler
        handler = build_handler(s3_adapter)
    
        # Call the handler with the valid event
        result = handler(valid_event, None)
    
        # Assertions to verify the result
>       assert "content_path" in result
E       assert 'content_path' in {'body': '{"errorMessage": "An internal error occurred."}', 'statusCode': 500}

tests/unit/pdf_text_extractor/test_pdf_text_extractor.py:106: AssertionError
-------------------------------------------------------------------------------------------------- Captured log setup ---------------------------------------------------------------------------------------------------
INFO     botocore.credentials:credentials.py:1213 Found credentials in environment variables.
--------------------------------------------------------------------------------------------------- Captured log call ---------------------------------------------------------------------------------------------------
INFO     root:pdf_text_extractor_handler.py:81 Received event: {"metadata": {"@unid": "12345", "region": "ON", "Province": "Ontario", "res_title": "MyTitle", "file_name": "12345__MyTitle.pdf", "url": "http://example.com", "Subject": "Test", "CategoryV2": "Cat", "SectionNameV2": "Sec", "SubSectionNameV2": "SubSec", "Langue": "en", "Status": "Published"}, "raw_path": "s3://test-bucket/inputs/test.json", "content_type": "pdf"}
WARNING  lambdas.pdf_text_extractor.metadata_model:metadata_model.py:51 Unknown content type 'None' for document with UNID 'None'. Returning empty file name.
WARNING  lambdas.pdf_text_extractor.metadata_model:metadata_model.py:86 Unknown content type '' for document with UNID 'None'. Returning empty URL.
ERROR    root:pdf_text_extractor_handler.py:59 PDF extraction failed
Traceback (most recent call last):
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/lambdas/pdf_text_extractor/pdf_text_extractor_handler.py", line 51, in extract_and_clean_text_from_pdf
    raw_bytes = textract.process(str(tmp_pdf_path))
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/__init__.py", line 79, in process
    return parser.process(filename, input_encoding, output_encoding, **kwargs)
           ~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/utils.py", line 46, in process
    byte_string = self.extract(filename, **kwargs)
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/pdf_parser.py", line 32, in extract
    raise ex
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/pdf_parser.py", line 24, in extract
    return self.extract_pdftotext(filename, **kwargs)
           ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/pdf_parser.py", line 47, in extract_pdftotext
    stdout, _ = self.run(args)
                ~~~~~~~~^^^^^^
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/utils.py", line 106, in run
    raise exceptions.ShellError(
        ' '.join(args), pipe.returncode, stdout, stderr,
    )
textract.exceptions.ShellError: The command `pdftotext /tmp/input.pdf -` failed with exit code 1
------------- stdout -------------
b''------------- stderr -------------
b"Syntax Warning: May not be a PDF file (continuing anyway)\nSyntax Error: Couldn't find trailer dictionary\nSyntax Error: Couldn't find trailer dictionary\nSyntax Error: Couldn't read xref table\n"
ERROR    root:decorator.py:64 Error: PDFExtractionError('Failed to extract or clean PDF content')
Traceback (most recent call last):
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/lambdas/pdf_text_extractor/pdf_text_extractor_handler.py", line 51, in extract_and_clean_text_from_pdf
    raw_bytes = textract.process(str(tmp_pdf_path))
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/__init__.py", line 79, in process
    return parser.process(filename, input_encoding, output_encoding, **kwargs)
           ~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/utils.py", line 46, in process
    byte_string = self.extract(filename, **kwargs)
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/pdf_parser.py", line 32, in extract
    raise ex
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/pdf_parser.py", line 24, in extract
    return self.extract_pdftotext(filename, **kwargs)
           ~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/pdf_parser.py", line 47, in extract_pdftotext
    stdout, _ = self.run(args)
                ~~~~~~~~^^^^^^
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/textract/parsers/utils.py", line 106, in run
    raise exceptions.ShellError(
        ' '.join(args), pipe.returncode, stdout, stderr,
    )
textract.exceptions.ShellError: The command `pdftotext /tmp/input.pdf -` failed with exit code 1
------------- stdout -------------
b''------------- stderr -------------
b"Syntax Warning: May not be a PDF file (continuing anyway)\nSyntax Error: Couldn't find trailer dictionary\nSyntax Error: Couldn't find trailer dictionary\nSyntax Error: Couldn't read xref table\n"

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/common/decorator.py", line 49, in wrapped
    response = fn(event, *args, **kwargs)
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/lambdas/pdf_text_extractor/pdf_text_extractor_handler.py", line 112, in handler
    cleaned_text = extract_and_clean_text_from_pdf(pdf_bytes)
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/lambdas/pdf_text_extractor/pdf_text_extractor_handler.py", line 60, in extract_and_clean_text_from_pdf
    raise PDFExtractionError("Failed to extract or clean PDF content") from e
lambdas.pdf_text_extractor.pdf_text_extractor_handler.PDFExtractionError: Failed to extract or clean PDF content
_____________________________________________________________________________________________ test_handler_invalid_raw_path _____________________________________________________________________________________________

setup_env = None, s3_adapter = <lambdas.pdf_text_extractor.s3_adapter.S3Adapter object at 0x7b3808098190>
valid_event = {'content_type': 'pdf', 'metadata': {'@unid': '12345', 'CategoryV2': 'Cat', 'Langue': 'en', 'Province': 'Ontario', ...}, 'raw_path': 'invalid-path'}

    def test_handler_invalid_raw_path(setup_env, s3_adapter, valid_event):
        valid_event["raw_path"] = "invalid-path"
        handler = build_handler(s3_adapter)
>       with pytest.raises(S3PathError):
             ^^^^^^^^^^^^^^^^^^^^^^^^^^
E       Failed: DID NOT RAISE <class 'lambdas.pdf_text_extractor.pdf_text_extractor_handler.S3PathError'>

tests/unit/pdf_text_extractor/test_pdf_text_extractor.py:120: Failed
-------------------------------------------------------------------------------------------------- Captured log setup ---------------------------------------------------------------------------------------------------
INFO     botocore.credentials:credentials.py:1213 Found credentials in environment variables.
--------------------------------------------------------------------------------------------------- Captured log call ---------------------------------------------------------------------------------------------------
INFO     root:pdf_text_extractor_handler.py:81 Received event: {"metadata": {"@unid": "12345", "region": "ON", "Province": "Ontario", "res_title": "MyTitle", "file_name": "12345__MyTitle.pdf", "url": "http://example.com", "Subject": "Test", "CategoryV2": "Cat", "SectionNameV2": "Sec", "SubSectionNameV2": "SubSec", "Langue": "en", "Status": "Published"}, "raw_path": "invalid-path", "content_type": "pdf"}
WARNING  lambdas.pdf_text_extractor.metadata_model:metadata_model.py:51 Unknown content type 'None' for document with UNID 'None'. Returning empty file name.
WARNING  lambdas.pdf_text_extractor.metadata_model:metadata_model.py:86 Unknown content type '' for document with UNID 'None'. Returning empty URL.
ERROR    root:decorator.py:64 Error: S3PathError('Invalid raw_path format: invalid-path')
Traceback (most recent call last):
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/common/decorator.py", line 49, in wrapped
    response = fn(event, *args, **kwargs)
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/lambdas/pdf_text_extractor/pdf_text_extractor_handler.py", line 96, in handler
    raise S3PathError(f"Invalid raw_path format: {raw_path}")
lambdas.pdf_text_extractor.pdf_text_extractor_handler.S3PathError: Invalid raw_path format: invalid-path
_________________________________________________________________________________________ test_handler_invalid_metadata_format __________________________________________________________________________________________

setup_env = None, s3_adapter = <lambdas.pdf_text_extractor.s3_adapter.S3Adapter object at 0x7b3804e07820>

    def test_handler_invalid_metadata_format(setup_env, s3_adapter):
        handler = build_handler(s3_adapter)
>       with pytest.raises(ValidationError):
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E       Failed: DID NOT RAISE <class 'pydantic_core._pydantic_core.ValidationError'>

tests/unit/pdf_text_extractor/test_pdf_text_extractor.py:126: Failed
-------------------------------------------------------------------------------------------------- Captured log setup ---------------------------------------------------------------------------------------------------
INFO     botocore.credentials:credentials.py:1213 Found credentials in environment variables.
--------------------------------------------------------------------------------------------------- Captured log call ---------------------------------------------------------------------------------------------------
INFO     root:pdf_text_extractor_handler.py:81 Received event: {"foo": "bar"}
ERROR    root:decorator.py:64 Error: 3 validation errors for NewMetadata
metadata
  Field required [type=missing, input_value={'foo': 'bar'}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.11/v/missing
raw_path
  Field required [type=missing, input_value={'foo': 'bar'}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.11/v/missing
content_type
  Field required [type=missing, input_value={'foo': 'bar'}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.11/v/missing
Traceback (most recent call last):
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/common/decorator.py", line 49, in wrapped
    response = fn(event, *args, **kwargs)
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/lambdas/pdf_text_extractor/pdf_text_extractor_handler.py", line 87, in handler
    raise e
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/lambdas/pdf_text_extractor/pdf_text_extractor_handler.py", line 85, in handler
    model = NewMetadata(**event)
  File "/home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/pydantic/main.py", line 253, in __init__
    validated_self = self.__pydantic_validator__.validate_python(data, self_instance=self)
pydantic_core._pydantic_core.ValidationError: 3 validation errors for NewMetadata
metadata
  Field required [type=missing, input_value={'foo': 'bar'}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.11/v/missing
raw_path
  Field required [type=missing, input_value={'foo': 'bar'}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.11/v/missing
content_type
  Field required [type=missing, input_value={'foo': 'bar'}, input_type=dict]
    For further information visit https://errors.pydantic.dev/2.11/v/missing
=================================================================================================== warnings summary ====================================================================================================
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_valid_pdf
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_valid_pdf
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_valid_pdf
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_skips_non_pdf
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_invalid_raw_path
tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_invalid_metadata_format
  /home/anushka-sharma/build/ai-in-pl-agent-assist-qna-data-pipeline/.venv/lib/python3.13/site-packages/botocore/auth.py:422: DeprecationWarning: datetime.datetime.utcnow() is deprecated and scheduled for removal in a future version. Use timezone-aware objects to represent datetimes in UTC: datetime.datetime.now(datetime.UTC).
    datetime_now = datetime.datetime.utcnow()

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
================================================================================================ short test summary info ================================================================================================
FAILED tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_valid_pdf - assert 'content_path' in {'body': '{"errorMessage": "An internal error occurred."}', 'statusCode': 500}
FAILED tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_invalid_raw_path - Failed: DID NOT RAISE <class 'lambdas.pdf_text_extractor.pdf_text_extractor_handler.S3PathError'>
FAILED tests/unit/pdf_text_extractor/test_pdf_text_extractor.py::test_handler_invalid_metadata_format - Failed: DID NOT RAISE <class 'pydantic_core._pydantic_core.ValidationError'>
======================================================================================== 3 failed, 4 passed, 6 warnings in 2.19s ========================================================================================
