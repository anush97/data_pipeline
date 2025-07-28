import json
import logging
import os
import uuid
import base64
from pathlib import Path
from typing import Dict, Any, Optional

import textract
from pydantic import ValidationError

from .metadata_model import NewMetadata
from .s3_adapter import S3Adapter, create_s3_client
from common.decorator import lambda_handler

# Logger setup
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


# Custom exceptions
class PDFExtractionError(Exception):
    pass


class S3PathError(Exception):
    pass


def extract_pdf_base64_from_json(json_data: Dict[str, Any]) -> bytes:
    """
    Extracts the base64-encoded PDF binary from the S3-stored JSON structure.
    """
    content_blocks = json_data.get("Body", {}).get("content", [])
    for block in content_blocks:
        if block.get("contentType", "").startswith("application/pdf"):
            base64_data = block.get("data")
            if base64_data:
                return base64.b64decode(base64_data)
    logger.warning("No PDF data found in Body.content[]. Skipping.")
    return b""


def extract_and_clean_text_from_pdf(pdf_bytes: bytes) -> str:
    """
    Use textract to extract and clean text from PDF binary.
    """
    tmp_pdf_path = Path("/tmp/input.pdf")
    try:
        tmp_pdf_path.write_bytes(pdf_bytes)
        raw_bytes = textract.process(str(tmp_pdf_path))
        raw_text = raw_bytes.decode("utf-8")
        # Clean text
        lines = raw_text.splitlines()
        cleaned = [line.strip() for line in lines if line.strip()]
        return "\n".join(cleaned)
    except Exception as e:
        logger.exception("PDF extraction failed")
        raise PDFExtractionError("Failed to extract or clean PDF content")
    finally:
        if tmp_pdf_path.exists():
            tmp_pdf_path.unlink()


def build_handler(s3_adapter: S3Adapter):
    bucket = os.environ.get("BUCKET_NAME")
    prefix = os.environ.get("PROCESSED_TEXT_PREFIX")

    if not bucket or not prefix:
        raise EnvironmentError("Missing BUCKET_NAME or PROCESSED_TEXT_PREFIX")

    @lambda_handler(
        error_status=[
            (ValidationError, 400),
            (PDFExtractionError, 500),
            (S3PathError, 400),
        ],
        logging_fn=logger.error,
    )
    def handler(
        event: Dict[str, Any], context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        logger.info(f"Received event: {json.dumps(event)}")

        # Validate input
        try:
            model = NewMetadata(**event)
        except ValidationError as e:
            raise e

        if model.content_type != "pdf":
            logger.info(f"Skipping non-PDF content_type={model.content_type}")
            return event

        # Parse raw_path: s3://bucket/key
        raw_path = model.raw_path
        if not raw_path.startswith("s3://"):
            raise S3PathError(f"Invalid raw_path format: {raw_path}")

        _, s3_path = raw_path.split("s3://", 1)
        raw_bucket, raw_key = s3_path.split("/", 1)

        # Read and parse the JSON document from S3
        response = s3_adapter.try_get_object(raw_bucket, raw_key)
        json_doc = json.loads(response["Body"].read().decode("utf-8"))

        # Extract base64 PDF from JSON
        pdf_bytes = extract_pdf_base64_from_json(json_doc)
        if not pdf_bytes:
            logger.warning("Empty PDF content. Skipping document.")
            return event

        # Extract and clean text from PDF
        cleaned_text = extract_and_clean_text_from_pdf(pdf_bytes)

        # Save cleaned text to S3
        txt_key = f"{prefix}/{uuid.uuid4()}.txt"
        s3_adapter.s3_client.put_object(
            Bucket=bucket,
            Key=txt_key,
            Body=cleaned_text.encode("utf-8"),
            ContentType="text/plain",
        )
        logger.info(f"Saved cleaned text to: s3://{bucket}/{txt_key}")

        # Return enriched result
        result = event.copy()
        result["content_path"] = f"s3://{bucket}/{txt_key}"
        return result

    return handler


# Register handler unless in test mode
if not bool(os.environ.get("TEST_FLAG", False)):
    _s3 = create_s3_client()
    _s3_adapter = S3Adapter(_s3)
    handler = build_handler(_s3_adapter)
