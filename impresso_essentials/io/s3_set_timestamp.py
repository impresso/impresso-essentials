"""
This script processes a .jsonl file stored in an S3 bucket to extract the latest
timestamp from a specified key in the file's records. It then updates the S3
object's metadata with the extracted timestamp. Optionally, the updated metadata
can be written to a new S3 location.

Usage:
    python s3_set_timestamp.py --s3-file s3://bucket/path/file.jsonl.bz2 \
        --metadata-key impresso-last-ts \
        --ts-key ts \
        --all-lines \
        --output s3://bucket/path/output.jsonl

    python s3_set_timestamp.py --s3-prefix s3://bucket/path/ \
        --metadata-key impresso-last-ts \
        --ts-key ts \
        --all-lines

Arguments:
    --s3-prefix: The S3 prefix to process multiple .jsonl.bz2 files.
    --s3-file: The S3 URI of a single .jsonl.bz2 file to process.
    --metadata-key: The metadata key to update with the latest timestamp
        (default: impresso-last-ts).
    --ts-key: The key in the JSONL records to extract the timestamp from
        (default: ts).
    --all-lines: If False, only the first timestamp is considered.
    --output: Optional S3 URI for the output file with updated metadata (only for --s3-file).
    --force: Force reprocessing even if metadata is already up-to-date (default: False).
"""

import os
import json
import argparse
import tempfile
from datetime import datetime
from urllib.parse import urlparse
import logging
import bz2
import signal
from contextlib import contextmanager
import boto3
from botocore.client import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)-15s %(filename)s:%(lineno)d %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


@contextmanager
def disable_interrupts():
    """Context manager to temporarily disable keyboard interrupts."""
    original_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, original_handler)


def get_s3_client() -> "boto3.client":
    """
    Creates and returns a boto3 S3 client configured with credentials and endpoint.

    The client is configured using environment variables:
        - SE_ACCESS_KEY: AWS access key ID.
        - SE_SECRET_KEY: AWS secret access key.
        - SE_HOST_URL: S3 endpoint URL (default: https://os.zhdk.cloud.switch.ch/).
        - SE_REGION: AWS region (default: us-east-1).

    Returns:
        boto3.client: A configured S3 client instance.
    """
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("SE_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SE_SECRET_KEY"),
        endpoint_url=os.getenv("SE_HOST_URL", "https://os.zhdk.cloud.switch.ch/"),
        region_name=os.getenv("SE_REGION", "us-east-1"),  # sometimes required by boto3
        config=Config(signature_version="s3v4"),
    )


def get_last_timestamp(fileobj, ts_key: str, all_lines: bool) -> str:
    """
    Extracts the latest timestamp from a .jsonl file based on the specified key.

    Args:
        fileobj: The file object or path to the .jsonl file (supports .bz2 compression).
        ts_key: The key in the JSONL records to extract the timestamp from.
        all_lines: If False, only the first timestamp is considered.

    Returns:
        str: The latest timestamp in ISO 8601 format (e.g., '2023-01-01T12:00:00Z').

    Raises:
        ValueError: If no valid timestamp is found or the key format is unknown.
    """
    latest_ts = None
    known_formats = {
        "ts": "%Y-%m-%dT%H:%M:%SZ",
        "cdt": "%Y-%m-%d %H:%M:%S",
        "timestamp": "%Y-%m-%dT%H:%M:%SZ",
    }
    skipped_records = 0

    try:
        fmt = known_formats.get(ts_key)
        if not fmt:
            raise ValueError(f"Unknown timestamp format for key: {ts_key}")

        log.debug("Processing file for timestamps with key '%s'", ts_key)

        # Handle .bz2 decompression
        with bz2.open(fileobj, "rb") as f:
            for line in f:
                try:
                    record = json.loads(line.decode("utf-8"))
                    ts_str = (
                        record.get(ts_key)
                        or record.get("cdt")
                        or record.get("timestamp")
                        or record.get("impresso_language_identifier_version").get()
                    )
                    if ts_str:
                        # Determine the correct format for parsing
                        for key, format_str in known_formats.items():
                            try:
                                parsed = datetime.strptime(ts_str, format_str)
                                ts_str = parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
                                break
                            except ValueError:
                                continue
                        else:
                            raise ValueError(f"Timestamp format not recognized: {ts_str}")

                        if not all_lines:
                            log.debug("Taking the first timestamp: %s", ts_str)
                            return ts_str
                        if latest_ts is None or parsed > latest_ts:
                            latest_ts = parsed
                            log.debug("Updated latest timestamp to: %s", latest_ts)
                except (ValueError, TypeError, json.JSONDecodeError) as e:
                    skipped_records += 1
                    log.warning("Skipping invalid record: %s. Line content: %s", e, line[:100])
                    continue

        if not latest_ts:
            log.warning("No valid timestamp found in records. Using file modification date.")
            mod_time = datetime.utcfromtimestamp(os.path.getmtime(fileobj))
            return mod_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    except Exception as e:
        log.error("Error processing timestamps: %s", e)
        raise ValueError(f"Error processing timestamps: {e}")

    log.debug(
        "Final latest timestamp: %s. Total skipped records: %d",
        latest_ts,
        skipped_records,
    )
    return latest_ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def update_metadata_if_needed(
    s3_uri: str,
    metadata_key: str,
    ts_key: str,
    all_lines: bool,
    output_s3_uri: str = None,
    force: bool = False,
):
    """
    Updates the metadata of an S3 object with the latest timestamp from a .jsonl file.

    Args:
        s3_uri: The S3 URI of the .jsonl file to process.
        metadata_key: The metadata key to update with the latest timestamp.
        ts_key: The key in the JSONL records to extract the timestamp from.
        all_lines: If False, only the first timestamp is considered.
        output_s3_uri: Optional S3 URI for the output file with updated metadata.
        force: Force reprocessing even if metadata is already up-to-date.

    Returns:
        None

    Raises:
        ValueError: If the timestamp extraction or metadata update fails.
    """
    parsed = urlparse(s3_uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    log.debug("Fetching S3 object metadata for: %s", s3_uri)

    s3 = get_s3_client()

    # Check if the metadata key exists before downloading the file
    head = s3.head_object(Bucket=bucket, Key=key)
    existing_metadata = head.get("Metadata", {})

    if metadata_key in existing_metadata and not force:
        log.info("[SKIP] Metadata key '%s' already exists.", metadata_key)
        raise ValueError("Metadata key already exists.")

    # Proceed with downloading the file only if the metadata key does not exist
    with tempfile.NamedTemporaryFile(mode="w+b", delete=True) as tmp:
        log.debug("Downloading S3 object to temporary file")
        s3.download_fileobj(bucket, key, tmp)
        tmp.seek(0)
        latest_ts = get_last_timestamp(
            tmp.name, ts_key, all_lines
        )  # Pass the file path to smart_open

    log.debug("Latest timestamp extracted: %s", latest_ts)

    # Create a backup of the original file
    backup_key = f"{key}.backup"
    log.debug("Creating backup of the original file: %s", backup_key)
    with disable_interrupts():
        s3.copy_object(
            Bucket=bucket,
            Key=backup_key,
            CopySource={"Bucket": bucket, "Key": key},
        )

    # Verify the checksum of the backup matches the original file
    original_head = s3.head_object(Bucket=bucket, Key=key)
    backup_head = s3.head_object(Bucket=bucket, Key=backup_key)

    if original_head.get("ETag") != backup_head.get("ETag"):
        log.error("Backup checksum mismatch! Aborting process.")
        raise ValueError(
            "Backup checksum mismatch. The backup file is not identical to the " "original."
        )

    log.debug("Backup checksum verified successfully.")

    updated_metadata = existing_metadata.copy()
    updated_metadata[metadata_key] = latest_ts

    log.debug("[UPDATE] Setting %s=%s on %s", metadata_key, latest_ts, s3_uri)

    destination_bucket = bucket
    destination_key = key

    if output_s3_uri:
        output_parsed = urlparse(output_s3_uri)
        destination_bucket = output_parsed.netloc
        destination_key = output_parsed.path.lstrip("/")

    with disable_interrupts():
        s3.copy_object(
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={"Bucket": bucket, "Key": key},
            Metadata=updated_metadata,
            MetadataDirective="REPLACE",
            ContentType=head.get("ContentType", "application/octet-stream"),
        )

    # Compare checksums of the updated file and the backup
    updated_head = s3.head_object(Bucket=bucket, Key=key)

    if updated_head.get("ETag") == backup_head.get("ETag"):
        log.debug("Checksum match confirmed. Deleting backup file: %s", backup_key)
        try:
            with disable_interrupts():
                s3.delete_object(Bucket=bucket, Key=backup_key)
            log.debug("Backup file deleted successfully: %s", backup_key)
        except Exception as e:
            log.warning("Failed to delete backup file: %s. Error: %s", backup_key, e)
    else:
        log.error("Checksum mismatch! Backup file retained: %s", backup_key)
        raise ValueError("Checksum mismatch between updated file and backup.")

    log.debug("[DONE] Metadata updated.")


def compute_statistics(skipped: int, processed: int):
    """
    Compute and log overall statistics for the files processed.

    Args:
        skipped (int): Number of files skipped.
        processed (int): Number of files processed.

    Returns:
        None
    """
    overall = skipped + processed
    log.info("Overall statistics:")
    log.info("Total files: %d", overall)
    log.info("Skipped files: %d", skipped)
    log.info("Processed files: %d", processed)


def update_metadata_for_prefix(
    s3_prefix: str,
    metadata_key: str,
    ts_key: str,
    all_lines: bool,
    force: bool = False,
):
    """
    Updates the metadata for all S3 objects matching a given prefix.

    Args:
        s3_prefix: The S3 prefix to search for .jsonl.bz2 files.
        metadata_key: The metadata key to update with the latest timestamp.
        ts_key: The key in the JSONL records to extract the timestamp from.
        all_lines: If False, only the first timestamp is considered.
        force: Force reprocessing even if metadata is already up-to-date.

    Returns:
        None

    Raises:
        ValueError: If the prefix does not match any files.
    """
    parsed = urlparse(s3_prefix)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    log.debug("Fetching S3 objects with prefix: %s", s3_prefix)

    s3 = get_s3_client()

    # Use a paginator to handle S3 object listing with paging
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    skipped = 0
    processed = 0

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".jsonl.bz2"):
                log.info("Processing file: %s", key)
                s3_uri = f"s3://{bucket}/{key}"
                try:
                    update_metadata_if_needed(
                        s3_uri,
                        metadata_key,
                        ts_key,
                        all_lines,
                        force,
                    )
                    processed += 1
                except ValueError as e:
                    if "already exists" in str(e):
                        log.info("File skipped: %s", key)
                        skipped += 1
                    else:
                        log.warning("Skipping file due to error: %s", e)
                        skipped += 1

    compute_statistics(skipped, processed)


def main():
    """
    Parses command-line arguments and triggers the metadata update process.

    This function handles the following arguments:
        - --s3-prefix: The S3 prefix to process multiple .jsonl.bz2 files.
        - --s3-file: The S3 URI of a single .jsonl.bz2 file to process.
        - --metadata-key: The metadata key to update with the latest timestamp.
        - --ts-key: The key in the JSONL records to extract the timestamp from.
        - --all-lines: If False, only the first timestamp is considered.
        - --output: Optional S3 URI for the output file with updated metadata.
          Only valid with --s3-file.
        - --force: Force reprocessing even if metadata is already up-to-date.

    Returns:
        None
    """
    parser = argparse.ArgumentParser(
        description=("Update S3 object metadata with the last timestamp in .jsonl file(s).")
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--s3-prefix",
        help=("S3 prefix to process multiple .jsonl.bz2 files (e.g., s3://bucket/path/)."),
    )
    group.add_argument(
        "--s3-file",
        help=(
            "S3 URI of a single .jsonl.bz2 file to process "
            "(e.g., s3://bucket/path/file.jsonl.bz2)."
        ),
    )
    parser.add_argument(
        "--metadata-key",
        default="impresso-last-ts",
        help="S3 metadata key to write (default: %(default)s).",
    )
    parser.add_argument(
        "--ts-key",
        default="ts",
        choices=["ts", "cdt", "timestamp"],
        help=("Key to look for the timestamp in each JSONL record (default: %(default)s)."),
    )
    parser.add_argument(
        "--all-lines",
        action="store_true",
        help="If set, searches all lines for the latest timestamp. Defaults to False.",
    )
    parser.add_argument(
        "--output",
        help=(
            "Optional S3 URI for the output file with updated metadata. "
            "Only valid with --s3-file."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=("Force reprocessing even if metadata is already up-to-date " "(default: False)."),
    )

    args = parser.parse_args()

    if args.s3_prefix:
        if args.output:
            parser.error("The --output option is not allowed with --s3-prefix.")
        update_metadata_for_prefix(
            args.s3_prefix,
            metadata_key=args.metadata_key,
            ts_key=args.ts_key,
            all_lines=args.all_lines,
            force=args.force,
        )
    elif args.s3_file:
        update_metadata_if_needed(
            args.s3_file,
            metadata_key=args.metadata_key,
            ts_key=args.ts_key,
            all_lines=args.all_lines,
            output_s3_uri=args.output,
            force=args.force,
        )


if __name__ == "__main__":
    main()
