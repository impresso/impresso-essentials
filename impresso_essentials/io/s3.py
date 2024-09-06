"""Reusable functions to read/write data from/to our S3 drive.
Warning: 2 boto libraries are used, and need to be kept until third party lib dependencies are solved.

"""

import bz2
import json
import logging
import os
import warnings
from typing import Generator, Union

import boto3
from boto3.resources.base import ServiceResource
import botocore
from botocore.client import BaseClient
from dotenv import load_dotenv
from smart_open import open as s_open
from smart_open.s3 import iter_bucket
from collections import namedtuple

from impresso_essentials.utils import bytes_to

logger = logging.getLogger(__name__)


def get_storage_options() -> dict[str, dict | str]:
    """Load environment variables from local .env files

    Assumes that two environment variables are set:
    `SE_ACCESS_KEY` and `SE_SECRET_KEY`.

    Returns:
        dict[str, dict | str]: Credentials to access a S3 endpoint.
    """
    load_dotenv()
    return {
        "client_kwargs": {"endpoint_url": "https://os.zhdk.cloud.switch.ch"},
        "key": os.environ["SE_ACCESS_KEY"],
        "secret": os.environ["SE_SECRET_KEY"],
    }


IMPRESSO_STORAGEOPT = get_storage_options()

IssueDir = namedtuple("IssueDirectory", ["journal", "date", "edition", "path"])


def get_s3_client(
    host_url: str | None = "https://os.zhdk.cloud.switch.ch/",
) -> BaseClient:
    """Create S3 boto3 client using environment variables from local .env files.

    Assumes that two environment variables are set:
    `SE_ACCESS_KEY` and `SE_SECRET_KEY`.

    Args:
        host_url (str | None, optional): _description_. Defaults to
            "https://os.zhdk.cloud.switch.ch/".

    Raises:
        e: Argument `host_url` was not provided and `SE_HOST_URL` was not in the env.
        e: `SE_ACCESS_KEY` or `SE_SECRET_KEY` was not in the environment variables.

    Returns:
        BaseClient: The S3 boto3 client.
    """
    # load environment variables from local .env files
    load_dotenv()
    if host_url is None:
        try:
            host_url = os.environ["SE_HOST_URL"]
        except Exception as e:
            raise e

    try:
        access_key = os.environ["SE_ACCESS_KEY"]
        secret_key = os.environ["SE_SECRET_KEY"]
    except Exception as e:
        raise e

    return boto3.client(
        "s3",
        aws_secret_access_key=secret_key,
        aws_access_key_id=access_key,
        endpoint_url=host_url,
    )


def get_s3_resource(
    host_url: str | None = "https://os.zhdk.cloud.switch.ch/",
) -> ServiceResource:
    """Get a boto3 resource object related to an S3 drive.

    Assumes that two environment variables are set:
    `SE_ACCESS_KEY` and `SE_SECRET_KEY`.

    Args:
        host_url (str | None, optional): _description_. Defaults to
            "https://os.zhdk.cloud.switch.ch/".

    Raises:
        e: Argument `host_url` was not provided and `SE_HOST_URL` was not in the env.
        e: `SE_ACCESS_KEY` or `SE_SECRET_KEY` was not in the environment variables.

    Returns:
        ServiceResource: S3 resource associated to the endpoint.
    """
    # load environment variables from local .env files
    load_dotenv()
    if host_url is None:
        try:
            host_url = os.environ["SE_HOST_URL"]
        except Exception as e:
            raise e

    try:
        access_key = os.environ["SE_ACCESS_KEY"]
        secret_key = os.environ["SE_SECRET_KEY"]
    except Exception as e:
        raise e

    return boto3.resource(
        "s3",
        aws_secret_access_key=secret_key,
        aws_access_key_id=access_key,
        endpoint_url=host_url,
    )


def get_or_create_bucket(name, create=False, versioning=True):
    """Create a boto3 s3 connection and returns the requested bucket.

    It is possible to ask for creating a new bucket
    with the specified name (in case it does not exist), and (optionally)
    to turn on the versioning on the newly created bucket.
    >>> b = get_bucket('testb', create=False)
    >>> b = get_bucket('testb', create=True)
    >>> b = get_bucket('testb', create=True, versioning=False)

    :param name: the bucket's name
    :type name: string
    :param create: creates the bucket if not yet existing
    :type create: boolean
    :param versioning: whether the new bucket should be versioned
    :type versioning: boolean
    :return: an s3 bucket
    :rtype: `boto3.resources.factory.s3.Bucket`
    """
    s3r = get_s3_resource()
    # try to fetch the specified bucket -- may return an empty list
    bucket = [b for b in s3r.buckets.all() if b.name == name]

    try:
        assert len(bucket) > 0
        return bucket[0]

    # bucket not found
    except AssertionError:
        if create:
            bucket = s3r.create_bucket(Bucket=name)
            print(f"New bucket {name} was created")
        else:
            print(f"Bucket {name} not found")
            return None

    # enable versioning
    if versioning:
        bucket_versioning = s3r.BucketVersioning(name)
        bucket_versioning.enable()

    print(f"Versioning: {bucket_versioning.status}")

    return bucket


def read_jsonlines(key_name: str, bucket_name: str) -> Generator:
    """Given the S3 key of a jsonl.bz2 archive, extract and return its lines.

    Usage example:
    >>> lines = db.from_sequence(read_jsonlines(s3r, key_name , bucket_name))
    >>> lines.map(json.loads).pluck('id').take(10)

    Args:
        key_name (str): S3 key, without S3 prefix, but with partitions within.
        bucket_name (str): Name of S3 bucket to use.

    Yields:
        Generator: generator yielding lines within the archive one by one.
    """
    s3r = get_s3_resource()
    body = s3r.Object(bucket_name, key_name).get()["Body"]
    data = body.read()
    text = bz2.decompress(data).decode("utf-8")

    for line in text.split("\n"):
        if line != "":
            yield line


def readtext_jsonlines(
    key_name: str,
    bucket_name: str,
    fields_to_keep: list[str] | None = None,
) -> Generator:
    """Given the S3 key of a jsonl.bz2 archive, return its lines textual information.

    Only the provided fields (or default ones) will be kept in the returned lines.
    By default, fields_to_keep = ["id", "pp", "ts", "lg", "tp", "t", "ft"].

    This can serve as the starting point for pure textual processing.
    Usage example:
    >>> lines = db.from_sequence(readtext_jsonlines(s3r, key_name , bucket_name))
    >>> lines.map(json.loads).pluck('ft').take(10)

    Args:
        key_name (str): S3 key, without S3 prefix, but with partitions within.
        bucket_name (str): Name of S3 bucket to use.

    Yields:
        Generator: generator yielding reformated lines within the archive one by one.
    """
    if fields_to_keep is None:
        # if no fields were provided
        fields_to_keep = ["id", "pp", "ts", "lg", "tp", "t", "ft"]

    s3r = get_s3_resource()
    body = s3r.Object(bucket_name, key_name).get()["Body"]
    data = body.read()
    text = bz2.decompress(data).decode("utf-8")
    for line in text.split("\n"):
        if line != "":
            article_json = json.loads(line)
            text = article_json["ft"]
            if len(text) != 0:
                article_reduced = {
                    k: article_json[k] for k in article_json if k in fields_to_keep
                }
                yield json.dumps(article_reduced)


def upload_to_s3(local_path: str, path_within_bucket: str, bucket_name: str) -> bool:
    """Upload a file to an S3 bucket.

    Args:
        local_path (str): The local file path to upload.
        path_within_bucket (str): The path within the bucket where the file will be uploaded.
        bucket_name (str): The name of the S3 bucket (without any partitions).

    Returns:
        bool: True if the upload is successful, False otherwise.
    """
    bucket = get_boto3_bucket(bucket_name)
    try:
        # ensure the path within the bucket is only the key
        path_within_bucket = path_within_bucket.replace("s3://", "")
        bucket.upload_file(local_path, path_within_bucket)
        logger.info("Uploaded %s to s3://%s.", path_within_bucket, bucket_name)
        return True
    except Exception as e:
        logger.error(e)
        logger.error("The upload of %s failed with error %s", local_path, e)
        return False


def get_boto3_bucket(bucket_name: str):
    """_summary_

    Args:
        bucket_name (str): _description_

    Returns:
        _type_: _description_
    """
    s3 = get_s3_resource()
    return s3.Bucket(bucket_name)


def fixed_s3fs_glob(path: str, boto3_bucket=None):
    """
    From Benoit, impresso-pyimages package
    A custom glob function as the s3fs one seems to be unable to list more than 1000 elements on the switch S3
    :param path:
    :return:
    """
    if boto3_bucket is None:
        if path.startswith("s3://"):
            path = path[len("s3://") :]
        bucket_name = path.split("/")[0]
        base_path = "/".join(path.split("/")[1:])  # Remove bucket name
        boto3_bucket = get_boto3_bucket(bucket_name)
    else:
        bucket_name = boto3_bucket.name
        base_path = path
    base_path, suffix_path = base_path.split("*")

    filenames = [
        "s3://"
        + os.path.join(
            bucket_name, o.key
        )  # prepend bucket-name as it is necessary for s3fs
        for o in boto3_bucket.objects.filter(Prefix=base_path)
        if o.key.endswith(suffix_path)
    ]

    return filenames


def s3_glob_with_size(path: str, boto3_bucket=None):
    """
    Custom glob function to list S3 objects matching a pattern. This function
    works around the 1000-object listing limit in S3 by using boto3 directly.

    Args:
        path (str): The S3 path with a wildcard (*) to match files.
                    Example: `s3://bucket_name/path/to/files/*.txt`.
        boto3_bucket (boto3.Bucket, optional): An optional boto3 Bucket object.
                                               If not provided, it will be
                                               created from the path.

    Returns:
        list: A list of tuples containing the full S3 paths of matching files
              and their sizes in megabytes.
    """
    if boto3_bucket is None:
        if path.startswith("s3://"):
            path = path[len("s3://") :]
        bucket_name = path.split("/")[0]
        base_path = "/".join(path.split("/")[1:])  # Remove bucket name
        boto3_bucket = get_boto3_bucket(bucket_name)
    else:
        bucket_name = boto3_bucket.name
        base_path = path

    base_path, suffix_path = base_path.split("*")

    filenames = [
        ("s3://" + os.path.join(bucket_name, o.key), round(bytes_to(o.size, "m"), 6))
        for o in boto3_bucket.objects.filter(Prefix=base_path)
        if o.key.endswith(suffix_path)
    ]

    return filenames


def alternative_read_text(
    s3_key: str, s3_credentials: dict, line_by_line: bool = True
) -> Union[list[str], str]:
    """Read from S3 a line-separated text file (e.g. `*.jsonl.bz2`).

    Note:
        The reason for this function is a bug in `dask.bag.read_text()`
        which breaks on buckets having >= 1000 keys.
        It raises a `FileNotFoundError`.
    """
    logger.info(f"reading {s3_key}")
    session = boto3.Session(
        aws_access_key_id=s3_credentials["key"],
        aws_secret_access_key=s3_credentials["secret"],
    )
    s3_endpoint = s3_credentials["client_kwargs"]["endpoint_url"]
    transport_params = {
        "client": session.client("s3", endpoint_url=s3_endpoint),
    }

    if line_by_line:
        with s_open(s3_key, "r", transport_params=transport_params) as infile:
            text = infile.readlines()
    else:
        with s_open(s3_key, "r", transport_params=transport_params) as infile:
            text = infile.read()

    return text


def list_s3_directories(bucket_name, prefix=""):
    """
    Retrieve the 'directory' names (media titles) in an S3 bucket after a
    given path prefix.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix path within the bucket to search. Default
                      is the root ('').

    Returns:
        list: A list of 'directory' names found in the specified bucket
              and prefix.
    """
    logger.info(f"Listing 'folders'' of '{bucket_name}' under prefix '{prefix}'")
    s3 = get_s3_client()
    result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    directories = []
    if "CommonPrefixes" in result:
        directories = [
            prefix["Prefix"][:-1].split("/")[-1] for prefix in result["CommonPrefixes"]
        ]
    logger.info(f"Returning {len(directories)} directories.")
    return directories


def get_s3_object_size(bucket_name, key):
    """
    Get the size of an object (key) in an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        key (str): The key (object) whose size you want to retrieve.

    Returns:
        int: The size of the object in bytes, or None if the object doesn't exist.
    """
    s3_client = get_s3_client()

    try:
        # Get the object metadata to retrieve its size
        response = s3_client.head_object(Bucket=bucket_name, Key=key)
        size = response["ContentLength"]
        return int(size)
    except botocore.exceptions.ClientError as err:
        logger.error(f"Error: {err} for {key} in {bucket_name}")
        return None