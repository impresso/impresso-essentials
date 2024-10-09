"""Reusable functions to read/write data from/to our S3 drive.
"""

import bz2
import json
import logging
import os
from typing import Callable, Generator
from collections import namedtuple

import boto3
from boto3.resources.base import ServiceResource
import botocore
from botocore.client import BaseClient
from dotenv import load_dotenv
from smart_open import open as s_open
import dask.bag as db

from impresso_essentials.utils import bytes_to, id_to_issuedir

logger = logging.getLogger(__name__)


def get_storage_options() -> dict[str, dict | str]:
    """Load environment variables from local .env files

    Assumes that two environment variables are set:
    `SE_ACCESS_KEY` and `SE_SECRET_KEY`.

    Returns:
        dict[str, dict | str]: Credentials to access a S3 endpoint.
    """
    load_dotenv()
    try:
        access_key = os.environ["SE_ACCESS_KEY"]
        secret_key = os.environ["SE_SECRET_KEY"]
    except Exception as e:
        msg = (
            "Variables SE_ACCESS_KEY and SE_ACCESS_KEY were not found in the environment! "
            "Setting default values '' instead. Note the connection to S3 won't be possible currently."
        )
        logger.warining(msg)
        print(msg)
        access_key = ''
        secret_key = ''

    return {
        "client_kwargs": {"endpoint_url": "https://os.zhdk.cloud.switch.ch"},
        "key": access_key,
        "secret": secret_key,
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


def get_or_create_bucket(name: str, create: bool = False):
    """Create a boto3 s3 connection and create or return the requested bucket.

    It is possible to ask for creating a new bucket
    with the specified name (in case it does not exist):
    >>> b = get_bucket('testb', create=False)
    >>> b = get_bucket('testb', create=True)

    Args:
        name (str): Name of thebucket to get of create.
        create (bool, optional): Whether to create the bucket if it doesn't exist.
            Defaults to False.

    Returns:
        boto3.resources.factory.s3.Bucket: S3 bucket, fetched or created.
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

    return bucket


def read_jsonlines(key_name: str, bucket_name: str) -> Generator:
    """Given the S3 key of a jsonl.bz2 archive, extract and return its lines.

    Usage example:
    >>> lines = db.from_sequence(read_jsonlines(s3r, key_name , bucket_name))
    >>> lines.map(json.loads).pluck('id').take(10)

    Args:
        key_name (str): S3 key, without S3 prefix, but with partitions within.
        bucket_name (str): Name of S3 bucket to use.

    Raises:
        ValueError: The provided key_name does not exist in the provided bucket.

    Yields:
         Generator: generator yielding lines within the archive one by one.
    """
    s3r = get_s3_resource()
    try:
        body = s3r.Object(bucket_name, key_name).get()["Body"]

    except s3r.meta.client.exceptions.NoSuchKey as e:
        msg = (
            f"The provided key_name {bucket_name}/{key_name} isn't in this bucket: {e}"
        )
        raise ValueError(msg) from e

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

    Raises:
        ValueError: The provided key_name does not exist in the provided bucket.

    Yields:
        Generator: generator yielding reformated lines within the archive one by one.
    """
    if fields_to_keep is None:
        # if no fields were provided
        fields_to_keep = ["id", "pp", "ts", "lg", "tp", "t", "ft"]

    s3r = get_s3_resource()
    try:
        body = s3r.Object(bucket_name, key_name).get()["Body"]
    except s3r.meta.client.exceptions.NoSuchKey as e:
        msg = (
            f"The provided key_name {bucket_name}/{key_name} isn't in this bucket: {e}"
        )
        raise ValueError(msg) from e
    data = body.read()
    text = bz2.decompress(data).decode("utf-8")
    for line in text.split("\n"):
        if line != "":
            article_json = json.loads(line)
            if article_json["tp"] == "ar":
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
    bucket = get_bucket(bucket_name)
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


def get_bucket(bucket_name: str):
    """Create a boto3 connection and return the desired bucket.

    Note:
        This function does not ensure that the bucket exists. If this verification
        is necessary, please prefer using `get_or_create_bucket()` instead.

    Args:
        bucket_name (str): Name of the S3 bucket to use.

    Returns:
        boto3.resources.factory.s3.Bucket: Desired S3 bucket.
    """
    s3 = get_s3_resource()
    return s3.Bucket(bucket_name)


def fixed_s3fs_glob(
    path: str, suffix: str | None = None, boto3_bucket=None
) -> list[str]:
    """Custom glob function able to list more than 1000 elements on s3 (fix of s3fs).

    Note:
        `path` should be of the form "[partition]*[suffix or file extensions]", with
        the partition potentially including the bucket name.
        If all files within the partitions should be considered, regardeless of their
        extension, "*" can be omitted.
        Conversely, `path` can be of the form "[partition]" if `suffix` is defined.

    Args:
        path (str): Glob path to the files, optionally including the bucket name.
            If the bucket name is not included, `boto3_bucket` should be defined.
        suffix (str | None, optional): Suffix or extension of the paths to consider
            within the bucket. Only used if "*" not found in `path`. Defaults to None.
        boto3_bucket (boto3.resources.factory.s3.Bucket, optional): S3 bucket to look
            into. Defaults to None.

    Returns:
        list[str]: List of filenames within the bucket corresponding to the provided path.
    """
    if boto3_bucket is None:
        if path.startswith("s3://"):
            path = path[len("s3://") :]
        bucket_name = path.split("/")[0]
        base_path = "/".join(path.split("/")[1:])  # Remove bucket name
        boto3_bucket = get_bucket(bucket_name)
    else:
        bucket_name = boto3_bucket.name
        base_path = path

    if "*" in base_path:
        base_path, suffix_path = base_path.split("*")
    else:
        suffix_path = suffix

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
        boto3_bucket = get_bucket(bucket_name)
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
) -> list[str] | str:
    """Read from S3 a line-separated text file (e.g. `*.jsonl.bz2`).

     Note:
        The reason for this function is a bug in `dask.bag.read_text()`
        which breaks on buckets having >= 1000 keys.
        It raises a `FileNotFoundError`.

    Args:
        s3_key (str): Full S3 path to the file to read.
        s3_credentials (dict): S3 credentials, `IMPRESSO_STORAGEOPT`.
        line_by_line (bool, optional): Whether to read the file line by line.
            Defaults to True.

    Returns:
        list[str] | str: Contents of the file, as a list of strings or as one string.
    """
    logger.info("reading the text of %s", s3_key)
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


def list_s3_directories(bucket_name: str, prefix: str = "") -> list[str]:
    """Retrieve 'directory' names (media titles) in an S3 bucket given a path prefix.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix path within the bucket to search. Default
                      is the root ('').

    Returns:
        list: A list of 'directory' names found in the specified bucket
              and prefix.
    """
    logger.info("Listing 'folders'' of '%s' under prefix '%s'", bucket_name, prefix)
    s3 = get_s3_client()
    result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    directories = []
    if "CommonPrefixes" in result:
        directories = [
            prefix["Prefix"][:-1].split("/")[-1] for prefix in result["CommonPrefixes"]
        ]
    logger.info("Returning %s directories.", len(directories))
    return directories


def get_s3_object_size(bucket_name: str, key: str) -> int:
    """Get the size of an object (key) in an S3 bucket.

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
        logger.error("Error: %s for %s in %s", err, key, bucket_name)
        return None


def s3_iter_bucket(
    bucket_name: str,
    prefix: str = "",
    suffix: str = "",
    accept_key: Callable[[str], bool] = lambda k: True,
) -> list:
    """Iterate over a bucket, returning all keys with some filtering options.

    >>> k = s3_iter_bucket("myBucket", prefix='GDL', suffix=".bz2")
    >>> k = s3_iter_bucket("myBucket", prefix='GDL', accept_key=lambda x: "page" in x)

    Note:
        If `suffix` is not "", the used accepting condition will become:
        `lambda key: accept_key(key) and key.endswith(suffix)`

    Args:
        bucket_name (str): Name of the S3 bucket to list the contents of
        prefix (str, optional): Partition prefix to filter bucket's keys. Defaults to "".
        suffix (str, optional): Suffix to filter the bucket's keys. Defaults to "".
        accept_key (Callable[[str], bool], optional): Filtering condition for the keys
            as a lambda function. Defaults to lambda k: True.

    Returns:
        list: List of keys corresponding ot the provided prefix, suffix and accept key.
    """
    if suffix != "":
        filter_condition = lambda k: accept_key(k) and k.endswith(suffix)
    else:
        filter_condition = accept_key

    client = get_s3_client()
    paginator = client.get_paginator("list_objects")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    keys = []
    for page in page_iterator:
        if "Contents" in page:
            for key in page["Contents"]:
                key_string = key["Key"]
                if filter_condition(key_string):
                    keys.append(key_string)
    return keys if keys else []


def read_s3_issues(
    newspaper: str, year: str, input_bucket: str
) -> list[tuple[IssueDir, dict]]:
    """Read the contents of canonical issues from a given S3 bucket.

    Args:
        newspaper (str): Name of the newspaper to read the issues from.
        year (str): Target year to tread issues from.
        input_bucket (str): Bucket from where to fetch the issues.

    Returns:
        list[tuple[IssueDir, dict]]: List of IssueDirs and the issues' contents.
    """
    issue_path_on_s3 = (
        f"s3://{input_bucket}/{newspaper}/issues/{newspaper}-{year}-issues.jsonl.bz2"
    )
    try:
        issues = (
            db.read_text(issue_path_on_s3, storage_options=IMPRESSO_STORAGEOPT)
            .map(json.loads)
            .map(lambda x: (id_to_issuedir(x["id"], issue_path_on_s3), x))
            .compute()
        )
    except FileNotFoundError as e:
        logger.error(e)
        return []

    return issues


def list_newspapers(
    bucket_name: str,
    s3_client=get_s3_client(),
    page_size: int = 10000,
) -> list[str]:
    """List newspapers contained in an s3 bucket with impresso data.

    Note:
        25,000 seems to be the maximum `PageSize` value supported by
        SwitchEngines' S3 implementation (ceph).
    Note:
        Copied from https://github.com/impresso/impresso-data-sanitycheck/tree/master/sanity_check/contents/s3_data.py

    Args:
        bucket_name (str): Name of the S3 bucket to consider
        s3_client (optional): S3 client to use. Defaults to get_s3_client().
        page_size (int, optional): Pagination configuration. Defaults to 10000.

    Returns:
        list[str]: List of newspaper (aliases) present in the given S3 bucket.
    """
    print(f"Fetching list of newspapers from {bucket_name}")

    if "s3://" in bucket_name:
        bucket_name = bucket_name.replace("s3://", "").split("/")[0]

    paginator = s3_client.get_paginator("list_objects")

    newspapers = set()
    for n, resp in enumerate(
        paginator.paginate(Bucket=bucket_name, PaginationConfig={"PageSize": page_size})
    ):
        # means the bucket is empty
        if "Contents" not in resp:
            continue

        for f in resp["Contents"]:
            newspapers.add(f["Key"].split("/")[0])
        msg = (
            f"Paginated listing of keys in {bucket_name}: page {n + 1}, listed "
            f"{len(resp['Contents'])}"
        )
        logger.info(msg)

    print(f"{bucket_name} contains {len(newspapers)} newspapers")

    return newspapers


def list_files(
    bucket_name: str,
    file_type: str = "issues",
    newspapers_filter: list[str] | None = None,
) -> tuple[list[str] | None, list[str] | None]:
    """List the canonical files located in a given S3 bucket.

    Args:
        bucket_name (str): S3 bucket name.
        file_type (str, optional): Type of files to list, possible values are "issues",
            "pages" and "both". Defaults to "issues".
        newspapers_filter (list[str] | None, optional): List of newspapers to consider.
            If None, all will be considered. Defaults to None.

    Raises:
        NotImplementedError: The given `file_type` is not one of ['issues', 'pages', 'both'].

    Returns:
        tuple[list[str] | None, list[str] | None]: [0] List of issue files or None and
            [1] List of page files or None based on `file_type`
    """
    if file_type not in ["issues", "pages", "both"]:
        logger.error("The provided type is not one of ['issues', 'pages', 'both']!")
        raise NotImplementedError

    # initialize the output lists
    issue_files, page_files = None, None
    # list the newspapers in the bucket
    newspapers = list_newspapers(bucket_name)

    if newspapers_filter is not None:
        suffix = f"for the provided newspapers {newspapers_filter}"
    else:
        suffix = ""

    if file_type in ["issues", "both"]:
        issue_files = [
            file
            for np in newspapers
            if newspapers_filter is not None and np in newspapers_filter
            for file in fixed_s3fs_glob(os.path.join(bucket_name, f"{np}/issues/*"))
        ]
        print(f"{bucket_name} contains {len(issue_files)} .bz2 issue files {suffix}")
    if file_type in ["pages", "both"]:
        page_files = [
            file
            for np in newspapers
            if newspapers_filter is not None and np in newspapers_filter
            for file in fixed_s3fs_glob(f"{os.path.join(bucket_name, f'{np}/pages/*')}")
        ]
        print(f"{bucket_name} contains {len(page_files)} .bz2 page files {suffix}")

    return issue_files, page_files


def fetch_files(
    bucket_name: str,
    compute: bool = True,
    file_type: str = "issues",
    newspapers_filter: list[str] | None = None,
) -> (
    tuple[db.core.Bag | None, db.core.Bag | None]
    | tuple[list[str] | None, list[str] | None]
):
    """Fetch issue and/or page canonical JSON files from an s3 bucket.

    If compute=True, the output will be a list of the contents of all files in the
    bucket for the specified newspapers and type of files.
    If compute=False, the output will remain in a distributed dask.bag.

    Based on file_type, the issue files, page files or both will be returned.
    In the returned tuple, issues are always in the first element and pages in the
    second, hence if file_type is not 'both', the tuple entry corresponding to the
    undesired type of files will be None.

    Args:
        bucket_name (str): Name of the s3 bucket to fetch the files form
        compute (bool, optional): Whether to compute result and output as list.
            Defaults to True.
        file_type: (str, optional): Type of files to list, possible values are "issues",
            "pages" and "both". Defaults to "issues".
        newspapers_filter: (list[str]|None,optional): List of newspapers to consider.
            If None, all will be considered. Defaults to None.

    Raises:
        NotImplementedError: The given `file_type` is not one of ['issues', 'pages', 'both'].

    Returns:
        tuple[db.core.Bag|None, db.core.Bag|None] | tuple[list[str]|None, list[str]|None]:
            [0] Issue files' contents or None and
            [1] Page files' contents or None based on `file_type`
    """
    if file_type not in ["issues", "pages", "both"]:
        logger.error("The provided type is not one of ['issues', 'pages', 'both']!")
        raise NotImplementedError

    issue_files, page_files = list_files(bucket_name, file_type, newspapers_filter)
    # initialize the outputs
    issue_bag, page_bag = None, None

    msg = "Fetching "
    if issue_files is not None:
        msg = f"{msg} issue ids from {len(issue_files)} .bz2 files, "
        issue_bag = db.read_text(issue_files, storage_options=IMPRESSO_STORAGEOPT).map(
            json.loads
        )
    if page_files is not None:
        # make sure all files are .bz2 files and exactly have the naming format they should
        prev_len = len(page_files)
        page_files = [
            p for p in page_files if ".jsonl.bz2" in p and len(p.split("-")) > 5
        ]
        msg = f"{msg} page ids from {len(page_files)} .bz2 files ({prev_len} files before filtering), "
        page_bag = db.read_text(page_files, storage_options=IMPRESSO_STORAGEOPT).map(
            json.loads
        )

    logger.info(msg)

    if compute:
        page_bag = page_bag.compute() if page_files is not None else page_bag
        issue_bag = issue_bag.compute() if issue_files is not None else issue_bag

    return issue_bag, page_bag
