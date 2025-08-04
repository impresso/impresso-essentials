"""Reusable functions to read/write data from/to our S3 drive."""

import bz2
import json
import logging
import os
from typing import Callable, Generator

import boto3
from boto3.resources.base import ServiceResource
import botocore
from botocore.client import BaseClient
from dotenv import load_dotenv
from smart_open import open as s_open
import dask.bag as db

from impresso_essentials.utils import (
    bytes_to,
    id_to_issuedir,
    IssueDir,
    get_provider_for_alias,
    PARTNER_TO_MEDIA,
    ALL_MEDIA,
)

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
    except Exception:
        msg = (
            "Variables SE_ACCESS_KEY and SE_ACCESS_KEY were not found in the environment! "
            "Setting default values '' instead. Note the connection to S3 won't be possible currently."
        )
        logger.warning(msg)
        print(msg)
        access_key = ""
        secret_key = ""

    return {
        "client_kwargs": {"endpoint_url": "https://os.zhdk.cloud.switch.ch"},
        "key": access_key,
        "secret": secret_key,
    }


IMPRESSO_STORAGEOPT = get_storage_options()


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
        msg = f"The provided key_name {bucket_name}/{key_name} isn't in this bucket: {e}"
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
    By default, fields_to_keep = ["id", "st", "sm", "pp", "rr", "ts", "lg", "tp", "title", "ft"]

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
        fields_to_keep = ["id", "st", "sm", "pp", "rr", "ts", "lg", "tp", "title", "ft"]

    s3r = get_s3_resource()
    try:
        body = s3r.Object(bucket_name, key_name).get()["Body"]
    except s3r.meta.client.exceptions.NoSuchKey as e:
        msg = f"The provided key_name {bucket_name}/{key_name} isn't in this bucket: {e}"
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
        logger.debug("Uploaded %s to s3://%s.", path_within_bucket, bucket_name)
        return True
    except Exception as e:
        msg = f"The upload of {local_path} failed with error {e}"
        logger.error(e)
        logger.error(msg)
        print(msg)
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


def fixed_s3fs_glob(path: str, suffix: str | None = None, boto3_bucket=None) -> list[str]:
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
        + os.path.join(bucket_name, o.key)  # prepend bucket-name as it is necessary for s3fs
        for o in boto3_bucket.objects.filter(Prefix=base_path)
        if o.key.endswith(suffix_path)
    ]

    return filenames


def s3_glob_with_size(path: str, boto3_bucket=None) -> list[tuple]:
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
    """Retrieve 'directory' names  in an S3 bucket given a path prefix.

    Depending on the prefix and the specific bucket, this will either list the media providers
    or the media aliases.
    For s3 partitions which have the partner layer, the `prefix` parameter allows to select a
    specific partner and list its associated media aliases.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix path within the bucket to search (should include a tailing `/`).
            Defaults to the root ('').

    Returns:
        list: A list of 'directory' names found in the specified bucket and prefix.
    """
    logger.info("Listing 'folders'' of '%s' under prefix '%s'", bucket_name, prefix)
    s3 = get_s3_client()
    result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    directories = []
    if "CommonPrefixes" in result:
        directories = [prefix["Prefix"][:-1].split("/")[-1] for prefix in result["CommonPrefixes"]]
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
        msg = f"Error: {err} for {key} in {bucket_name}"
        logger.error(msg)
        print(msg)
        return None


def s3_iter_bucket(
    bucket_name: str,
    prefix: str = "",
    suffix: str = "",
    accept_key: Callable[[str], bool] = lambda k: True,
) -> list:
    """Iterate over a bucket, returning all keys with some filtering options.

    Note that `prefix` should now include the provider if it's the case in the bucket.

    >>> k = s3_iter_bucket("myBucket", prefix='SNL', suffix=".bz2")
    >>> k = s3_iter_bucket("myBucket", prefix='SNL/GDL', accept_key=lambda x: "page" in x)
    >>> k = s3_iter_bucket("myBucket",  accept_key=lambda x: "/GDL-" in x)

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
    alias: str,
    year: str,
    input_bucket: str,
    provider: str | None = None,
    incl_provider: bool = True,
) -> list[tuple[IssueDir, dict]]:
    """Read the contents of canonical issues from a given S3 bucket.

    By default, it's considered that the bucket includes the provider in its organization.
    If it's not the case, the parameter `incl_provider=False` should be set to ensure it's
    in the constructed s3 path.
    If the provider is not provided, it will be deduced from the alias.
    The provider will however be returned within the IssueDir object anyways.

    Args:
        alias (str): Alias of the media title to read the issues from.
        year (str): Target year to tread issues from.
        input_bucket (str): Bucket from where to fetch the issues.
        provider (str|None, optional): Provider for the given alias. Defaults to None.
        incl_provider (bool, optional): Whether to include the provider in the S3 path.
            Defaults to True.

    Returns:
        list[tuple[IssueDir, dict]]: List of IssueDirs and the issues' contents.
    """
    # construct the various elements of the s3 path for canonical issues
    path_parts = [f"s3://{input_bucket}", alias, "issues", f"{alias}-{year}-issues.jsonl.bz2"]

    if not provider:
        provider = get_provider_for_alias(alias)

    # if the provider should be included, add it at the top of the structure
    if incl_provider:
        path_parts.insert(1, provider)

    issue_path_on_s3 = "/".join(path_parts)

    try:
        issues = (
            db.read_text(issue_path_on_s3, storage_options=IMPRESSO_STORAGEOPT)
            .map(json.loads)
            .map(lambda x: (id_to_issuedir(x["id"], issue_path_on_s3, provider), x))
            .compute()
        )
    except FileNotFoundError as e:
        # logger.error(e)
        print(f"FileNotFoundError: {e}")
        return []

    return issues


def list_media_titles(
    bucket_name: str,
    s3_client=get_s3_client(),
    page_size: int = 10000,
    prov_included: bool = True,
) -> list[str]:
    """List media titles contained in an s3 bucket with impresso data.

    By default, it is considered that the bucket contains the provided level.

    Note:
        25,000 seems to be the maximum `PageSize` value supported by
        SwitchEngines' S3 implementation (ceph).

    Args:
        bucket_name (str): Name of the S3 bucket to consider
        s3_client (optional): S3 client to use. Defaults to get_s3_client().
        page_size (int, optional): Pagination configuration. Defaults to 10000.
        prov_included (bool, optional): Whether the provider level is included in
            the bucket structure. Defaults to True

    Returns:
        list[str]: Sorted list of media titles (aliases) present in the given S3 bucket.
    """
    print(f"Fetching list of media titles from {bucket_name}")

    if "s3://" in bucket_name:
        bucket_name = bucket_name.replace("s3://", "").split("/")[0]

    paginator = s3_client.get_paginator("list_objects")

    aliases = set()
    for n, resp in enumerate(
        paginator.paginate(Bucket=bucket_name, PaginationConfig={"PageSize": page_size})
    ):
        # means the bucket is empty
        if "Contents" not in resp:
            continue

        for f in resp["Contents"]:
            aliases.add(f["Key"].split("/")[1 if prov_included else 0])
        msg = (
            f"Paginated listing of keys in {bucket_name}: page {n + 1}, listed "
            f"{len(resp['Contents'])}"
        )
        logger.info(msg)

    print(f"{bucket_name} contains {len(aliases)} media titles")

    return sorted(list(aliases))


def list_providers_and_aliases(bucket_name: str, prefix: str = "") -> list[str]:
    """Lists providers and their alias directories from an S3 bucket.

    Traverses the given S3 bucket to identify provider directories and their associated alias
    subdirectories, under an optional prefix. Returns a dictionary mapping each provider
    to a sorted list of aliases.

    Args:
        bucket_name (str): The name of the S3 bucket to query.
        prefix (str, optional): The prefix path within the bucket to search (should have a tailing `/`).
            Defaults to the root ('').

    Returns:
        dict[str, list[str]]: Dict mapping sorted provider names to lists of alias directory names.

    Raises:
        botocore.exceptions.ClientError: If there is an issue communicating with S3.

    Example:
        >>> list_providers_and_aliases('141-processed-data-staging', 'embeddings/images/embeddings_dinov2_v1-0-0/')
        {
            'BCUL': ['ACI', 'AV', 'Bombe', 'CL', ..., 'esta', 'ouistiti'],
            ...
            'SNL': ['BDC', 'CDV', ..., 'WHD', 'ZBT']
        }
    """
    msg = f"Listing providers and aliases present in '{bucket_name}' under prefix '{prefix}'."
    logger.info(msg)
    print(msg)
    s3 = get_s3_client()
    result = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")

    prov_to_aliases = {}
    alias_count = 0
    if "CommonPrefixes" in result:

        # extract the provider names from the common prefixes, and sort them
        prov_prefixes = sorted(
            common_prefix["Prefix"][:-1].split("/")[-1]
            for common_prefix in result["CommonPrefixes"]
        )
        # create a dict mapping each provider to its full prefix object
        prov_to_prefixes = dict(zip(prov_prefixes, result["CommonPrefixes"]))

        for prov, prov_prefix in prov_to_prefixes.items():
            if prov not in PARTNER_TO_MEDIA:
                print(f"{prov} is not a provider, skipping it.")
                continue
            aliases_result = s3.list_objects_v2(
                Bucket=bucket_name, Prefix=prov_prefix["Prefix"], Delimiter="/"
            )
            if "CommonPrefixes" in aliases_result:
                # also sort the extracted aliases for this provider
                prov_to_aliases[prov] = sorted(
                    [
                        alias_pref["Prefix"][:-1].split("/")[-1]
                        for alias_pref in aliases_result["CommonPrefixes"]
                    ]
                )
                alias_count += len(prov_to_aliases[prov])

            msg = (
                f"Found provider {prov} with the following "
                f"{len(prov_to_aliases[prov])} alias directories"
            )
            logger.debug(msg)

    msg = f"Returning a total of {alias_count} alias dirs for {len(prov_to_aliases)} providers."
    logger.info(msg)
    print(msg)

    return prov_to_aliases


def list_canonical_files(
    bucket_name: str,
    file_type: str = "issues",
    providers_filter: list[str] | None = None,
    aliases_filter: list[str] | None = None,
) -> tuple[list[str] | None, list[str] | None]:
    """List the canonical files located in a given S3 bucket.

    Note:
        The filters are applied in a hierchical manner; first at provider level,
        then at alias level if no provider filter was given.

    Note:
        For the file type, the possible values are the following:
        - "issues", "pages", "audios": include only bz2 files of the given type.
        - "supports": include all pages and audios bz2 files, returned [1] element of the tuple.
        - "both": include all types of files, with issues ([0]) and supports ([1]).

    Args:
        bucket_name (str): S3 bucket name.
        file_type (str, optional): Type of files to list, possible values are "issues",
            "pages", "audios", "supports" and "both". Defaults to "issues".
        providers_filter (list[str] | None, optional): List of providers for which to consider
            the aliases. If None, `aliases_filter` will be considered. Defaults to None.
        aliases_filter (list[str] | None, optional): List of aliases to consider.
            If None, all will be considered. Defaults to None.

    Raises:
        NotImplementedError: The given `file_type` is not one of ['issues', 'pages', 'audios', 'supports', 'both'].

    Returns:
        tuple[list[str] | None, list[str] | None]: [0] List of issue files or None and
            [1] List of page/audio files or None based on `file_type`
    """
    if file_type not in ["issues", "pages", "audios", "both", "supports"]:
        logger.error(
            "The provided type is not one of ['issues', 'pages', 'audios', 'both', 'supports']!"
        )
        raise NotImplementedError

    # initialize the output lists
    issue_files, page_files, audio_files = None, None, None
    # initialize the output lists
    issue_files, page_files, audio_files = None, None, None
    # list all the providers and aliases in the bucket
    all_present = list_providers_and_aliases(bucket_name)
    if providers_filter:
        # only keep the providers listed
        aliases_list = [
            (prov, a)
            for prov, aliases in all_present.items()
            for a in aliases
            if prov in providers_filter
        ]
        suffix = (
            f"for the media aliases {aliases_list} (from the provided providers {providers_filter})"
        )
    elif aliases_filter:
        # only keep the aliases listed
        aliases_list = [
            (prov, a)
            for prov, aliases in all_present.items()
            for a in aliases
            if a in aliases_filter
        ]
        suffix = f"for the provided media aliases {aliases_list}"
    else:
        aliases_list = [(prov, a) for prov, aliases in all_present.items() for a in aliases]
        suffix = ""

    if file_type in ["issues", "both"]:
        issue_files = [
            file
            for prov, alias in aliases_list
            for file in fixed_s3fs_glob(os.path.join(bucket_name, f"{prov}/{alias}/issues/*"))
        ]
        print(f"{bucket_name} contains {len(issue_files)} .bz2 issue files {suffix}")
    if file_type in ["pages", "supports", "both"]:
        page_files = [
            file
            for prov, alias in aliases_list
            for file in fixed_s3fs_glob(f"{os.path.join(bucket_name, f'{prov}/{alias}/pages/*')}")
        ]
        print(f"{bucket_name} contains {len(page_files)} .bz2 page files {suffix}")
    if file_type in ["audios", "supports", "both"]:
        audio_files = [
            file
            for prov, alias in aliases_list
            for file in fixed_s3fs_glob(f"{os.path.join(bucket_name, f'{prov}/{alias}/audios/*')}")
        ]
        print(f"{bucket_name} contains {len(audio_files)} .bz2 audio files {suffix}")

    support_files = []
    if page_files is not None:
        support_files.extend(page_files)
    if audio_files is not None:
        support_files.extend(audio_files)

    return issue_files, support_files if len(support_files) != 0 else None


def fetch_files(
    bucket_name: str,
    compute: bool = True,
    file_type: str = "issues",
    providers_filter: list[str] | None = None,
    aliases_filter: list[str] | None = None,
) -> tuple[db.core.Bag | None, db.core.Bag | None] | tuple[list[str] | None, list[str] | None]:
    """Fetch issue and/or page canonical JSON files from an s3 bucket.

    If compute=True, the output will be a list of the contents of all files in the
    bucket for the specified newspapers and type of files.
    If compute=False, the output will remain in a distributed dask.bag.

    For the file type, the possible values are the following:
    - 'issues', 'pages', 'audios': include only bz2 files of the given type.
    - 'supports': include all pages and audios bz2 files, returned in element [1] of the tuple.
    - 'both': include all types of files, with issues ([0]) and supports -pages and audios- ([1]).

    Based on file_type, the issue files, page/audio ("support") files or both will be returned.
    In the returned tuple, issues are always in the first element and supports in the
    second, hence if file_type is not 'both', the tuple entry corresponding to the
    undesired type of files will be None.

    Args:
        bucket_name (str): Name of the s3 bucket to fetch the files form
        compute (bool, optional): Whether to compute result and output as list.
            Defaults to True.
        file_type: (str, optional): Type of files to list, possible values are "issues",
            "pages", "audios", "supports" and "both". Defaults to "issues".
        providers_filter (list[str] | None, optional): List of providers for which to consider
            the aliases. If None, `aliases_filter` will be considered. Defaults to None.
        aliases_filter (list[str] | None, optional): List of aliases to consider.
            If None, all will be considered. Defaults to None.

    Raises:
        NotImplementedError: The given `file_type` is not one of ['issues', 'pages', 'audios', 'support', 'both'].

    Returns:
        tuple[db.core.Bag|None, db.core.Bag|None] | tuple[list[str]|None, list[str]|None]:
            [0] Issue files' contents or None and
            [1] Page and Audio Record files' contents or None based on `file_type`

    """
    if file_type not in ["issues", "pages", "audios", "supports", "both"]:
        logger.error(
            "The provided type is not one of ['issues', 'pages', 'audios', 'supports', 'both']!"
        )
        raise NotImplementedError

    issue_files, support_files = list_canonical_files(
        bucket_name, file_type, providers_filter, aliases_filter
    )
    # initialize the outputs
    issue_bag, support_files = None, None

    msg = "Fetching "
    if issue_files is not None:
        msg = f"{msg} issue ids from {len(issue_files)} .bz2 files, "
        issue_bag = db.read_text(issue_files, storage_options=IMPRESSO_STORAGEOPT).map(json.loads)
    if support_files is not None:
        # make sure all files are .bz2 files and exactly have the naming format they should
        prev_len = len(support_files)
        support_files = [s for s in support_files if ".jsonl.bz2" in s and len(s.split("-")) > 5]
        msg = f"{msg} page and audio ids from {len(support_files)} .bz2 files ({prev_len} files before filtering), "
        support_bag = db.read_text(support_files, storage_options=IMPRESSO_STORAGEOPT).map(
            json.loads
        )

    logger.info(msg)

    if compute:
        support_bag = support_bag.compute() if support_files is not None else support_bag
        issue_bag = issue_bag.compute() if issue_files is not None else issue_bag

    return issue_bag, support_bag


def extract_provider_alias_key(
    s3_key: str, bucket: str, prov_included: bool = True
) -> tuple[str, str]:
    """Extract the media alias an s3:key corresponds to given the bucket and partition

    eg. s3_key is in format:
    - s3_key: 's3://31-passim-rebuilt-staging/passim/[provider]/[alias]/[alias]-[year].jsonl.bz2'
    - bucket: '31-passim-rebuilt-staging/passim'
    - prov_included: True
    --> returns (provider, alias)

    Args:
        s3_key (str): Full S3 path of a file (as returned by fixed_s3fs_glob).
        bucket (str): S3 bucket, including partition, in which the media dirs are.
        prov_included (bool, optional): Whether or not the provider level is present in the
            structure of the provided bucket. Defaults to True.

    Returns:
        tuple[str, str]: Media alias of the corresponding media, and corresponding provider.
    """
    # in format: 's3://31-passim-rebuilt-staging/passim/BNL/indeplux/indeplux-1889.jsonl.bz2'
    if not bucket.endswith("/"):
        bucket = f"{bucket}/"

    if "s3://" in bucket:
        key_no_bucket = s3_key.replace(f"{bucket}", "")
    else:
        key_no_bucket = s3_key.replace(f"s3://{bucket}", "")

    # Not all buckets separate the data per title, but the title will always come first.

    sep = "/" if "/" in key_no_bucket else "-"
    alias = key_no_bucket.split(sep)[1 if prov_included else 0]
    provider = key_no_bucket.split(sep)[0] if prov_included else get_provider_for_alias(alias)

    return provider, alias


def provider_in_path(s3_path=None, bucket=None, prefix="") -> bool:
    """Determines whether the given S3 path or prefix includes a provider-level directory structure.

    Args:
        s3_path (str, optional): Full S3 URI (e.g., 's3://my-bucket/prefix/').
            If provided, it overrides `bucket` and `prefix`.
        bucket (str, optional): S3 bucket name. Required if `s3_path` is not given.
        prefix (str, optional): Prefix (folder path) within the bucket.
            Ignored if `s3_path` is provided. Defaults to ''.

    Returns:
        bool:
            - True if all immediate subfolders match known providers.
            - False if all match known media aliases.

    Raises:
        AttributeError:
            - If neither `s3_path` nor `bucket` is provided.
            - If a mix of provider and alias directories is found.
    """
    if not bucket and not s3_path:
        raise AttributeError("At least one of s3_path or bucket must be defined!!")
    if s3_path:
        if s3_path.startswith("s3://"):
            s3_path = s3_path[5:]
        bucket = s3_path.split("/")[0]
        prefix = "/".join(s3_path.split("/")[1:])

    s3 = get_s3_client()
    msg = f"Checking S3 structure in bucket '{bucket}' under prefix '{prefix}'"
    print(msg)
    logger.info(msg)
    result = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")

    if "CommonPrefixes" in result:
        prefixes = [
            common_prefix["Prefix"][:-1].split("/")[-1]
            for common_prefix in result["CommonPrefixes"]
        ]
        print(prefixes)
        if all(p in PARTNER_TO_MEDIA.keys() for p in prefixes):
            return True
        if all(p in ALL_MEDIA for p in prefixes):
            return False
        msg = f"Warning! Bucket {bucket} with prefix {prefix} has a mix of partner and media levels in its strucure!"
        raise AttributeError(msg)
    else:
        msg = f"No subdirectories found under given bucket & prefix {bucket}, {prefix}"
        print(msg)
        logger.warning(msg)
        return False
