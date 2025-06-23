"""Helper functions to read, generate and write data versioning manifests."""

import copy
import json
import logging
import os
import re
from time import strptime
from typing import Any, Union, Optional
from tqdm import tqdm

from impresso_essentials.utils import (
    bytes_to,
    PARTNER_TO_MEDIA,
    get_provider_for_alias,
    get_src_info_for_alias,
    DataStage,
    validate_stage
)
from impresso_essentials.io.s3 import (
    fixed_s3fs_glob,
    alternative_read_text,
    get_storage_options,
    get_bucket,
    get_s3_object_size,
)

logger = logging.getLogger(__name__)


IMPRESSO_STORAGEOPT = get_storage_options()
POSSIBLE_GRANULARITIES = ["corpus", "title", "year"]
VERSION_INCREMENTS = ["major", "minor", "patch"]

###############################
###### VERSION FUNCTIONS ######


def validate_version(v: str, regex: str = "^v([0-9]+[.]){2}[0-9]+$") -> Optional[str]:
    """Validate the provided string version against a regex.

    The provided version should be in format "vM.m.p", where M, m and p are
    integers representing respectively the Major, minor and patch version.

    Args:
        v (str): version in string format to validate.
        regex (str, optional): Regex against which to match the version.
            Defaults to "^v([0-9]+[.]){2}[0-9]+$".

    Returns:
        Optional[str]: The provided version if it's valid, None otherwise.
    """
    # accept versions with hyphens in case of mistake
    v = v.replace("-", ".")

    if re.match(regex, v) is not None:
        return v

    msg = f"Non conforming version {v} provided: ({regex}), version will be inferred."
    logger.critical(msg)
    return None


def version_as_list(version: str) -> list[int]:
    """Return the provided string version as a list of three ints.

    Args:
        version (str): String version to return as list

    Returns:
        list[int]: list of len 3 where indices respecively correspond to the
            Major, minor and patch versions.
    """
    if version[0] == "v":
        version = validate_version(version)
        start = 1
    else:
        start = 0
    sep = "." if "." in version else "-"
    return version[start:].split(sep)


def extract_version(name_or_path: str, as_int: bool = False) -> Union[str, int]:
    """Extract the version from a string filename or path.

    This function is in particular mean to extract the version from paths or filenames
    of manifests: structured as [data-stage]_vM-m-p.json.

    Args:
        name_or_path (str): Filename or path from which to extract the version.
        as_int (bool, optional): Whether to return the extracted version as int or str.
            Defaults to False.

    Returns:
        Union[str, int]: Extracted version, as int or str based on `as_int`.
    """
    # in the case it's a path
    basename = os.path.basename(name_or_path)
    version = basename.replace(".json", "").split("_")[-1]

    if as_int:
        ind_nums = version[1:].split("-")
        # multiply each part of the version with a larger multiple of 10
        as_ints = [int(n) * (10 ** (2 * i)) for i, n in enumerate(ind_nums[::-1])][::-1]
        return sum(as_ints)
    return version.replace("-", ".")


def increment_version(prev_version: str, increment: str) -> str:
    """Update  given version accoding to the given increment.

    When the increment is major or minor, all following numbers are reset to 0.

    Args:
        prev_version (str): Version to increment
        increment (str): Increment, can be one of major, minor and patch.

    Raises:
        e: Increment value provided is not valid.

    Returns:
        str: Vesion incremented accordingly.
    """
    try:
        incr_val = VERSION_INCREMENTS.index(increment)
        list_v = version_as_list(prev_version)
        # increase the value of the correct "sub-version" and reset the ones right of it
        list_v[incr_val] = str(int(list_v[incr_val]) + 1)
        if incr_val < 2:
            list_v[incr_val + 1 :] = ["0"] * (2 - incr_val)

        return "v" + ".".join(list_v)
    except ValueError as e:
        logger.error("Provided invalid increment %s: not in %s", increment, VERSION_INCREMENTS)
        raise e


#####################################
###### S3 READ/WRITE FUNCTIONS ######


def find_s3_data_manifest_path(
    bucket_name: str, data_stage: str, partition: Optional[str] = None
) -> Optional[str]:
    """Find and return the latest data manifest in a given S3 bucket.

    On S3, different Data stages will be stored in different ways.
    In particular, data stages corresponding to enrichments are all placed in the
    same bucket but in different partitions.
    Data stages "canonical", "rebuilt", "evenized-rebuilt" & ones related to Solr
    are the ones where each stage has its own bucket.

    Args:
        bucket_name (str): Name of the bucket in which to look.
        data_stage (str): Data stage corresponding to the manifest to fetch.
        partition (Optional[str], optional): Partition within the bucket to look
            into. Defaults to None.

    Returns:
        Optional[str]: S3 path of the latest manifest in the bucket, None if no
            manifests were found inside.
    """
    # fetch the data stage as the naming value
    if isinstance(data_stage, DataStage):
        stage_value = data_stage.value
    else:
        stage_value = validate_stage(data_stage, return_value_str=True)

    # manifests have a json extension and are named after the format (value)
    path_filter = f"{stage_value}_v*.json"

    if partition is None and stage_value in [
        DataStage.CANONICAL.value,  # "canonical"
        DataStage.REBUILT.value,  # "rebuilt"
        DataStage.PASSIM.value,  # "passim"
        DataStage.SOLR_TEXT.value,  # "solr-ingestion-text"
    ]:
        # manifest in top-level partition of bucket
        bucket = get_bucket(bucket_name)
        matches = fixed_s3fs_glob(path_filter, boto3_bucket=bucket)
    else:
        assert partition is not None, "partition should be provided for processed data"
        # processed data are all in the same bucket,
        # manifest should be directly fetched from path
        full_s3_path = os.path.join(bucket_name, partition, path_filter)
        # print(full_s3_path)
        matches = fixed_s3fs_glob(full_s3_path)

    # matches will always be a list
    if len(matches) == 1:
        return matches[0]
    if len(matches) == 0:
        # no matches means it's the first manifest for the stage or bucket
        return None

    # if multiple versions exist, return the latest one
    return sorted(matches, key=lambda x: extract_version(x, as_int=True))[-1]


def read_manifest_from_s3(
    bucket_name: str,
    data_stage: Union[DataStage, str],
    partition: Optional[str] = None,
) -> Optional[tuple[str, dict[str, Any]]]:
    """Read and load manifest given an S3 bucket.

    Args:
        bucket_name (str): NAme of the s3 bucket to look into
        data_stage (Union[DataStage, str]): Data stage corresponding to the
            manifest to fetch.
        partition (Optional[str], optional): Partition within the bucket to look
            into. Defaults to None.

    Returns:
        tuple[str, dict[str, Any]] | tuple[None, None]: S3 path of the manifest
            and corresponding contents, if a manifest was found, None otherwise.
    """
    # reset the partition to None if it's empty
    partition = None if partition == "" else partition
    manifest_s3_path = find_s3_data_manifest_path(bucket_name, data_stage, partition)
    if manifest_s3_path is None:
        logger.info("No %s manifest found in bucket %s", data_stage, bucket_name)
        return None, None

    raw_text = alternative_read_text(manifest_s3_path, IMPRESSO_STORAGEOPT, line_by_line=False)

    return manifest_s3_path, json.loads(raw_text)


def read_manifest_from_s3_path(manifest_s3_path: str) -> Optional[dict[str, Any]]:
    """read and extract the contents of an arbitrary manifest,

    Args:
        manifest_s3_path (str): S3 path of the manifest to read.

    Returns:
        Optional[dict[str, Any]]: Contents of manifest if found on S3, None otherwise.
    """
    try:
        raw_text = alternative_read_text(
            manifest_s3_path, IMPRESSO_STORAGEOPT, line_by_line=False
        )
        return json.loads(raw_text)
    except FileNotFoundError as e:
        logger.error("No manifest found at s3 path %s. %s", manifest_s3_path, e)
        return None


##########################################
###### MEDIA LIST & STATS FUNCTIONS ######


def media_list_from_mft_json(json_mft: dict[str, Any]) -> dict[str, dict]:
    """Extract the `media_list` from a manifest as a dict where each title is a key.

    For each title, all fields from the original media list will still be present
    along with an additional `stats_as_dict` field containing a dict mapping each
    year to its specific statistics.

    As a result:
        - All represented titles are within the keys of the returned media list.
        - For each title, represented years are in the keys of its `stats_as_dict` field.

    Args:
        json_mft (dict[str, Any]): Dict following the JSON schema of a manifest from
            which to extract the media list.

    Returns:
        dict[str, dict]: Media list of given manifest, with `stats_as_dict` field.
    """
    # prevent modification or original manifest, to recover later
    manifest = copy.deepcopy(json_mft)
    new_media_list = {}
    for media in manifest["media_list"]:
        if media["media_title"] not in ["0002088", "0002244"]:
            yearly_media_stats = {
                year_stats["element"].split("-")[1]: year_stats
                for year_stats in media["media_statistics"]
                if year_stats["granularity"] == "year"
            }

            new_media_list[media["media_title"]] = media
            new_media_list[media["media_title"]]["stats_as_dict"] = yearly_media_stats
        else:
            logger.info("Skipping %s as it's BL and only a sample.", media["media_title"])

    return new_media_list


def init_media_info(
    add: bool = True,
    full_title: bool = True,
    years: Optional[list[str]] = None,
    fields: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Initialize the media update dict for a title given relevant information.

    All the update informations are relating to the newly processed data, in
    comparison with the one computed during the last processing.

    Args:
        add (bool, optional): Whether new data was added. Defaults to True.
        full_title (bool, optional): Whether all the title's years were modified.
            Defaults to True.
        years (Optional[list[str]], optional): When `full_title`, the specific years
            which were modified/updated. Defaults to None.
        fields (Optional[list[str]], optional): List of specific fields that were
            modified/updated. Defaults to None.

    Returns:
        dict[str, Any]: Instantiated dict with the update information for a given media.
    """
    return {
        "update_type": "addition" if add else "modification",
        "update_level": "title" if full_title else "year",
        "updated_years": sorted(years) if years is not None else [],
        "updated_fields": fields if fields is not None else [],
    }


def add_media_source_metadata(
    title: str, old_media_title_info: dict[str, dict], provider: str | None = None
) -> dict[str, Any]:
    """Add the source metadata to an exsiting media title info dict.

    Args:
        title (str): Impresso alias of the media title.
        old_media_title_info (dict[str, dict]): Existing info to update.

    Returns:
        dict[str, Any]: Media title info dict with additional source metadata.
    """
    assert (
        title == old_media_title_info["media_title"]
    ), f"Title mismatch: {title} != {old_media_title_info['media_title']}"

    # this new dict allows to manufacture the key ordering in the final manifest
    new_media_title_info = {"media_title": title}

    # don't fetch the provider if already present
    if not provider or provider not in PARTNER_TO_MEDIA:
        provider = get_provider_for_alias(title)
    if "data_provider" not in old_media_title_info:
        new_media_title_info["data_provider"] = provider
    if "source_medium" not in old_media_title_info:
        new_media_title_info["source_medium"] = get_src_info_for_alias(title, provider)
    if "source_type" not in old_media_title_info:
        new_media_title_info["source_type"] = get_src_info_for_alias(title, provider, False)

    new_media_title_info.update(old_media_title_info)

    return new_media_title_info


def sort_media_list_years_and_titles(media_list: dict[str, dict]) -> dict[str, dict]:
    """Sort the media titles and corresponding years by alphabetical order.

    The media_list is in a format such that:
    - media_list[title]["stats_as_dict"][year] = stats
    where title and year are strings
    Since it's a dict, we can't directly sort it.
    However we can populate a new dict with the exact same format where the keys are sorted.
    This ensures we keep always the same ordering in manifests.

    Args:
        media_list (dict[str, dict]): Media list to sort.

    Returns:
        dict[str, dict]: Same dict, where the title and subsequent year keys are sorted.
    """
    sorted_media_list = {}
    for title in sorted(list(media_list.keys())):
        # redefine a new stats_as_dict dict with sorted years and set it
        new_stats_as_dict = {
            str(y): media_list[title]["stats_as_dict"][str(y)]
            for y in sorted([int(y) for y in media_list[title]["stats_as_dict"].keys()])
        }
        media_list[title]["stats_as_dict"] = new_stats_as_dict
        # set this new media in the sorted media list, now sorted by title
        sorted_media_list[title] = media_list[title]

    return sorted_media_list


################ MANIFEST DIFFS #################


def manifest_summary(mnf_json: dict[str, Any], extended_summary: bool = False) -> None:
    """
    Generate a summary of the manifest data.

    Args:
        mnf_json (dict): A dictionary containing manifest data.
        extended_summary (bool, optional): Whether to include extended summary
        with year statistics. Defaults to False.

    Returns:
        None

    Prints: Summary of the manifest including the number of media items, additions,
    and modifications.

    Example:
        >>> manifest_summary(manifest_json)
        Summary of manifest /path/to/manifest.json:
        Number of media items: 10 (8 from set)
        Number of addition at title level: 5
        Number of addition at year level: 3
        Number of modification at title level: 2
        Number of modification at year level: 1
    """
    nb_media_items = len(mnf_json["media_list"])
    nb_addition_title_level = 0
    nb_addition_year_level = 0
    nb_modification_title_level = 0
    nb_modification_year_level = 0

    media_item_set = set()
    title_nbyear = {}

    for media_item in mnf_json["media_list"]:
        media_item_set.add(media_item["media_title"])
        if media_item["update_type"] == "addition":
            if media_item["update_level"] == "title":
                nb_addition_title_level += 1
            elif media_item["update_level"] == "year":
                nb_addition_year_level += 1
        elif media_item["update_type"] == "modification":
            if media_item["update_level"] == "title":
                nb_modification_title_level += 1
            elif media_item["update_level"] == "year":
                nb_modification_year_level += 1
        if extended_summary:
            title_nbyear[media_item["media_title"]] = len(
                [
                    year_stats
                    for year_stats in media_item["media_statistics"]
                    if year_stats["granularity"] == "year"
                ]
            )

    # print summary
    print(
        f"\n*** Summary of manifest: [{mnf_json['mft_s3_path']}] (regardless of any "
        f"modification date):"
    )
    print(f"- Number of media items: {nb_media_items} ({len(media_item_set)} from set)")
    print(f"- Number of addition at title level: {nb_addition_title_level}")
    print(f"- Number of addition at year level: {nb_addition_year_level}")
    print(f"- Number of modification at title level: {nb_modification_title_level}")
    print(f"- Number of modification at year level: {nb_modification_year_level}")
    print(f"- List of media titles:\n{get_media_titles(mnf_json['media_list'])}\n")

    if extended_summary:
        print(
            f"\nExtended summary - Number of years per title "
            f"(regardless of modification/addition or not):"
            f"{nb_modification_year_level}"
        )
        for key, val in title_nbyear.items():
            print(f"- {key:<18}: {val:>5}y")
            print("\n")


def filter_new_or_modified_media(
    rebuilt_mft_json: dict[str, Any], previous_mft_json: dict[str, Any]
) -> dict[str, Any]:
    """
    Compares two manifests to determine new or modified media items.

    Typical use-case is during an atomic update, when only media items added or modified
    compared to the previous process need to be ingested or processed.

    Args:
        rebuilt_mft_json (dict[str, Any]): json of the rebuilt manifest (new).
        previous_mft_json (dict[str, Any]): json of the previous process manifest.

    Returns:
        list[dict[str, Any]]: A manifest identical to 'rebuilt_mft_path' but only with
        media items that are new or modified in the media list.

    Example: >>> new_or_modified = get_new_or_modified_media("new_manifest.json",
    "previous_manifest.json") >>> print(new_or_modified) [{'media_title':
    'new_media_item_1', 'last_modif_date': '2024-04-04T12:00:00Z', etc.},
    {'media_title': 'modified_media_item_2', 'last_modif_date':
    '2024-04-03T12:00:00Z', etc.}]
    """
    filtered_manifest = copy.deepcopy(rebuilt_mft_json)

    # Extract last modification date of each media item of the previous process
    previous_media_items = {
        media["media_title"]: strptime(media["last_modification_date"], "%Y-%m-%d %H:%M:%S")
        for media in previous_mft_json["media_list"]
    }

    # Print rebuilt manifest summary
    manifest_summary(rebuilt_mft_json, extended_summary=False)

    # Filter: keep only media items newly added or modified after last process
    filtered_media_list = []
    for rebuilt_media_item in rebuilt_mft_json["media_list"]:
        if rebuilt_media_item["media_title"] not in previous_media_items:
            filtered_media_list.append(rebuilt_media_item)
        elif (
            strptime(rebuilt_media_item["last_modification_date"], "%Y-%m-%d %H:%M:%S")
            > previous_media_items[rebuilt_media_item["media_title"]]
        ):
            filtered_media_list.append(rebuilt_media_item)

    logger.info(
        "\n*** Getting new or modified items:"
        "\nInput (rebuilt) manifest has %s media items.",
        len(get_media_titles(rebuilt_mft_json)),
    )
    logger.info("Resulting filtered manifest has %s media items.", len(filtered_media_list))
    logger.info(
        "Media items that will be newly processed:\n %s\n",
        get_media_titles(filtered_media_list),
    )

    filtered_manifest["media_list"] = filtered_media_list

    return filtered_manifest


def get_media_titles(input_data: Union[dict[str, Any], list[dict[str, Any]]]) -> list[str]:
    """
    Extracts media titles from the input data which can be either a manifest
    or a media list.

    Args:
        input_data (Union[dict[str, Any], list[dict[str, Any]]]): A manifest dictionary
            or the media list of a manifest.

    Returns:
        list[str]: A list of media titles extracted from the input data.
        Ex:  ['Title 1', 'Title 2']
    Raises:
        TypeError: If the input data is not in the expected format.
        KeyError: If the 'media_title' key is not found in the input data.
    """
    if isinstance(input_data, list):
        titles = [media_item["media_title"] for media_item in input_data]
    else:
        titles = [media_item["media_title"] for media_item in input_data["media_list"]]
    return titles


def get_media_item_years(mnf_json: dict[str, Any]) -> dict[str, dict[str, float]]:
    """
    Retrieves the s3 key and size in MB of each year of media items from a manifest.

    Args:
        mnf_json (dict): A manifest dictionary.

    Returns:
       media_items_years (dict): A dictionary where media titles are keys,
            and each value is a dictionary with s3 key as key and its size as value.
    """

    bucket_name = mnf_json["mft_s3_path"].rsplit("/", 1)[0]
    media_items_years = {}

    logger.info("*** Retrieving size info for each key")
    for media_item in tqdm(mnf_json["media_list"]):
        title = media_item["media_title"]
        years = {}

        for media_year in media_item["media_statistics"]:
            if media_year["granularity"] != "year":
                continue

            year_key = title + "/" + media_year["element"] + ".jsonl.bz2"
            s3_key = bucket_name + "/" + year_key
            year_size_b = get_s3_object_size(bucket_name.split("//")[1], year_key)
            year_size_m = (
                round(bytes_to(year_size_b, "m"), 2) if year_size_b is not None else None
            )
            # print(f"Size in b: {year_size_b}, in mb: {year_size_m}")

            years[s3_key] = year_size_m

        media_items_years[title] = years

    logger.info("*** About the collection if s3 keys for each year:")
    for t, y in media_items_years.items():
        no_s3_keys = [s3k for s3k, size in y.items() if size is None]
        valid_s3_keys = [s3k for s3k, size in y.items() if size is not None]
        logger.info(
            "\t[%s] has %s existing s3 keys and %s missing keys.",
            t,
            len(valid_s3_keys),
            len(no_s3_keys),
        )

    return media_items_years


def remove_media_in_manifest(mnf_json: dict[str, Any], white_list: list[str]) -> None:
    """
    Removes media items from the given manifest JSON object based on a whitelist.
    Typical use case is ingestion or processing only part of the media for whatever reason.

    Parameters:
        mnf_json (dict[str, Any]): The manifest JSON object containing a 'media_list'.
        white_list (list[str]): A list of media titles to be retained in the manifest.

    Returns:
        None: Modifies the input manifest JSON object in-place by removing media items
        not in the whitelist.
    """
    new_media_list = [
        media_item
        for media_item in mnf_json["media_list"]
        if media_item["media_title"] in white_list
    ]
    mnf_json["media_list"] = new_media_list
