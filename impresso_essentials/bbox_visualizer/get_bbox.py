"""
Helper functions to extract bounding boxes from the manifest,
with improvements to reduce S3 calls.
"""
import logging
import json
from dask import bag as db
from tqdm import tqdm

from impresso_essentials.io.s3 import IMPRESSO_STORAGEOPT

TYPE_MAPPINGS = {
    "article": "ar",
    "ar": "ar",
    "advertisement": "ad",
    "ad": "ad",
    "pg": None,
    "image": "img",
    "table": "tb",
    "death_notice": "ob",
    "weather": "w",
    "page":"page"
}

# Global cache for CI type lookups
_ci_type_cache = {}
MAX_CACHE_SIZE = 10000

logger = logging.getLogger(__name__)

def create_s3_path(element_id: str) -> str:
    """
    Constructs the S3 path based on the provided id.

    Args:
        id (str): Identifier string for page, CI, or issue.

    Returns:
        str: S3 path string.

    Raises:
        ValueError: If the id format is invalid.
    """
    s3_path = "s3://"
    id_parts = element_id.split("-")
    newspaper = id_parts[0]
    year = id_parts[1]
    if len(id_parts) == 6:  # Either a page or a CI
        month = id_parts[2]
        day = id_parts[3]
        edition = id_parts[4]
        if "p" in id_parts[-1]:  # It's a page (canonical manifest)
            s3_path += "12-canonical-final/"
            s3_path += f"{newspaper}/pages/"
            s3_path += f"{newspaper}-{year}/"
            s3_path += f"{newspaper}-{year}-{month}-{day}-{edition}-pages.jsonl.bz2"
            return s3_path
        elif "i" in id_parts[-1]:
            s3_path += "22-rebuilt-final/"
            s3_path += f"{newspaper}/"
            s3_path += f"{newspaper}-{year}.jsonl.bz2"
            return s3_path
    elif len(id_parts) == 5:  # It is an issue
        s3_path += "12-canonical-final/"
        s3_path += f"{newspaper}/issues/"
        s3_path += f"{newspaper}-{year}-issues.jsonl.bz2"
        return s3_path
    else:
        raise ValueError("Invalid id format")


def get_base_url(canonical_page_json: dict) -> str:
    """
    Retrieves the base URL of the IIIF server from page JSON in canonical format.

    Args:
        canonical_page_json (dict): JSON object of the page in canonical format.
        This should contain either "iiif" or "iiif_img_base_uri".

    Returns:
        str: The IIIF base URL.
    """
    if "iiif" in canonical_page_json:
        return canonical_page_json["iiif"]
    elif "iiif_img_base_uri" in canonical_page_json:
        return canonical_page_json["iiif_img_base_uri"]
    else:
        raise ValueError("No IIIF base URL found in the manifest")


def create_image_url(canonical_page_json: dict) -> str:
    """
    Creates the URL for the page image using the base IIIF URL.

    Args:
        canonical_page_json (dict): JSON object of the page in canonical format.

    Returns:
        str: URL of the page image.
    """
    return f"{get_base_url(canonical_page_json)}/full/full/0/default.jpg"


def get_page_bounding_boxes(canonical_page_json: dict, level: str = "regions") -> dict:
    """
    Extract bounding boxes from the manifest at the specified level

    Args:
        canonical_page_json (dict): JSON object of the page in canonical format.
        level (str): The level at which to extract the bounding boxes
                     Options: "regions", "paragraphs", "lines", "tokens"

    Returns:
        dict: A dictionary mapping the base image URL to a list of bounding boxes

    Raises:
        ValueError: If the level is not recognized
    """
    base_url = get_base_url(canonical_page_json)

    if level == "regions":
        return {base_url: _get_page_regions_bboxes(canonical_page_json)}
    elif level == "paragraphs":
        return {base_url: _get_page_paragraphs_bboxes(canonical_page_json)}
    elif level == "lines":
        return {base_url: _get_page_lines_bboxes(canonical_page_json)}
    elif level == "tokens":
        return {base_url: _get_page_tokens_bboxes(canonical_page_json)}
    else:
        raise ValueError(f"Unknown level: {level}")


def _get_page_regions_bboxes(canonical_page_json: dict):
    """
    Extract bounding boxes of the regions from the manifest.

    Args:
        canonical_page_json (dict): JSON object of the page in canonical format.
    """
    bboxes = []
    page_number = page_number = int(canonical_page_json["id"].split("-")[-1].replace("p", ""))
    for region in tqdm(
        canonical_page_json["r"], desc=f"Getting bboxes for page {page_number}"
    ):
        ci = None
        if "pOf" in region:
            ci = region["pOf"]
        ci_type = get_ci_type(ci)
        bboxes.append({"t": ci_type, "ci": ci, "c": region["c"]})
    return bboxes


def _get_page_paragraphs_bboxes(canonical_page_json: dict):
    """
    Extract bounding boxes of the paragraphs from the manifest.

    Args:
        canonical_page_json (dict): JSON object of the page in canonical format.
    """
    page_number = page_number = int(canonical_page_json["id"].split("-")[-1].replace("p", ""))
    bboxes = []
    for region in tqdm(
        canonical_page_json["r"], desc=f"Getting bboxes for page {page_number}"
    ):
        ci = None
        if "pOf" in region:
            ci = region["pOf"]
        ci_type = get_ci_type(ci)
        for p in region["p"]:
            bboxes.append({"t": ci_type, "ci": ci, "c": p["c"]})
    return bboxes


def _get_page_lines_bboxes(canonical_page_json: dict):
    """
    Extract bounding boxes of the lines from the manifest.

    Args:
        canonical_page_json (dict): JSON object of the page in canonical format.
    """
    bboxes = []
    page_number = page_number = int(canonical_page_json["id"].split("-")[-1].replace("p", ""))
    for region in tqdm(
        canonical_page_json["r"], desc=f"Getting bboxes for page {page_number}"
    ):
        ci = None
        if "pOf" in region:
            ci = region["pOf"]
        ci_type = get_ci_type(ci)
        for p in region["p"]:
            for line in p["l"]:
                bboxes.append({"t": ci_type, "ci": ci, "c": line["c"]})
    return bboxes


def _get_page_tokens_bboxes(canonical_page_json: dict):
    """
    Extract bounding boxes of the tokens from the manifest.

    Args:
        canonical_page_json (dict): JSON object of the page in canonical format.
    """
    bboxes = []
    page_number = page_number = int(canonical_page_json["id"].split("-")[-1].replace("p", ""))
    for region in tqdm(
        canonical_page_json["r"], desc=f"Getting bboxes for page {page_number}"
    ):
        ci = None
        if "pOf" in region:
            ci = region["pOf"]
        ci_type = get_ci_type(ci)
        for p in region["p"]:
            for line in p["l"]:
                for t in line["t"]:
                    bboxes.append({"t": ci_type, "ci": ci, "c": t["c"]})
    return bboxes


def get_ci_type(ci_id: str) -> str:
    """
    Get the type of the CI from its ID from the canonical manifest of the issue.

    Uses a cache to avoid repeated S3 calls.

    Args:
        ci_id (str): The ID of the CI

    Returns:
        str: The mapped CI type
    """
    if ci_id is None:
        return None

    if ci_id in _ci_type_cache:
        return _ci_type_cache[ci_id]

    ci_id_parts = ci_id.split("-")
    # Construct the issue id from the first 5 parts
    issue_id = f"{ci_id_parts[0]}-{ci_id_parts[1]}-{ci_id_parts[2]}-{ci_id_parts[3]}-{ci_id_parts[4]}"
    issue_s3_path = create_s3_path(issue_id)

    # Build the Dask graph and compute once
    ci_type_raw = (
        db.read_text(issue_s3_path, storage_options=IMPRESSO_STORAGEOPT)
        .map(json.loads)
        .filter(lambda r: r.get("id") == issue_id)
        .pluck("i")
        .flatten()
        .pluck("m")
        .filter(lambda r: r.get("id") == ci_id)
        .pluck("tp")
        .compute()[0]
    )
    mapped_type = TYPE_MAPPINGS[ci_type_raw]
    _ci_type_cache[ci_id] = mapped_type
    # Clear the cache if it exceeds the maximum size
    if len(_ci_type_cache) > MAX_CACHE_SIZE:
        _ci_type_cache.clear()
    return mapped_type


def get_ci_bounding_boxes(rebuilt_ci_json: dict, level: str = "regions") -> dict:
    """
    Extract bounding boxes from the CI manifest at the specified level from the rebuilt manifest.

    Args:
            rebuilt_ci_json (dict): The JSON dict of a CI from the rebuilt manifest
            level (str): The level at which to extract the bounding boxes
                    - "regions": Extract the bounding boxes of the regions
                    - "tokens": Extract the bounding boxes of the tokens
                    - Default: "regions"
    Returns:
            dict: A dictionary of bounding boxes (coordinates) type and CI ID with the image URL as key
    """
    bounding_boxes = {}
    # We have to fetch the page canonical manifests to get the image URLs
    pages_s3_path = create_s3_path(rebuilt_ci_json["ppreb"][0]["id"])
    page_manifests = (
        db.read_text(pages_s3_path, storage_options=IMPRESSO_STORAGEOPT)
        .map(json.loads)
        .compute()
    )
    for page in tqdm(
        rebuilt_ci_json["ppreb"],
        desc=f"Getting bboxes for each page of CI {rebuilt_ci_json['id']}",
    ):
        page_manifest = next(
            (page_m for page_m in page_manifests if page_m.get("id") == page["id"])
        )
        image_url = get_base_url(page_manifest)
        if level == "regions":
            bounding_boxes[image_url] = []
            for region in page["r"]:  # For each box given as region
                bounding_boxes[image_url].append(
                    {"t": rebuilt_ci_json["tp"], "ci": rebuilt_ci_json["id"], "c": region}
                )
        elif level == "tokens":
            bounding_boxes[image_url] = []
            for token in page["t"]:
                bounding_boxes[image_url].append(
                    {"t": rebuilt_ci_json["tp"], "ci": rebuilt_ci_json["id"], "c": token["c"]}
                )
    return bounding_boxes


def get_issue_bounding_boxes(canonical_issue_json: dict, level: str = "regions") -> dict:
    """
    Extract bounding boxes from the issue manifest at the specified level from the rebuilt manifest.

    Args:
        canonical_issue_json (dict): The JSON dict of an issue from the canonical manifest
        level (str): The level at which to extract the bounding boxes
                     Options: "regions", "tokens"

    Returns:
        dict: A dictionary mapping image URLs to lists of bounding boxes.
    """
    bounding_boxes = {}
    pages_manifest_s3_path = create_s3_path(canonical_issue_json["pp"][0])
    pages = (
        db.read_text(pages_manifest_s3_path, storage_options=IMPRESSO_STORAGEOPT)
        .map(json.loads)
        .take(len(canonical_issue_json["pp"]))
    )
    logger.info(f"Getting bboxes for {len(pages)} pages:")
    for page_manifest in pages:
        bounding_boxes.update(get_page_bounding_boxes(page_manifest, level))
    return bounding_boxes
