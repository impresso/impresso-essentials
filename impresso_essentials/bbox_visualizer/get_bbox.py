"""
Helper functions to extract bounding boxes from the manifest,
with improvements to reduce S3 calls.
"""

import json
from dask import bag as db
from tqdm import tqdm

from impresso_essentials.bbox_visualizer.utils import (
    create_s3_path,
    get_base_url,
)
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
}

# Global cache for CI type lookups
_ci_type_cache = {}


def get_page_bounding_boxes(page_dict: dict, level: str = "regions") -> dict:
    """
    Extract bounding boxes from the manifest at the specified level

    Args:
        page_dict (dict): The manifest of a page
        level (str): The level at which to extract the bounding boxes
                     Options: "regions", "paragraphs", "lines", "tokens"

    Returns:
        dict: A dictionary mapping the base image URL to a list of bounding boxes
    """
    base_url = get_base_url(page_dict)

    if level == "regions":
        return {base_url: _get_page_regions_bboxes(page_dict)}
    elif level == "paragraphs":
        return {base_url: _get_page_paragraphs_bboxes(page_dict)}
    elif level == "lines":
        return {base_url: _get_page_lines_bboxes(page_dict)}
    elif level == "tokens":
        return {base_url: _get_page_tokens_bboxes(page_dict)}
    else:
        raise ValueError(f"Unknown level: {level}")


def _get_page_regions_bboxes(page_manifest):
    """
    Extract bounding boxes of the regions from the manifest.
    """
    bboxes = []
    page_number = page_number = int(page_manifest["id"].split("-")[-1].replace("p", ""))
    for region in tqdm(
        page_manifest["r"], desc=f"Getting bboxes for page {page_number}"
    ):
        ci = None
        if "pOf" in region:
            ci = region["pOf"]
        ci_type = get_ci_type(ci)
        bboxes.append({"t": ci_type, "ci": ci, "c": region["c"]})
    return bboxes


def _get_page_paragraphs_bboxes(page_manifest):
    """
    Extract bounding boxes of the paragraphs from the manifest.
    """
    page_number = page_number = int(page_manifest["id"].split("-")[-1].replace("p", ""))
    bboxes = []
    for region in tqdm(
        page_manifest["r"], desc=f"Getting bboxes for page {page_number}"
    ):
        ci = None
        if "pOf" in region:
            ci = region["pOf"]
        ci_type = get_ci_type(ci)
        for p in region["p"]:
            bboxes.append({"t": ci_type, "ci": ci, "c": p["c"]})
    return bboxes


def _get_page_lines_bboxes(page_manifest):
    """
    Extract bounding boxes of the lines from the manifest.
    """
    bboxes = []
    page_number = page_number = int(page_manifest["id"].split("-")[-1].replace("p", ""))
    for region in tqdm(
        page_manifest["r"], desc=f"Getting bboxes for page {page_number}"
    ):
        ci = None
        if "pOf" in region:
            ci = region["pOf"]
        ci_type = get_ci_type(ci)
        for p in region["p"]:
            for l in p["l"]:
                bboxes.append({"t": ci_type, "ci": ci, "c": l["c"]})
    return bboxes


def _get_page_tokens_bboxes(page_manifest):
    """
    Extract bounding boxes of the tokens from the manifest.
    """
    bboxes = []
    page_number = page_number = int(page_manifest["id"].split("-")[-1].replace("p", ""))
    for region in tqdm(
        page_manifest["r"], desc=f"Getting bboxes for page {page_number}"
    ):
        ci = None
        if "pOf" in region:
            ci = region["pOf"]
        ci_type = get_ci_type(ci)
        for p in region["p"]:
            for l in p["l"]:
                for t in l["t"]:
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
    return mapped_type


def get_ci_bounding_boxes(ci_manifest: dict, level: str = "regions") -> dict:
    """
    Extract bounding boxes from the CI manifest at the specified level from the rebuilt manifest.

    Args:
            ci_manifest (dict): The manifest of a CI
            level (str): The level at which to extract the bounding boxes
                    - "regions": Extract the bounding boxes of the regions
                    - "tokens": Extract the bounding boxes of the tokens
                    - Default: "regions"
    Returns:
            dict: A dictionary of bounding boxes (coordinates) type and CI ID with the image URL as key
    """
    bounding_boxes = {}
    # We have to fetch the page canonical manifests to get the image URLs
    pages_s3_path = create_s3_path(ci_manifest["ppreb"][0]["id"])
    page_manifests = (
        db.read_text(pages_s3_path, storage_options=IMPRESSO_STORAGEOPT)
        .map(json.loads)
        .compute()
    )
    for page in tqdm(
        ci_manifest["ppreb"],
        desc=f"Getting bboxes for each page of CI {ci_manifest['id']}",
    ):
        page_manifest = next(
            (page_m for page_m in page_manifests if page_m.get("id") == page["id"])
        )
        image_url = get_base_url(page_manifest)
        if level == "regions":
            bounding_boxes[image_url] = []
            for region in page["r"]:  # For each box given as region
                bounding_boxes[image_url].append(
                    {"t": ci_manifest["tp"], "ci": ci_manifest["id"], "c": region}
                )
        elif level == "tokens":
            bounding_boxes[image_url] = []
            for token in page["t"]:
                bounding_boxes[image_url].append(
                    {"t": ci_manifest["tp"], "ci": ci_manifest["id"], "c": token["c"]}
                )
    return bounding_boxes


def get_issue_bounding_boxes(issue_manifest: dict, level: str = "regions") -> dict:
    """
    Extract bounding boxes from the issue manifest at the specified level from the rebuilt manifest.

    Args:
        issue_manifest (dict): The manifest of an issue
        level (str): The level at which to extract the bounding boxes
                     Options: "regions", "tokens"

    Returns:
        dict: A dictionary mapping image URLs to lists of bounding boxes.
    """
    bounding_boxes = {}
    pages_manifest_s3_path = create_s3_path(issue_manifest["pp"][0])
    pages = (
        db.read_text(pages_manifest_s3_path, storage_options=IMPRESSO_STORAGEOPT)
        .map(json.loads)
        .take(len(issue_manifest["pp"]))
    )
    print(f"Getting bboxes for {len(pages)} pages:")
    for page_manifest in pages:
        bounding_boxes.update(get_page_bounding_boxes(page_manifest, level))
    return bounding_boxes
