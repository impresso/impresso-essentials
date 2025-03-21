"""
Helper functions to extract bounding boxes from the manifest
"""

import json
from dask import bag as db

from impresso_essentials.bbox_visualizer.utils import create_image_url, create_s3_path, get_base_url
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

# This could be done recursively to handle nested structures
# but for now we will keep it simple
def get_page_bounding_boxes(
    page_dict: dict[dict], level: str = "regions"
) -> list[list]:
    """
    Extract bounding boxes from the manifest at the specified level

    Args:
            page_dict (dict): The manifest of a page
            level (str): The level at which to extract the bounding boxes
                    - "regions": Extract the bounding boxes of the regions
                    - "paragraphs": Extract the bounding boxes of the paragraphs
                    - "lines": Extract the bounding boxes of the lines
                    - "tokens": Extract the bounding boxes of the tokens
                    - Default: "regions"

    Returns:
            list[list]: A list of bounding boxes (coordinates)
    """
    base_url = get_base_url(page_dict)

    if level == "regions":
        return {f"{base_url}": _get_page_regions_bboxes(page_dict)}
    elif level == "paragraphs":
        return {f"{base_url}": _get_page_paragraphs_bboxes(page_dict)}
    elif level == "lines":
        return {f"{base_url}": _get_page_lines_bboxes(page_dict)}
    elif level == "tokens":
        return {f"{base_url}": _get_page_tokens_bboxes(page_dict)}
    else:
        raise ValueError(f"Unknown level: {level}")


def _get_page_regions_bboxes(page_manifest):
    """
    Extract bounding boxes of the regions from the manifest
    """
    bboxes = []
    for region in page_manifest["r"]:
        ci_type = get_ci_type(region["pOf"])
        bboxes.append({"t": ci_type, "ci": region["pOf"], "c": region["c"]})
    return bboxes


def _get_page_paragraphs_bboxes(page_manifest):
    """
    Extract bounding boxes of the paragraphs from the manifest
    """
    bboxes = []
    for region in page_manifest["r"]:
        ci_type = get_ci_type(region["pOf"])
        for p in region["p"]:
            bboxes.append({"t": ci_type, "ci": region["pOf"], "c": p["c"]})
    return bboxes


def _get_page_lines_bboxes(page_manifest):
    """ "
    Extract bounding boxes of the lines from the manifest
    """
    bboxes = []
    for region in page_manifest["r"]:
        ci_type = get_ci_type(region["pOf"])
        for p in region["p"]:
            for l in p["l"]:
                bboxes.append({"t": ci_type, "ci": region["pOf"], "c": l["c"]})
    return bboxes


def _get_page_tokens_bboxes(page_manifest):
    """ "
    Extract bounding boxes of the tokens from the manifest
    """
    bboxes = []
    for region in page_manifest["r"]:
        ci_type = get_ci_type(region["pOf"])
        for p in region["p"]:
            for l in p["l"]:
                for t in l["t"]:
                    bboxes.append({"t": ci_type, "ci": region["pOf"], "c": t["c"]})
    return bboxes


def get_ci_type(ci_id: str) -> str:
    """
    Get the type of the CI from its ID from the canonical manifest of the issue

    Args:
            ci_id (str): The ID of the CI

    Returns:
            str: The type of the CI
    """
    if ci_id is None: # If there is no associated content item
        return None
    ci_id_parts = ci_id.split("-")
    issue_id = f"{ci_id_parts[0]}-{ci_id_parts[1]}-{ci_id_parts[2]}-{ci_id_parts[3]}-{ci_id_parts[4]}"
    issue_s3_path = create_s3_path(issue_id)
    return TYPE_MAPPINGS[(
        db.read_text(issue_s3_path, storage_options=IMPRESSO_STORAGEOPT)
        .map(json.loads)
        .filter(lambda r: r.get("id") == issue_id)
        .pluck("i")
        .flatten()
        .pluck("m")
        .filter(lambda r: r.get("id") == ci_id)
        .pluck("tp")
        .compute()[0]
    )]


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
    for page in ci_manifest["ppreb"]:
        page_s3_path = create_s3_path(page["id"])
        page_manifest = (
            db.read_text(page_s3_path, storage_options=IMPRESSO_STORAGEOPT)
            .map(json.loads)
            .filter(lambda r: r.get("id") == page["id"])
            .take(1)[0]
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
