import json
import requests
from io import BytesIO
import matplotlib.pyplot as plt
from impresso_essentials.io.s3 import IMPRESSO_STORAGEOPT
from dask import bag as db
import random

from impresso_essentials.bbox_visualizer.get_bbox import (
    get_page_bounding_boxes,
    get_ci_bounding_boxes,
    get_issue_bounding_boxes
)
from impresso_essentials.bbox_visualizer.utils import create_image_url, create_s3_path
from impresso_essentials.bbox_visualizer.utils import get_base_url


def build_bbox_json(id: str, level: str = "regions", output_path: str = None) -> dict:
    """
    Build the JSON of the bounding boxes of a page at the specified level

    Args:
        id (str): The id of the page
        level (str): The level at which to extract the bounding boxes
                     - "regions": Extract the bounding boxes of the regions
                     - "paragraphs": Extract the bounding boxes of the paragraphs
                     - "lines": Extract the bounding boxes of the lines
                     - "tokens": Extract the bounding boxes of the tokens
                     - Default: "regions"

    Returns:
        dict: The JSON of the bounding boxes
    """
    s3_path = create_s3_path(id)
    # Get the dictionary from the manifest of the page in the S3
    manifest = (
        db.read_text(s3_path, storage_options=IMPRESSO_STORAGEOPT)
        .map(json.loads)
        .filter(lambda r: r.get("id") == id)
        .take(1)[0]
    )
    
    id_parts = id.split("-")
    if len(id_parts) == 6:  # either a page or a CI
        if "p" in id_parts[-1]:  # it is a page so a canonical manifest
            bounding_boxes = get_page_bounding_boxes(manifest, level)
        elif "i" in id_parts[-1]:
            bounding_boxes = get_ci_bounding_boxes(manifest, level)
    elif len(id_parts) == 5: # It is an issue
        bounding_boxes = get_issue_bounding_boxes(manifest, level)

    bbox_json = {
        "iiif_img_base_uri": list(bounding_boxes.keys()),
        "bboxes": bounding_boxes,
    }

    if not output_path:
        output_path = f"{id}_bbox.json"
    with open(output_path, "w") as f:
        json.dump(bbox_json, f)
    return bbox_json

