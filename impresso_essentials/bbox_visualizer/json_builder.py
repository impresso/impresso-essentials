"""Module allowing to generate a JSON file used to visualize the bounding boxes of a canonical Newspaper element (issue, page or content-item).

Usage:
    python json_builder.py <element_ID> --level <level of bboxes> --output <output_path.json> --verbose --log-file <path/to/log_file>

- element_id (positional) : ID of the element you want to extract the JSON from
- level : level of the bounding boxes you want to visualize, it can be from {regions,paragraphs,lines,tokens}
- output : path where the correspondin JSON with the bounding boxes will be outputed.
- verbose : set the log level to DEBUG, otherwise will be INFO.
- log-file : path to logfile to use, otherwise will print in stdout.
"""
import json
import logging
import argparse
from dask import bag as db

from impresso_essentials.io.s3 import IMPRESSO_STORAGEOPT
from impresso_essentials.utils import init_logger
from impresso_essentials.bbox_visualizer.get_bbox import (
    get_page_bounding_boxes,
    get_ci_bounding_boxes,
    get_issue_bounding_boxes,
    create_s3_path
)

logger = logging.getLogger(__name__)

def build_bbox_json(
    element_id: str, level: str = "regions", output_path: str = None
) -> dict:
    """
    Build the JSON of the bounding boxes of a page, CI, or issue at the specified level.

    Args:
        id (str): The id of the page, CI, or issue.
        level (str): The level at which to extract the bounding boxes
                     Options: "regions", "paragraphs", "lines", "tokens"
        output_path (str): Optional output file path

    Returns:
        dict: The JSON structure containing the bounding boxes

    Raises:
        ValueError: If the level is not recognized or if the element_id is invalid
    """
    s3_path = create_s3_path(element_id)
    # Build Dask graph and compute manifest list in one go
    manifest_list = (
        db.read_text(s3_path, storage_options=IMPRESSO_STORAGEOPT)
        .map(json.loads)
        .filter(lambda r: r.get("id") == element_id)
        .compute()
    )
    if not manifest_list:
        raise ValueError(f"Manifest for id {element_id} not found.")
    manifest_json = manifest_list[0]

    id_parts = element_id.split("-")
    bounding_boxes = {}
    if len(id_parts) == 6:  # either a page or a CI
        if "p" in id_parts[-1]:  # a page (canonical manifest)
            bounding_boxes = get_page_bounding_boxes(manifest_json, level)
        elif "i" in id_parts[-1]: # a CI (rebuilt manifest)
            bounding_boxes = get_ci_bounding_boxes(manifest_json, level)
    elif len(id_parts) == 5:  # It is an issue
        bounding_boxes = get_issue_bounding_boxes(manifest_json, level)
    else:
        raise ValueError("Invalid id format.")

    bbox_json = {
        "iiif_img_base_uri": list(bounding_boxes.keys()),
        "bboxes": bounding_boxes,
    }
    new_uri =  []
    for iiif_img_base_uri in bbox_json["iiif_img_base_uri"]:
            if "unibas" in iiif_img_base_uri:
                bbox_json["bboxes"][iiif_img_base_uri + "/info.json"] = bbox_json["bboxes"].pop(iiif_img_base_uri)   
                new_uri.append(iiif_img_base_uri + "/info.json")
    if new_uri:            
        bbox_json["iiif_img_base_uri"] = new_uri
            
    if not output_path:
        output_path = f"{element_id}_bbox.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(bbox_json, f)

    return bbox_json


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build the JSON of bounding boxes for a given element."
    )
    parser.add_argument(
        "element_id", type=str, help="The id of the element (page, CI, or issue)"
    )
    parser.add_argument(
        "--level",
        type=str,
        default="regions",
        choices=["regions", "paragraphs", "lines", "tokens"],
        help="The level at which to extract bounding boxes (default: regions)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional output file path (default: <element_id>_bbox.json)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging (default: False)",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Path to the log file (default: None)",
    )

    args = parser.parse_args()
    log_level = logging.DEBUG if args.verbose else logging.INFO

    init_logger(logger, log_level, args.log_file) 

    build_bbox_json(args.element_id, args.level, args.output)
    logger.info(
        f"Bounding boxes JSON for {args.element_id} at level {args.level} saved to {args.output}"
    )
