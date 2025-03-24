import json
from dask import bag as db
from impresso_essentials.io.s3 import IMPRESSO_STORAGEOPT

from impresso_essentials.bbox_visualizer.get_bbox import (
    get_page_bounding_boxes,
    get_ci_bounding_boxes,
    get_issue_bounding_boxes,
)
from impresso_essentials.bbox_visualizer.utils import (
    create_s3_path,
)


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
        dict: The JSON structure containing the bounding boxes.
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
    manifest = manifest_list[0]

    id_parts = element_id.split("-")
    if len(id_parts) == 6:  # either a page or a CI
        if "p" in id_parts[-1]:  # a page (canonical manifest)
            bounding_boxes = get_page_bounding_boxes(manifest, level)
        elif "i" in id_parts[-1]:
            bounding_boxes = get_ci_bounding_boxes(manifest, level)
    elif len(id_parts) == 5:  # It is an issue
        bounding_boxes = get_issue_bounding_boxes(manifest, level)
    else:
        raise ValueError("Invalid id format.")

    bbox_json = {
        "iiif_img_base_uri": list(bounding_boxes.keys()),
        "bboxes": bounding_boxes,
    }

    if not output_path:
        output_path = f"{element_id}_bbox.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(bbox_json, f)
    return bbox_json


# if __name__ == "__main__":
#     import argparse

#     parser = argparse.ArgumentParser(
#         description="Build the JSON of bounding boxes for a given element."
#     )
#     parser.add_argument(
#         "element_id", type=str, help="The id of the element (page, CI, or issue)"
#     )
#     parser.add_argument(
#         "--level",
#         type=str,
#         default="regions",
#         choices=["regions", "paragraphs", "lines", "tokens"],
#         help="The level at which to extract bounding boxes (default: regions)",
#     )
#     parser.add_argument(
#         "--output",
#         type=str,
#         default=None,
#         help="Optional output file path (default: <element_id>_bbox.json)",
#     )
#     args = parser.parse_args()

#     # Call your function
#     result = build_bbox_json(args.element_id, args.level, args.output)
#     print("JSON built successfully:", result)
