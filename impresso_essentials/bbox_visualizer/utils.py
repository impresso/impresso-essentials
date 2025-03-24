"""
Utils function for the Bounding Box Visualizer
"""

def create_s3_path(element_id: str) -> str:
    """
    Create the S3 path of the manifest of the element from its ID
    
    Args:   
        element_id (str): The ID of the element (page, CI, or issue)

    Returns:
        str: The S3 path of the manifest of the element
    """
    s3_path = "s3://"
    id_parts = element_id.split("-")
    newspaper = id_parts[0]
    year = id_parts[1]
    if len(id_parts) == 6:  # either a page or a CI
        month = id_parts[2]
        day = id_parts[3]
        edition = id_parts[4]
        if "p" in id_parts[-1]:  # it is a page so a canonical manifest
            s3_path += "12-canonical-final/"  # add canonical
            s3_path += f"{newspaper}/pages/"  # add newspaper and go to pages
            s3_path += f"{newspaper}-{year}/"  # add year of the issue
            s3_path += f"{newspaper}-{year}-{month}-{day}-{edition}-pages.jsonl.bz2"  # add the file name
            return s3_path
        elif "i" in id_parts[-1]:
            s3_path += "22-rebuilt-final/"  # add rebuilt
            s3_path += f"{newspaper}/"  # add newspaper
            s3_path += f"{newspaper}-{year}.jsonl.bz2"  # add year of the issue
            return s3_path
    elif len(id_parts) == 5:  # It is an issue
        s3_path += "12-canonical-final/"  # add canonical
        s3_path += f"{newspaper}/issues/"  # add newspaper and go to pages
        s3_path += f"{newspaper}-{year}-issues.jsonl.bz2"  # add the file name
        return s3_path


def get_base_url(page_manifest: dict) -> str:
    """
    Get the base URL of the IIIF server from the manifest (handles previous versions of the manifest).

    Args:
                page_manifest (dict): The manifest of the page

    Returns:
                str: The base URL of the IIIF server
    """
    if "iiif" in page_manifest:
        return page_manifest["iiif"]
    elif "iiif_img_base_uri" in page_manifest:
        return page_manifest["iiif_img_base_uri"]
    else:
        raise ValueError("No IIIF base URL found in the manifest")


def create_image_url(page_manifest: dict) -> str:
    """ "
    Create the URL of the image of the page from the manifest"
    """
    return f"{get_base_url(page_manifest)}/full/full/0/default.jpg"


# def draw_bounding_boxes(img, bounding_boxes: list[list]):
#     ci_colors = {}
#     draw = ImageDraw.Draw(img)
#     for box in bounding_boxes:
#         # Extract coordinates and calculate the rectangle endpoints
#         x, y, w, h = box["c"]
#         coords = (x, y, x + w, y + h)

#         # Use the original box dictionary to check the content item id ('ci')
#         ci = box["ci"]
#         if ci not in ci_colors:
#             # assign a random color to the ci
#             ci_colors[ci] = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
#         color = ci_colors[ci]

#         # Draw the rectangle with the assigned color
#         draw.rectangle(coords, outline=color, width=3)
#     return img
