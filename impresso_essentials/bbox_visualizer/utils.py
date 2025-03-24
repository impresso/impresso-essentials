def create_s3_path(element_id: str) -> str:
    """
    Constructs the S3 path based on the provided id.

    Args:
        id (str): Identifier string for page, CI, or issue.

    Returns:
        str: S3 path string.
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


def get_base_url(page_manifest: dict) -> str:
    """
    Retrieves the base URL of the IIIF server from the manifest.

    Args:
        page_manifest (dict): The manifest of the page.

    Returns:
        str: The IIIF base URL.
    """
    if "iiif" in page_manifest:
        return page_manifest["iiif"]
    elif "iiif_img_base_uri" in page_manifest:
        return page_manifest["iiif_img_base_uri"]
    else:
        raise ValueError("No IIIF base URL found in the manifest")


def create_image_url(page_manifest: dict) -> str:
    """
    Creates the URL for the page image using the base IIIF URL.

    Args:
        page_manifest (dict): The manifest of the page.

    Returns:
        str: URL of the page image.
    """
    return f"{get_base_url(page_manifest)}/full/full/0/default.jpg"