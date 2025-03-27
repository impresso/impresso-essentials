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