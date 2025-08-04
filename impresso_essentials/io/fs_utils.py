"""Code for parsing impresso's canonical directory structures."""

import sys
import os
import json
import logging
import re
import glob
from typing import Any

from impresso_essentials.utils import bytes_to, IssueDir

logger = logging.getLogger(__name__)


def parse_json(filename: str) -> dict[str, Any]:
    """Load the contents of a JSON file.

    Args:
        filename (str): Path to the json file.

    Returns:
        dict[str, Any]: Resulting json, contained inside the file
    """
    if os.path.isfile(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        msg = f"File {filename} does not exist."
        if logger.level == logging.DEBUG:
            print(msg)
        logger.info(msg)


def glob_with_size(directory: str, file_suffix: str) -> list[str]:
    """
    List all files in a directory with a given suffix and their size in MB.

    Args:
        directory (str): The directory path to search for files.
        file_suffix (str): The file extension or suffix to match.

    Returns:
        list[str]: A list of tuples, each containing the file path and its
                   size in megabytes, rounded to six decimal places.
    """
    if sys.version < "3.11":
        file_paths = glob.glob(os.path.join(directory, "*"))
    else:
        file_paths = glob.glob(os.path.join(directory, "*"), include_hidden=False)

    files = [
        (path, round(bytes_to(os.path.getsize(path), "m"), 6))
        for path in file_paths
        if path.endswith(file_suffix)
    ]

    return files


def list_local_directories(path: str) -> list[str]:
    """List the directories present at a local path.

    Args:
        path (str): Local path from which to list the directories.

    Returns:
        list[str]: List of subdirectories in `path`.
    """
    return [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]


def canonical_path(
    issuedir: IssueDir,
    suffix: str = None,
    extension: str = None,
    as_dir: bool = False,
    incl_provider: bool = True,
) -> str:
    """Create a canonical dir, filename or ID from an `IssueDir` object.

    Args:
        issuedir (IssueDir): IssueDir object to create the path for
        suffix (str, optional): Suffix to use which will follow the issue ID.
            eg. Can be 'pages', 'i0001' or `"p" + str(num).zfill(4)`. Defaults to None.
        extension (str, optional): File extension to use if creating a filename.
            Defaults to None.
        as_dir (bool, optional): Whether the result is a directory ('/' separator) or a
            filename or ID ('-' separator). Defaults to False.
        incl_provider (bool, optional): Whether to include the provider when `as_dir=True`.
            Defaults to True.

    Returns:
        str: Constructed canonical ID, filename or canonical path for given IssueDir.
    """
    sep = "/" if as_dir else "-"

    # if in dir mode add provider
    parts = [issuedir.provider] if incl_provider and as_dir else []
    parts.extend(
        [
            issuedir.alias,
            str(issuedir.date.year),
            str(issuedir.date.month).zfill(2),
            str(issuedir.date.day).zfill(2),
            issuedir.edition,
        ]
    )
    base = sep.join(parts)

    # if the suffix and extension are not defined, return directly
    if as_dir or not (extension or suffix):
        return base

    if suffix:
        base = sep.join([base, suffix])
    if extension:
        base = f"{base}.{extension}" if "." not in extension else f"{base}{extension}"

    return base


def check_filenaming(file_basename: str, object_type: str = "issue") -> re.Match[str] | None:
    """Check whether a file's basename complies with the naming convention.

    Args:
        file_basename (str): Basename of file to check (excluding extension).
        object_type (str, optional): Type of objects in the given file.
            One of "issue", "page", "audio", "rebuilt". Defaults to 'issue'.

    Returns:
        Match[str] | None: The resulting match if correct, None otherwise.
    """
    # if the file extension is still included, remove it.
    if "." in file_basename:
        file_basename = file_basename.split(".")[0]
    match object_type:
        case "issue":
            pattern = re.compile(r"^[A-Za-z][A-Za-z0-9_]*-\d{4}-issues$")
        case "page":
            pattern = re.compile(r"^[A-Za-z][A-Za-z0-9_]*-\\d{4}-\\d{2}-\\d{2}-[a-z]{1,2}-pages$")
        case "audio":
            pattern = re.compile(r"^[A-Za-z][A-Za-z0-9_]*-\\d{4}-\\d{2}-\\d{2}-[a-z]{1,2}-audios$")
        case "rebuilt":
            pattern = re.compile(r"^[A-Za-z][A-Za-z0-9_]*-\d{4}$")

    return pattern.match(file_basename)


def check_id(canonical_id: str, object_type: str = "issue") -> re.Match[str] | None:
    """Check whether a canonical ID complies with the naming convention.

    Args:
        canonical_id (str): Canonical ID to check.
        object_type (str, optional): Object it corresponds to.
            One of "issue", "page", "audio", "content-item". Defaults to 'issue'.

    Returns:
        Match[str] | None: The resulting match if correct, None otherwise
    """
    match object_type:
        case "issue":
            pattern = re.compile(r"^[A-Za-z][A-Za-z0-9_]*-\\d{4}-\\d{2}-\\d{2}-[a-z]{1,2}$")
        case "page":
            pattern = re.compile(
                r"^[A-Za-z][A-Za-z0-9_]*-\\d{4}-\\d{2}-\\d{2}-[a-z]{1,2}-p[0-9]{4}$"
            )
        case "audio":
            pattern = re.compile(
                r"^[A-Za-z][A-Za-z0-9_]*-\\d{4}-\\d{2}-\\d{2}-[a-z]{1,2}-r[0-9]{4}$"
            )
        case "content-item":
            pattern = re.compile(
                r"^[A-Za-z][A-Za-z0-9_]*-\\d{4}-\\d{2}-\\d{2}-[a-z]{1,2}-i[0-9]{4}$"
            )
    return pattern.match(canonical_id)


def get_issueshortpath(issuedir: IssueDir, incl_provider: bool = True) -> str:
    """Return short version of an IssueDir's path, starting from the media alias.

    If `incl_provider`, beware that the provider needs to be

    Args:
        issuedir (IssueDir): IssueDir instance from which to get the short path.
        incl_provider (bool, optional): whether to include the provider. Defaults to True.

    Returns:
        str: Canonical path to the issue starting at the media alias.
    """
    path = issuedir.path
    if incl_provider:
        if issuedir.provider in path:
            return path[path.index(issuedir.provider) :]
        else:
            msg = f"Warning! Provider not in path {path}. Acting as if `incl_provider=False`."
            print(msg)
            logger.warning(msg)
    return path[path.index(issuedir.alias) :]


def parse_canonical_filename(filename: str) -> tuple[str, tuple, str, str, int, str]:
    """Parse a canonical page/audio or CI ID or filename into its components.

    >>> filename = "GDL-1950-01-02-a-i0002"
    >>> parse_canonical_filename(filename)
    >>> ('GDL', ('1950', '01', '02'), 'a', 'i', 2, '')

    The second-to-last element is the "filetype", and can have 3 values if defined:
    - i : the element is a content-item
    - p : the element is a page
    - r : the element is an audio record

    Args:
        filename (str): ID or filename to parse.

    Returns:
        tuple[str, tuple, str, str, int, str]: Parsed ID or filename.
    """
    regex = re.compile(
        (
            r"^(?P<alias>[A-Za-z][A-Za-z0-9_]*)-(?P<year>\d{4})"
            r"-(?P<month>\d{2})-(?P<day>\d{2})"
            r"-(?P<ed>[a-z]{1,2})-(?P<type>[r|p|i])(?P<pgnb>\d{4})(?P<ext>.*)?$"
        )
    )
    result = re.match(regex, filename)
    alias = result.group("alias")
    date = (result.group("year"), result.group("month"), result.group("day"))
    page_number = int(result.group("pgnb"))
    edition = result.group("ed")
    filetype = result.group("type")
    extension = result.group("ext")
    return (alias, date, edition, filetype, page_number, extension)
