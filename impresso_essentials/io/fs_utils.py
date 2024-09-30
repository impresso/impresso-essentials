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
        logger.info("File %s does not exist.", filename)


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
    issuedir: IssueDir, suffix: str = None, extension: str = None, as_dir: bool = False
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

    Returns:
        str: Constructed canonical ID, filename or canonical path for given IssueDir.
    """
    sep = "/" if as_dir else "-"
    base = sep.join(
        [
            issuedir.journal,
            str(issuedir.date.year),
            str(issuedir.date.month).zfill(2),
            str(issuedir.date.day).zfill(2),
            issuedir.edition,
        ]
    )

    # if the suffix and extension are not defined, return directly
    if as_dir or not (extension or suffix):
        return base

    if suffix:
        base = sep.join([base, suffix])
    if extension:
        base = f"{base}.{extension}" if "." not in extension else f"{base}{extension}"

    return base


def check_filenaming(
    file_basename: str, object_type: str = "issue"
) -> re.Match[str] | None:
    """Check whether a file's basename complies with the naming convention.

    Args:
        file_basename (str): Basename of file to check (excluding extension).
        object_type (str, optional): Type of objects in the given file.
            One of "issue", "page", "rebuilt". Defaults to 'issues'.

    Returns:
        Match[str] | None: The resulting match if correct, None otherwise.
    """
    # if the file extension is still included, remove it.
    if "." in file_basename:
        file_basename = file_basename.split(".")[0]
    match object_type:
        case "issue":
            pattern = re.compile(r"^[A-Z]+-\d{4}-issues$")
        case "page":
            pattern = re.compile(r"^[A-Z]+-\d{4}-\d{2}-\d{2}-[a-z]-pages$")
        case "rebuilt":
            pattern = re.compile(r"^[A-Z]+-\d{4}$")

    return pattern.match(file_basename)


def check_id(canonical_id: str, object_type: str = "issue") -> re.Match[str] | None:
    """Check whether a canonical ID complies with the naming convention.

    Args:
        canonical_id (str): Canonical ID to check.
        object_type (str, optional): Object it corresponds to.
            One of "issue", "page", "content-item". Defaults to 'issues'.

    Returns:
        Match[str] | None: The resulting match if correct, None otherwise
    """
    match object_type:
        case "issue":
            pattern = re.compile(r"^[A-Z]+-\d{4}-\d{2}-\d{2}-[a-z]$")
        case "page":
            pattern = re.compile(r"^[A-Z]+-\d{4}-\d{2}-\d{2}-[a-z]-p\d{4}$")
        case "content-item":
            pattern = re.compile(r"^[A-Z]+-\d{4}-\d{2}-\d{2}-[a-z]-i\d{4}$")
    return pattern.match(canonical_id)


def get_issueshortpath(issuedir: IssueDir) -> str:
    """Return short version of an IssueDir's path, starting from the journal.

    Args:
        issuedir (IssueDir): IssueDir instance from which to get the short path.

    Returns:
        str: Canonical path to the issue starting at the journal name.
    """
    path = issuedir.path
    return path[path.index(issuedir.journal) :]


def parse_canonical_filename(filename: str) -> tuple[str, tuple, str, str, int, str]:
    """Parse a canonical page or CI ID or filename into its components.

    >>> filename = "GDL-1950-01-02-a-i0002"
    >>> parse_canonical_filename(filename)
    >>> ('GDL', ('1950', '01', '02'), 'a', 'i', 2, '')

    Args:
        filename (str): ID or filename to parse.

    Returns:
        tuple[str, tuple, str, str, int, str]: Parsed ID or filename.
    """
    regex = re.compile(
        (
            r"^(?P<np>[A-Za-z0-9]+)-(?P<year>\d{4})"
            r"-(?P<month>\d{2})-(?P<day>\d{2})"
            r"-(?P<ed>[a-z])-(?P<type>[p|i])(?P<pgnb>\d{4})(?P<ext>.*)?$"
        )
    )
    result = re.match(regex, filename)
    newspaper_id = result.group("np")
    date = (result.group("year"), result.group("month"), result.group("day"))
    page_number = int(result.group("pgnb"))
    edition = result.group("ed")
    filetype = result.group("type")
    extension = result.group("ext")
    return (newspaper_id, date, edition, filetype, page_number, extension)
