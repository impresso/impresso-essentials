"""Helper functions for data versioning manifests relating to git."""

import os
import logging
from typing import Optional
import re
import git

logger = logging.getLogger(__name__)

###########################
###### GIT FUNCTIONS ######


def write_dump_to_fs(file_contents: str, abs_path: str, filename: str) -> Optional[str]:
    """Write a provided string dump to the local filesystem given its path and filename.

    TODO: Potentially moving this method to `utils.py`.

    Args:
        file_contents (str): Dumped contents in str format, ready to be written.
        abs_path (str): Local path to the directory in which the file will be.
        filename (str): Filename of the file to write, including its extension.

    Returns:
        Optional[str]: Full path of writen file, or None if an IOError occurred.
    """
    full_file_path = os.path.join(abs_path, filename)

    # ensure the path where to write the local manifest exists
    os.makedirs(abs_path, exist_ok=True)

    # write file to absolute path provided
    try:
        with open(full_file_path, "w", encoding="utf-8") as outfile:
            outfile.write(file_contents)

        return full_file_path
    except IOError as e:
        logger.error("Writing dump %s to local fs triggered the error: %s", full_file_path, e)
        return None


def is_git_repo(path: str) -> bool:
    """Check if a directory contains a Git repository.

    Args:
        path (str): The path to the directory to be checked.

    Returns:
        bool: True if the directory contains a Git repository, False otherwise.
    """
    try:
        _ = git.Repo(path).git_dir
        return True
    except git.exc.InvalidGitRepositoryError:
        return False
    except git.NoSuchPathError:
        return False


def clone_git_repo(
    path: str, repo_name: str = "impresso/impresso-data-release", branch: str = "master"
) -> git.Repo:
    """Clone a git repository into a given path in the local file-system.

    Args:
        path (str): Path (ideally absolute) to the dir in which to clone the git repo.
        repo_name (str, optional): Full name of the git repository to clone, as it
            appears in its URL. Defaults to "impresso/impresso-data-release".
        branch (str, optional): Specific branch to clone. Defaults to "master".

    Raises:
        e: Cloning the repo failed, both using SSH and HTTPS.

    Returns:
        git.Repo: Object representing the cloned repository if it was cloned.
    """
    # WARNING: path should be absolute path!!!
    repo_ssh_url = f"git@github.com:{repo_name}.git"
    repo_https_url = f"https://github.com/{repo_name}.git"

    repo_path = os.path.join(path, repo_name.split("/")[1])

    # if the repository was already cloned, pull and return it.
    if os.path.exists(repo_path) and is_git_repo(repo_path):
        msg = (
            f"Git repository {repo_name} had already been cloned, "
            f"pulling from branch {branch}."
        )
        logger.info(msg)
        print(msg)
        repo = git.Repo(repo_path)
        # check if the current branch is the correct one & pull latest version
        if branch not in repo.active_branch.name:
            logger.info("Switching branch from %s to %s", repo.active_branch.name, branch)
            print("Switching branch from %s to %s", repo.active_branch.name, branch)
            repo.git.checkout(branch)
        repo.remotes.origin.pull()

        return repo

    # try to clone using ssh, if it fails, retry with https.
    try:
        logger.info("Cloning the %s git repository with ssh.", repo_name)
        return git.Repo.clone_from(repo_ssh_url, repo_path, branch=branch)

    except git.exc.GitCommandError as e:
        err_msg = (
            f"Error while cloning the git repository {repo_name} using ssh, trying "
            f"with https. \n{e}"
        )
        logger.warning(err_msg)
    # Fallback to https
    try:
        logger.info("Cloning the %s git repository with https.", repo_name)
        return git.Repo.clone_from(repo_https_url, repo_path, branch=branch)

    except Exception as e:
        err_msg = (
            f"Error while cloning the git repository {repo_name}, it was not possible "
            f"to clone it with ssh or https. \n{e}"
        )
        logger.critical(err_msg)
        raise e


def write_and_push_to_git(
    file_contents: str,
    git_repo: git.Repo,
    path_in_repo: str,
    filename: str,
    commit_msg: Optional[str] = None,
) -> tuple[bool, str]:
    """Given a serialized dump, write it in local git repo, commit and push.

    Args:
        file_contents (str): Serialized dump of a JSON file.
        git_repo (git.Repo): Object representing the git repository to push to.
        path_in_repo (str): Relative path where to write the file.
        filename (str): Desired name for the file, including extension.
        commit_msg (Optional[str], optional): Commit message. If not defined, a
            basic message on the added manifest will be used.Defaults to None.

    Returns:
        tuple[bool, str]: Whether the process was successful and corresponding filepath.
    """
    # given the serialized dump or a json file, write it in local git repo
    # folder and push it to the given subpath on git
    local_repo_base_dir = git_repo.working_tree_dir

    git_path = os.path.join(local_repo_base_dir, path_in_repo)
    # write file in git repo cloned locally
    full_git_path = write_dump_to_fs(file_contents, git_path, filename)

    if full_git_path is not None:
        return git_commit_push(full_git_path, git_repo, commit_msg), full_git_path
    else:
        return False, os.path.join(git_path, filename)


def git_commit_push(
    full_git_filepath: str, git_repo: git.Repo, commit_msg: Optional[str] = None
) -> bool:
    """Commit and push the addition of a given file within the repository.

    TODO: make more general for non-manifest related uses?

    Args:
        full_git_filepath (str): Path to the file added to the git repository.
        git_repo (git.Repo): git.Repo object of the repository to commit and push to.
        commit_msg (Optional[str], optional): Message to use when commiting. If not
            defined, a basic message on the added manifest will be used. Defaults to None.

    Returns:
        bool: Whether the commit and push operations were successful.
    """
    # add, commit and push the file at the given path.
    filename = os.path.basename(full_git_filepath)
    # git add file
    git_repo.index.add([full_git_filepath])
    try:
        # git commit and push
        if commit_msg is None:
            commit_msg = f"Add generated manifest file {filename}."
        git_repo.index.commit(commit_msg)
        origin = git_repo.remote(name="origin")

        push_msg = f"Pushing {filename} with commit message '{commit_msg}'"
        logger.info(push_msg)
        origin.push()

        return True
    except git.exc.GitError as e:
        err_msg = f"Error while pushing {filename} to its remote repository. \n{e}"
        logger.error(err_msg)
        return False


def get_head_commit_url(repo: str | git.Repo) -> str:
    """Get the URL of the last commit on a given Git repository.

    TODO: test the function when repo is https url of repository.
    TODO: provide branch argument.

    `repo` can be one of three things:
        - a git.Repo instantiated object (if alreaday instantiated outside).
        - the local path to the git repository (previously cloned).
        - the HTTPS URL to the Git repository.

    Note:
        The returned commit URL corresponds to the one on the repository's active
        branch (master for the URL).

    Args:
        repo (str | git.Repo): local path, git.Repo object or URL of the repository.

    Returns:
        str: The HTTPS URL of the last commit on the git repository's master branch.
    """
    # ensure they are defined (will always be re-assigned later)
    raw_url = ""
    commit_hash = ""
    not_repo_url = True
    if isinstance(repo, str):
        if "https" in repo:
            # if it's the https link to the repo, get the hash of last commit
            not_repo_url = False
            commit_hash = git.cmd.Git().ls_remote(repo, heads=True).split()[0]
            raw_url = repo
        else:
            # get the actual repo if it's only the path.
            repo = git.Repo(repo)

    if not_repo_url:
        # final url of shape 'https://github.com/[orga_name]/[repo_name]/commit/[hash]'
        # warning --> commit on the repo's current branch!
        commit_hash = str(repo.head.commit)
        # url of shape 'git@github.com:[orga_name]/[repo_name].git'
        # or of shape 'https://github.com/[orga_name]/[repo_name].git'
        raw_url = repo.remotes.origin.url

    if raw_url.startswith("https://"):
        if ".git" in raw_url:
            url_end = "/".join(["", "commit", commit_hash])
            return raw_url.replace(".git", url_end)
        return "/".join([raw_url, "commit", commit_hash])

    # now list with contents ['git', 'github', 'com', [orga_name]/[repo_name], 'git']
    url_pieces = re.split(r"[@.:]", raw_url)
    # replace the start and end of the list
    url_pieces[0] = "https:/"
    url_pieces[-1] = "commit"
    # merge back the domain name and remove excedentary element
    url_pieces[1] = ".".join(url_pieces[1:3])
    del url_pieces[2]
    # add the commit hash at the end
    url_pieces.append(commit_hash)

    return "/".join(url_pieces)
