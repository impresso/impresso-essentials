"""Command-line script to generate a manifest for an S3 bucket or partition after a processing.

Usage:
    compute_manifest.py --config-file=<cf> --log-file=<lf> [--scheduler=<sch> --nworkers=<nw> --verbose]

Options:

--config-file=<cf>  Path to config file containing all arguments for manifest computation.
--log-file=<lf>  Path to log file to use.
--scheduler=<sch>  Tell dask to use an existing scheduler (otherwise it'll create one)
--nworkers=<nw>  number of workers for (local) Dask client.
--verbose  Set logging level to DEBUG (by default is INFO).
"""

import json
import os
import traceback
import logging
from typing import Any, Optional
import git
from docopt import docopt
from tqdm import tqdm

import dask.bag as db
from dask.distributed import Client
from impresso_essentials.io.s3 import (
    fixed_s3fs_glob,
    IMPRESSO_STORAGEOPT,
    extract_provider_alias_key,
    provider_in_path,
)
from impresso_essentials.utils import (
    init_logger,
    ALL_MEDIA,
    PARTNER_TO_MEDIA,
    get_provider_for_alias,
    get_src_info_for_alias,
    validate_stage,
    DataStage,
)
from impresso_essentials.versioning import aggregators
from impresso_essentials.versioning.data_manifest import DataManifest

logger = logging.getLogger(__name__)

# list of optional configurations
OPT_CONFIG_KEYS = [
    "input_bucket",
    "providers",
    "media_aliases",
    "alias_blacklist",
    "previous_mft_s3_path",
    "is_patch",
    "patched_fields",
    "only_counting",
    "push_to_git",
    "notes",
    "relative_git_path",
    "compute_altogether",
    "model_id",
    "run_id",
    "check_s3_archives",
]
# list of requirec configurations
REQ_CONFIG_KEYS = [
    "data_stage",
    "output_bucket",
    "git_repository",
    "file_extensions",
]


def remove_corrupted_files(
    s3_files: dict[str, dict[str, list[str]]],
) -> dict[str, dict[str, list[str]]]:
    """Check if any of the files to consider found on S3 are corrupted or empty.

    If the files are corrupted or empty, they can cause errors in the later steps of
    the manifest computation. Hence this step allows to prevent this.
    This step is optional and creates some overhead for the manifest computation.
    This method also logs any file that has been found to create errors when being
    read by dask.

    Args:
        s3_files (dict[str, dict[str, list[str]]]): S3 archive files to consider that where found.

    Returns:
        dict[str, dict[str, list[str]]]: All non-corrupted/empty archives to use for the manifest.
    """
    msg = "Starting to check all considered s3 archives to ensure they are not corrupted..."
    logger.info(msg)
    print(msg)
    correct_files = {}
    corrupted_files = []

    for prov, prov_s3_files in s3_files.items():
        for idx, (alias, files_alias) in enumerate(prov_s3_files.items()):
            msg = f"Checking for corrupted S3 archives for {alias} ({idx+1}/{len(prov_s3_files)}): {len(files_alias)} archives"
            logger.info(msg)
            print(msg)
            try:
                # try to read the file and only take the first one
                contents = (
                    db.read_text(files_alias, storage_options=IMPRESSO_STORAGEOPT)
                    .map(lambda x: (len(json.loads(x)), json.loads(x).keys()))
                    .compute()
                )

                # add any non-corrupted files to the list of files to consider
                if prov in correct_files:
                    correct_files[prov][alias] = files_alias
                else:
                    correct_files[prov] = {alias: files_alias}
                del contents
            except Exception as e:
                msg = (
                    f"{alias}, an exception occurred trying to read some archives, "
                    f"checking one by 1 for {len(files_alias)} archives. \nException: {e}"
                )
                logger.info(msg)
                print(msg)
                msg = f"List of archives to check one by one: {files_alias}"
                logger.debug(msg)
                for file in tqdm(files_alias, total=len(files_alias)):
                    try:
                        corr_contents = (
                            db.read_text(file, storage_options=IMPRESSO_STORAGEOPT)
                            .map(lambda x: len(json.loads(x)))
                            .compute()
                        )

                        # add any non-corrupted files to the list of files to consider
                        if prov in correct_files:
                            if alias in correct_files:
                                correct_files[prov][alias].append(file)
                            else:
                                correct_files[prov][alias] = [file]
                        else:
                            correct_files[prov] = {alias: [file]}
                        del corr_contents
                    except Exception as e2:
                        msg = (
                            f"{file}, an exception occurred trying to read it, "
                            f"it is probably corrupted. {e2}"
                        )
                        logger.info(msg)
                        print(msg)
                        corrupted_files.append(file)

    total_num_files = sum(len(v) for prov_v in s3_files.values() for v in prov_v.values())
    num_ok_files = sum(len(v) for prov_v in correct_files.values() for v in prov_v.values())
    msg = (
        f"Found {total_num_files} files on S3, {len(corrupted_files)} were corrupted. As a "
        f"result, the remaining {num_ok_files} will be considered for the manifest computation."
    )
    logger.info(msg)
    print(msg)
    # if there are any empty archives, print which ones
    if len(corrupted_files) != 0:
        msg = f"Corrupted archives: {corrupted_files}."
        logger.info(msg)
        print(msg)

    return correct_files


def get_files_to_consider(config: dict[str, Any]) -> Optional[dict[str, dict[str, list[str]]]]:
    """Get the list of S3 files to consider based on the provided configuration.

    The s3_files mapping is now provider -> alias -> list of files.

    Args:
        config (dict[str, Any]): Configuration parameters with the s3 bucket, titles,
            and file extensions

    Returns:
        dict[str, dict[str, list[str]]] | None: Dict mapping each provider to a dict mapping each
            alias to the s3 files to consider, or None if no files found.

    Raises:
        ValueError: If `file_extensions` in the config is empty or None.
    """
    if config["file_extensions"] == "" or config["file_extensions"] is None:
        raise ValueError("Config file's `file_extensions` should not be empty or None.")

    ext = config["file_extensions"]
    if config["data_stage"] == "canonical":
        # for canonical data, we only select issues, override the value provided
        ext = "issues.jsonl.bz2"
    # change "." in ext with `ext.startswith('.')`?
    extension_filter = f"*{ext}" if "." in ext else f"*.{ext}"
    # check if bucket has provider level while some buckets have providers and others not
    incl_provider = provider_in_path(config["output_bucket"])

    if config["prov_alias_pairs"] is None:
        # if media_aliases is empty, include all media_aliases
        logger.info("Fetching the files to consider for all titles...")
        print("Fetching the files to consider for all titles...")
        # return all filenames in the given bucket partition with the correct extension
        files = fixed_s3fs_glob(os.path.join(config["output_bucket"], extension_filter))
        # Ensure blacklist is defined and a list in the case there is any alias to exclude
        blacklist = blacklist = (
            config["alias_blacklist"] if config["alias_blacklist"] is not None else []
        )
        s3_files = {}
        for s3_key in files:
            provider, alias = extract_provider_alias_key(
                s3_key, config["output_bucket"], prov_included=incl_provider
            )
            if alias not in blacklist:
                # add the provider as a first level key
                if provider in s3_files:
                    if alias in s3_files[provider]:
                        s3_files[provider][alias].append(s3_key)
                    else:
                        s3_files[provider][alias] = [s3_key]
                else:
                    s3_files[provider] = {alias: [s3_key]}
            else:
                msg = (
                    f"Skipping the processing of alias {alias} since it's part of the blacklist..."
                )
                print(msg)
                logger.info(msg)

    else:
        # here only add files for aliases in media_aliases instead
        msg = f"Fetching the files to consider for provider-alias pairs {config['prov_alias_pairs']}..."
        logger.info(msg)
        print(msg)
        s3_files = {}
        for provider, alias in config["prov_alias_pairs"]:
            # Temporary fix until all buckets have the provider level
            if incl_provider:
                s3_path = fixed_s3fs_glob(
                    os.path.join(config["output_bucket"], provider, alias, extension_filter)
                )
            else:
                s3_path = fixed_s3fs_glob(
                    os.path.join(config["output_bucket"], alias, extension_filter)
                )
            if provider in s3_files:
                s3_files[provider][alias] = s3_path
            else:
                s3_files[provider] = {alias: s3_path}

    if config["check_s3_archives"]:
        # filter out empty or corrupted files
        correct_s3_files = remove_corrupted_files(s3_files)
        return correct_s3_files

    msg = (
        "Not checking for any corrupted S3 archives before launching the manifest computation. "
        "If you encounter any problems regarding the reading of the archives, "
        "please set `check_s3_archives` to True."
    )
    logger.info(msg)
    print(msg)
    return s3_files


def compute_stats_for_stage(
    files_bag: db.core.Bag,
    stage: DataStage,
    client: Client | None = None,
    title: str | None = None,
    src_medium: str | None = None,
) -> list[dict] | None:
    """Compute statistics for a specific data stage.

    Args:
        files_bag (db.core.Bag): A bag containing files for statistics computation.
        stage (DataStage): The data stage for which statistics are computed.
        client (Client | None, optional): Dask client to use.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.
        src_medium (str, optional): Source medium of the title to process, used for canonical data.

    Returns:
        list[dict] | None]: List of computed yearly statistics, or None if statistics
            computation for the given stage is not implemented.
    """
    match stage:
        case DataStage.CANONICAL:
            return aggregators.compute_stats_in_canonical_bag(
                files_bag, client=client, title=title, src_medium=src_medium
            )
        case DataStage.REBUILT:
            return aggregators.compute_stats_in_rebuilt_bag(
                files_bag, include_alias=True, client=client, title=title
            )
        case DataStage.ENTITIES:
            return aggregators.compute_stats_in_entities_bag(files_bag, client=client, title=title)
        case DataStage.NEWS_AGENCIES:
            return aggregators.compute_stats_in_entities_bag(files_bag, client=client, title=title)
        case DataStage.PASSIM:
            return aggregators.compute_stats_in_rebuilt_bag(
                files_bag,
                include_alias=True,
                passim=True,
                client=client,
                title=title,
            )
        case DataStage.LANGIDENT:
            return aggregators.compute_stats_in_langident_bag(files_bag, client=client, title=title)
        case DataStage.TEXT_REUSE:
            return aggregators.compute_stats_in_text_reuse_passage_bag(
                files_bag, client=client, title=title
            )
        case DataStage.TOPICS:
            return aggregators.compute_stats_in_topics_bag(files_bag, client=client, title=title)
        case DataStage.EMB_IMAGES:
            return aggregators.compute_stats_in_img_emb_bag(files_bag, client=client, title=title)
        case DataStage.EMB_DOCS:
            return aggregators.compute_stats_in_doc_emb_bag(files_bag, client=client, title=title)
        case DataStage.LINGPROC:
            return aggregators.compute_stats_in_lingproc_bag(files_bag, client=client, title=title)
        case DataStage.SOLR_TEXT:
            return aggregators.compute_stats_in_solr_text_ing_bag(
                files_bag, client=client, title=title
            )
        case DataStage.OCRQA:
            return aggregators.compute_stats_in_ocrqa_bag(files_bag, client=client, title=title)
    raise NotImplementedError(
        "The function computing statistics for this DataStage is not yet implemented."
    )


def aliases_to_process(config: dict[str, Any]) -> list[tuple[str, str]] | None:
    """Generate a list of (provider, alias) pairs to consider based on the config.

    Uses the input configuration to determine which provider-alias pairs should be
    included when building the manifest. Blacklisted aliases in `alias_blacklist` are
    excluded from the result.

    If neither `providers` nor `media_aliases` are specified, the function returns `None`,
    indicating that all data should be processed. However, any aliases in the blacklist will
    still be excluded from the processing in this case.

    Args:
        config (dict[str, Any]): A configuration dictionary expected to contain:
            - "providers" (list[str] | None): Optional list of provider names.
            - "media_aliases" (list[str] | None): Optional list of specific media aliases.
            - "alias_blacklist" (list[str] | None): Optional list of aliases to exclude.

    Returns:
        list[tuple[str, str]] | None: A list of (provider, alias) tuples to process,
        or None if all data should be considered (no filtering).

    Example:
        >>> config = {
            ...
            "providers": ["BNF"],
            "media_aliases": ["NZZ"],
            "alias_blacklist": ["JGD", "marieclaire", "lepetitparisien", "legaulois", "lematin"]
        }
        >>> aliases_to_process(config)
        [('BNF', 'excelsior'), ('BNF', 'lafronde'), ('BNF', 'oeuvre'), ('BNF', 'jdpl'),
         ('BNF', 'lepji'), ('BNF', 'oecaen'), ('BNF', 'oerennes'), ('NZZ', 'NZZ')]
    """
    aliases = []

    blacklist = config["alias_blacklist"] if config["alias_blacklist"] is not None else []
    providers = config["providers"] if config["providers"] is not None else []
    media_aliases = config["media_aliases"] if config["media_aliases"] is not None else []

    # if both lista are empty, there is no list to return
    if not providers and not media_aliases:
        return None

    # first collect all the aliases from the list of providers
    aliases = [(p, a) for p in providers for a in PARTNER_TO_MEDIA[p] if a not in blacklist]
    # then add the aliases from the list of aliases
    aliases.extend([(get_provider_for_alias(a), a) for a in media_aliases if a not in blacklist])

    return aliases


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    """Ensure all required configurations are defined, add any missing optional ones.

    Args:
        config (dict[str, Any]): Provided configuration dict to compute the manifest.

    Raises:
        ValueError: Some required arguments of the configuration are missing.

    Returns:
        dict[str, Any]: Updated config, with any mssing optional argument set to None.
    """
    logger.info("Validating that the provided configuration has all required arugments.")
    if not all(k in config for k in REQ_CONFIG_KEYS):
        raise ValueError(f"Missing some required configurations: {REQ_CONFIG_KEYS}")

    for k in OPT_CONFIG_KEYS:
        if k not in config:
            logger.debug("%s was missing form the configuration, setting it to None", k)
            config[k] = None

    config["prov_alias_pairs"] = aliases_to_process(config)

    return config


def add_stats_to_mft(
    manifest: DataManifest,
    media_alias: str,
    computed_stats: list[dict],
    src_medium: str | None = None,
    provider: str | None = None,
) -> DataManifest:
    """Add the statistics computed for a given media alias to an instantiated manifest.

    Performs validation to ensure that statistics are being added for the correct media title.

    Args:
        manifest (DataManifest): The manifest object to which the statistics will be added.
        media_alias (str): The alias representing the media title these stats belong to.
        computed_stats (list[dict]): A list of dictionaries, each containing computed
            statistics corresponding to a given stage along with `media_alias` and `year` keys.
        src_medium (str | None, optional): The source medium of the alias. Defaults to None.
        provider (str | None, optional): The data provider identifier. Defaults to None.

    Returns:
        DataManifest: The updated DataManifest instance with the added statistics.
    """
    logger.info(
        "%s (%s) - Populating the manifest with the resulting %s yearly statistics found...",
        media_alias,
        provider,
        len(computed_stats),
    )
    logger.debug("%s (%s) - computed_stats: %s", media_alias, provider, computed_stats)

    for stats in computed_stats:
        title = stats["media_alias"]
        if title != media_alias and media_alias in ALL_MEDIA:
            # ensure the correct stats are being added.
            msg = (
                "Warning, some stats were computed on the wrong title! Not adding them."
                f"provider={provider}, alias={media_alias}, title={title}, year={stats['year']}."
            )
            print(msg)
            logger.info(msg)
        else:
            # added here meanwhile we add the provider in process_by_title
            year = stats["year"]
            del stats["media_alias"]
            del stats["year"]
            logger.debug("Adding %s to %s-%s (%s)", stats, title, year, provider)
            manifest.add_by_title_year(title, year, stats, src_medium=src_medium, provider=provider)

    logger.info("%s - Finished adding stats, going to the next title...", media_alias)

    return manifest


def process_by_title(
    manifest: DataManifest,
    s3_files: dict[str, list[str]],
    stage: DataStage,
    client: Client | None,
) -> DataManifest:
    """Process compute statistics for stage by media title and add them to the manifest.

    Invalid or mismatched aliases are logged and ignored.

    Args:
        manifest (DataManifest): The manifest object to be populated with computed statistics.
        s3_files (dict[str, list[str]]): A nested dictionary mapping each provider to a dictionary
            of media aliases and their corresponding lists of S3 file paths.
        stage (DataStage): The processing stage that determines how statistics should be computed.
        client (Client | None): Optional Dask client used to parallelize computation, if available.

    Returns:
        DataManifest: The updated DataManifest instance with added statistics for each valid alias.
    """
    print("\n-> Starting computing the manifest by title <-")
    logger.info("\n-> Starting computing the manifest by title <-")

    for provider, provider_alias_files in s3_files.items():
        for alias, s3_files_for_alias in provider_alias_files.items():

            if alias in PARTNER_TO_MEDIA[provider]:
                logger.info("---------- %s (%s) ----------", alias, provider)
                msg = f"The list of files selected for {alias} is: {s3_files_for_alias}"
                logger.info(msg)
                # load the selected files in dask bags
                processed_files = db.read_text(
                    s3_files_for_alias, storage_options=IMPRESSO_STORAGEOPT
                ).map(json.loads)

                msg = f"{alias} - Starting to compute the statistics on the fetched files..."
                logger.info(msg)
                print(msg)
                src_medium = get_src_info_for_alias(alias, provider)
                computed_stats = compute_stats_for_stage(
                    processed_files,
                    stage,
                    client,
                    title=alias,
                    src_medium=src_medium,
                )

                manifest = add_stats_to_mft(manifest, alias, computed_stats, src_medium, provider)
            elif alias in ALL_MEDIA:
                msg = (
                    f"Found S3 files for {alias} which is in ALL_MEDIA but not of "
                    f"the provider {provider} - error to be checked, it will be ignored."
                )
                logger.info(msg)
                print(msg)
            else:
                msg = (
                    f"Found S3 files for {alias} which is not a media title of the "
                    f"provider {provider}, it will be ignored.",
                )
                logger.info(msg)
                print(msg)

    return manifest


def process_altogether(
    manifest: DataManifest,
    s3_files: dict[str, list[str]],
    stage: DataStage,
    client: Client | None,
) -> DataManifest:
    """Process all fetched S3 files at once, filtering them by title to populate the manifest.

    The equivalent of `process_by_title` but when working with large unified datasets.

    Args:
        manifest (DataManifest): The manifest object to be populated with computed statistics.
        s3_files (dict[str, list[str]]): A dictionary mapping each provider to a list of S3 file paths.
        stage (DataStage): The stage of data processing, which determines how statistics should be computed.
        client (Client | None): Optional Dask client to parallelize computation when available.

    Returns:
        DataManifest: The updated manifest instance containing statistics for each processed media alias.
    """
    msg = "\n-> Starting to compute the manifest altogether, filterting iteratively by title <-"
    logger.info(msg)
    print(msg)

    s3_fpaths = [j for part_j in s3_files.values() for j in part_j]
    logger.debug("The list of files selected is: %s", s3_fpaths)
    # load the selected files in dask bags
    processed_files = (
        db.read_text(s3_fpaths, storage_options=IMPRESSO_STORAGEOPT).map(json.loads).persist()
    )  # .map(lambda x: (x['ci_id'].split('-')[0], x)).persist()

    total_num = len(ALL_MEDIA)
    cum_idx = 0
    for provider, prov_aliases in PARTNER_TO_MEDIA.items():
        num_aliases = len(prov_aliases)
        msg = f"{'-'*10} PROCESSING MEDIA TITLES FROM {provider} ({num_aliases} titles) {'-'*10}"
        logger.info(msg)

        for idx, alias in enumerate(prov_aliases):

            msg = (
                f"{'-'*10} {alias} - {cum_idx + idx + 1}/{total_num} - "
                f"(for {provider} {idx+1}/{num_aliases}) {'-'*10}"
            )
            logger.info(msg)

            # filter to only keep the tr_passages for this title
            filtered = processed_files.filter(
                lambda x: x["ci_id"].startswith(f"{alias}-")  # in x["ci_id"]
            ).persist()
            logger.info("%s - Computing the statistics on the filtered files...", alias)

            src_medium = get_src_info_for_alias(alias, provider)
            computed_stats = compute_stats_for_stage(
                filtered, stage, client, title=alias, src_medium=src_medium
            )

            manifest = add_stats_to_mft(manifest, alias, computed_stats, src_medium, provider)
        cum_idx += num_aliases

    return manifest


def create_manifest(config_dict: dict[str, Any], client: Optional[Client] = None) -> None:
    """Given its configuration, generate the manifest for a given s3 bucket partition.

    TODO: separate further into functions

    Note:
        The contents of the configuration file (or dict) are given in markdown file
        `impresso_commons/data/manifest_config/manifest.config.example.md``

    Args:
        config_dict (dict[str, Any]): Configuration following the guidelines.
        client (Client | None, optional): Dask client to use.
    """
    # if the logger was not previously inialized, do it
    if not logger.hasHandlers():
        init_logger(logger)

    # ensure basic validity of the provided configuration
    config_dict = validate_config(config_dict)
    stage = validate_stage(config_dict["data_stage"])
    logger.info("Provided config validated.")

    logger.info("Starting to generate the manifest for DataStage: '%s'", stage)

    # fetch the names of the files to consider separated per title
    s3_files = get_files_to_consider(config_dict)

    num_files = sum(len(aliases.values()) for prov in s3_files.values() for aliases in prov)
    num_aliases = sum(len(prov.values()) for prov in s3_files.values())
    logger.info(
        "Collected a total of %s files from %s aliases (and %s providers), reading them...",
        num_files,
        num_aliases,
        len(s3_files.values()),
    )

    logger.info("Files identified successfully, initialising the manifest.")
    # init the git repo object for the processing's repository.
    repo = git.Repo(config_dict["git_repository"])

    # ideally any not-defined param should be None.
    in_bucket = config_dict["input_bucket"]
    p_fields = config_dict["patched_fields"]
    if p_fields is not None and len(p_fields) == 0:
        p_fields = None
    prev_mft = config_dict["previous_mft_s3_path"]
    only_counting = config_dict["only_counting"]
    relative_git_path = config_dict["relative_git_path"]

    # init the manifest given the configuration
    manifest = DataManifest(
        data_stage=stage,
        s3_output_bucket=config_dict["output_bucket"],
        s3_input_bucket=in_bucket if in_bucket != "" else None,
        git_repo=repo,
        temp_dir=config_dict["temp_directory"],
        is_patch=config_dict["is_patch"],
        patched_fields=p_fields,
        previous_mft_path=prev_mft if prev_mft != "" else None,
        only_counting=only_counting,
        relative_git_path=relative_git_path if relative_git_path != "" else None,
        model_id=config_dict["model_id"],
        run_id=config_dict["run_id"],
    )

    # `compute_altogether` can be None (counts as False)
    if config_dict["compute_altogether"]:
        # when the output data is not organized by title,
        # the manifest needs to be computed on all the data at once
        manifest = process_altogether(manifest, s3_files, stage, client)
    else:
        # processing media_aliases one at a time
        manifest = process_by_title(manifest, s3_files, stage, client)

    logger.info("Finalizing the manifest, and computing the result...")
    # Add the note to the manifest
    if config_dict["notes"] is not None and config_dict["notes"] != "":
        manifest.append_to_notes(config_dict["notes"])
    else:
        note = f"Processing data to generate {stage} for "
        if config_dict["prov_alias_pairs"]:
            note += f"titles: {config_dict['prov_alias_pairs']}."
        else:
            blacklist_aliases = (
                f" except {config_dict['alias_blacklist']}"
                if config_dict["alias_blacklist"] is not None
                else None
            )
            note += f"all media titles{blacklist_aliases}."

        manifest.append_to_notes(note)

    if config_dict["push_to_git"]:
        manifest.compute(export_to_git_and_s3=True)
    else:
        manifest.compute(export_to_git_and_s3=False)
        manifest.validate_and_export_manifest(push_to_git=False)


def main():
    arguments = docopt(__doc__)
    config_file_path = arguments["--config-file"]
    log_file = arguments["--log-file"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO
    nworkers = int(arguments["--nworkers"]) if arguments["--nworkers"] else 8
    scheduler = arguments["--scheduler"]

    init_logger(logger, log_level, log_file)

    # suppressing botocore's verbose logging
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("smart_open").setLevel(logging.WARNING)

    # start the dask local cluster
    if scheduler is None:
        client = Client(n_workers=nworkers, threads_per_worker=1)
    else:
        client = Client(scheduler)

    dask_cluster_msg = f"Dask local cluster: {client}"
    logger.info(dask_cluster_msg)
    print(dask_cluster_msg)

    logger.info("Reading the arguments inside %s", config_file_path)
    with open(config_file_path, "r", encoding="utf-8") as f_in:
        config_dict = json.load(f_in)

    try:
        logger.info("Provided configuration: ")
        logger.info(config_dict)
        create_manifest(config_dict, client)

    except Exception as e:
        traceback.print_tb(e.__traceback__)
        print(e)
        client.shutdown()


if __name__ == "__main__":
    main()
