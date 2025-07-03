"""Command-line script to generate a manifest for an S3 bucket or partition after a processing.

Usage:
    s3_add_provider.py --s3-partition-path=<pp> --log-file=<lf> [--dest-bucket=<db> --remove-src-keys --no-copy --verbose]

Options:

--s3-partition-path=<pp>  S3 path to the partition to which the provider level should be added. 
    Corresponds to the last partition before the list of media titles - where to add the provider.
    Eg. ""s3://122-rebuilt-final" or "s3://142-processed-data-final/langident/langident_v1-4-4"
--log-file=<lf>  Path to log file to use.
--dest-bucket=<db>  Destination bucket in which to copy the data with the provider layer. If not 
    defined, will default to the input bucket (corresponding to simply adding the provider).
    Eg. "122-rebuilt-staging".
--remove-src-keys   Whether to remove the source keys which don't have the provider after performing the
    addition and/or copy. If True, the old keys without the provider will be removed. Defaults to False.
--no-copy   Launch the scrip in debug mode - will not perform copies but list which would have been done
--verbose   Set logging level to DEBUG (by default is INFO).
"""

import os
import boto3
from docopt import docopt
import logging
from impresso_essentials.io.s3 import (
    IMPRESSO_STORAGEOPT,
    get_s3_resource,
    get_s3_client
)
from impresso_essentials.utils import (
    get_provider_for_alias, 
    ALL_MEDIA, 
    PARTNER_TO_MEDIA, 
    disable_interrupts,
    init_logger
)
from urllib.parse import urlparse


logger = logging.getLogger(__name__)


def get_alias_from_path(source_path, og_partition):
    if not og_partition.endswith('/') and og_partition!= '':
        og_partition = og_partition + "/"

    split = source_path.replace(og_partition, "").split('/') if og_partition else source_path.split('/')

    # make sure to know if the partner is already in this path or not (especially NZZ)
    if split[0] in ALL_MEDIA and split[1] not in ALL_MEDIA:
        msg = f"returnning split[0], None: {split[0], None}"
        logger.debug(msg)
        return split[0], None
    elif split[0] in PARTNER_TO_MEDIA and split[1] in PARTNER_TO_MEDIA[split[0]]:
        # also return the provider if it's there
        msg = f"returnning split[1], split[0]: {split[1], split[0]}"
        logger.debug(msg)
        return split[1], split[0]
    else:
        msg = f"The source path {source_path} does not contain the media alias at an expected place."
        raise AttributeError(msg)
    

def construct_dest_key(src_key, provider, og_partition, current_alias=None, found_prov=None):

    if found_prov:
        msg = f"construct_dest_key - found_provider is not None ({found_prov}), skipping, and returning key as-is: {src_key}"
        logger.debug(msg)
        return src_key
    
    msg = f"og_partition : {og_partition}, current_alias={current_alias}"
    logger.debug(msg)

    if og_partition != '':
        # remove the tailing '/' if it exists
        if og_partition.endswith('/'):
            og_partition = og_partition[:-1]
        # add the provider right after the en of the partition
        return src_key.replace(og_partition, os.path.join(og_partition, provider))
    
    # if there is no specific partition, the provider becomes the first element of the key
    return os.path.join(provider, src_key)


def add_provider_to_s3_partition(src_bucket: str, dest_bucket: str, exact_partition: str, perform_copy=False, remove_src_keys=False, metadata_directive="COPY"):

    s3 = get_s3_client()

    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=src_bucket, Prefix=exact_partition)

    current_alias = None
    provider = None
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key[:-1]==exact_partition:
                print(f"(partition: {key})")
            elif not key.endswith(".jsonl.bz2") and key.endswith("/"):
                current_alias = key.split('/')[-2]
                provider = get_provider_for_alias(current_alias)
                msg = f"Now processing alias {current_alias} from {provider}"
                print(msg)
                logger.info(msg)
            elif key.endswith(".jsonl.bz2"):

                # check if we have now changed Alias or provider
                new_alias, found_prov = get_alias_from_path(key, exact_partition)

                if current_alias!=new_alias:
                    current_alias=new_alias
                    provider = get_provider_for_alias(current_alias)
                    msg = (
                        f"Found new alias in key - Now processing {current_alias} from {provider}"
                    )
                    print(msg)
                    logger.info(msg)

                dest_key = construct_dest_key(key, provider, exact_partition, current_alias, found_prov)
                if perform_copy:
                    if dest_key!=key:
                        try:
                            existing_dest = s3.head_object(Bucket=dest_bucket, Key=dest_key)
                            msg = (
                                f"    The destination key {dest_key} already exists in bucket "
                                f"{dest_bucket}, skipping."
                            )
                            print(msg)
                            logger.info(msg)
                        except Exception:
                            # the destination does not exist yet, perform the copy
                            msg = f"    File key {key} will be copied to {dest_key} in bucket {dest_bucket}"
                            logger.debug(msg)
                            with disable_interrupts():
                                s3.copy_object(
                                    Bucket=dest_bucket,
                                    Key=dest_key,
                                    CopySource={"Bucket": src_bucket, "Key": key},
                                    MetadataDirective=metadata_directive,
                                )
                                
                        if remove_src_keys:
                            if "pages" not in key:
                                msg = f"    File key {key} will be deleted from bucket {dest_bucket} - new location: {dest_key}"
                                logger.info(msg)
                                print(msg)
                            s3.delete_object(
                                Bucket=src_bucket,
                                Key=key,
                                BypassGovernanceRetention=False,
                            )
                    else:
                        msg = f"    will NOT copy {key} to {dest_key} - same key."
                        logger.debug(msg)
                else:
                    msg = f"Debug Mode - would have copied {key} to {dest_key}."
                    logger.info(msg)
                    print(msg)
            else:
                print(f"    -> another file: {key}")


def main():
    arguments = docopt(__doc__)
    full_s3_partition = arguments["--s3-partition-path"]
    log_file = arguments["--log-file"]
    dest_bucket = arguments["--dest-bucket"] if arguments["--dest-bucket"] else None
    remove_src_keys = arguments["--remove-src-keys"]
    no_copy = arguments["--no-copy"]
    log_level = logging.DEBUG if arguments["--verbose"] else logging.INFO

    init_logger(logger, log_level, log_file)

    # suppressing botocore's verbose logging
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("smart_open").setLevel(logging.WARNING)

    parsed_input = urlparse(full_s3_partition)
    src_bucket = parsed_input.netloc
    exact_partition = parsed_input.path.lstrip("/")
    # if destination bucket is not provided, set it to the source
    dest_bucket = dest_bucket if dest_bucket else src_bucket
    
    msg = (
        f"Will add the provider level to the contents of {full_s3_partition},"
        f"{' not' if no_copy else ''} copying them in bucket {dest_bucket}, "
        f"and {'' if remove_src_keys else 'not '}deleting the source keys after."
    )
    print(msg)
    logger.info(msg)

    add_provider_to_s3_partition(
        src_bucket,
        dest_bucket,
        exact_partition,
        perform_copy=not no_copy,
        remove_src_keys=remove_src_keys
    )


if __name__ == "__main__":
    main()