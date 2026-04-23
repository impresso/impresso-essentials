"""Helper functions to used to compute and aggragate the statistics of manifests."""

import logging
from ast import literal_eval
from collections import Counter
from typing import Any
import numpy as np
import pandas as pd
from dask import dataframe as dd
from dask.dataframe import Aggregation
from dask.bag.core import Bag
from dask.distributed import progress, Client
from itertools import chain

logger = logging.getLogger(__name__)


def log_src_medium_mismatch(
    obj_id: str, stage: str, prov_src_medium: str, found_src_medium: str
) -> None:
    """Log that the source medium found in the data to agg doesn't match the one previously set.

    Args:
        obj_id (str): Impresso ID of the object for which the mismatch was observed.
        stage (str): Data Stage of the data which was being aggregated.
        prov_src_medium (str): Previously given source medium.
        found_src_medium (str): Source medium found in the data to be aggregated.

    Raises:
        AttributeError: There was a mismatch in the expected and found source mediums.
    """
    msg = (
        f"{obj_id} - {stage} stage - Warning, mismatch between provided "
        f"src_medium={prov_src_medium} and found src_medium={found_src_medium}!!"
    )
    logger.error(msg)
    print(msg)
    raise AttributeError(msg)


def counts_for_canonical_issue(
    issue: dict[str, Any],
    incl_alias_yr: bool = False,
    src_medium: str | None = None,
) -> dict[str, int]:
    """Given the canonical representation of an issue, get its counts.

    Args:
        issue (dict[str, Any]): Canonical JSON representation of an issue.
        incl_alias_yr (bool, optional): Whether the newspaper title and year should
            be included in the returned dict for later aggregation. Defaults to False.
        src_medium (str, optional): The source medium of this issue. Defaults to None.

    Returns:
        dict[str, int]: Dict listing the counts for this issue, ready to be aggregated.
    """
    counts = (
        {
            "media_alias": issue["id"].split("-")[0],
            "year": issue["id"].split("-")[1],
        }
        if incl_alias_yr
        else {}
    )

    update_dict = {
        "issues": 1,
        "content_items_out": len(issue["i"]),
    }

    if src_medium and src_medium == "audio":
        if "sm" not in issue or issue["sm"] != src_medium:
            # the source medium should always be defined for radio data
            log_src_medium_mismatch(issue["id"], "canonical", src_medium, issue["sm"])

        # case of audio
        update_dict["audios"] = len(set(issue["rr"]))
        counts.update(update_dict)
    else:
        if "sm" in issue and issue["sm"] != src_medium:
            log_src_medium_mismatch(issue["id"], "canonical", src_medium, issue["sm"])

        # case of paper (print and typescripts)
        update_dict["pages"] = len(set(issue["pp"]))
        update_dict["images"] = len([item for item in issue["i"] if item["m"]["tp"] == "image"])
        counts.update(update_dict)

    return counts


def counts_for_rebuilt(
    rebuilt_ci: dict[str, Any],
    include_alias: bool = False,
    passim: bool = False,
) -> dict[str, int | str]:
    """Define the counts for 1 given rebuilt content-item to match the count keys.

    Args:
        rebuilt_ci (dict[str, Any]): Rebuilt content-item from which to extract counts.
        include_alias (bool, optional): Whether to include the title in resulting dict,
            not necessary for on-the-fly computation. Defaults to False.
        passim (bool, optional): True if rebuilt is in passim format. Defaults to False.

    Returns:
        dict[str, Union[int, str]]: Dict with rebuilt (passim) keys and counts for 1 CI.
    """
    split_id = rebuilt_ci["id"].split("-")
    counts = {"media_alias": split_id[0]} if include_alias else {}
    counts.update(
        {
            "year": split_id[1],
            "issues": "-".join(split_id[:-1]),  # count the issues represented
            "content_items_out": 1,
        }
    )
    if not passim:
        counts.update(
            {
                "ft_tokens": (
                    len(rebuilt_ci["ft"].split()) if "ft" in rebuilt_ci else 0
                ),  # split on spaces to count tokens
            }
        )

    return counts


def compute_stats_in_canonical_bag(
    s3_canonical_issues: Bag,
    client: Client | None = None,
    title: str | None = None,
    src_medium: str | None = None,
) -> list[dict[str, Any]]:
    """Computes number of issues and supports per alias from a Dask bag of canonical data.

    Args:
        s3_canonical_issues (db.core.Bag): Bag with the contents of canonical files to
            compute statistics on.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.
        src_medium (str, optional): The source medium of this issue. Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match canonical DataStatistics keys.
    """

    print(f"{title} - Fetched all issues, gathering desired information.")
    logger.info("%s - Fetched all issues, gathering desired information.", title)

    def _new_canonical_stats(alias: str, year: str) -> dict[str, Any]:
        counts = {
            "media_alias": alias,
            "year": year,
            "issues": 0,
            "content_items_out": 0,
        }
        if src_medium and src_medium == "audio":
            counts["audios"] = 0
        else:
            counts["pages"] = 0
            counts["images"] = 0
        return counts

    def _update_canonical_stats(
        acc: dict[tuple[str, str], dict[str, Any]], issue: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        issue_id = issue["id"]
        alias, year = issue_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_canonical_stats(alias, year))

        entry["issues"] += 1
        entry["content_items_out"] += len(issue["i"])

        if src_medium and src_medium == "audio":
            if "sm" not in issue or issue["sm"] != src_medium:
                log_src_medium_mismatch(issue_id, "canonical", src_medium, issue["sm"])
            entry["audios"] += len(set(issue["rr"]))
        else:
            if "sm" in issue and issue["sm"] != src_medium:
                log_src_medium_mismatch(issue_id, "canonical", src_medium, issue["sm"])
            entry["pages"] += len(set(issue["pp"]))
            entry["images"] += len([item for item in issue["i"] if item["m"]["tp"] == "image"])
        return acc

    def _partition_canonical_stats(records):
        aggregated = {}
        for issue in records:
            _update_canonical_stats(aggregated, issue)
        return aggregated

    def _merge_canonical_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_canonical_stats(values["media_alias"], values["year"])
                )
                entry["issues"] += values["issues"]
                entry["content_items_out"] += values["content_items_out"]
                if src_medium and src_medium == "audio":
                    entry["audios"] += values["audios"]
                else:
                    entry["pages"] += values["pages"]
                    entry["images"] += values["images"]
        return merged

    aggregated = s3_canonical_issues.reduction(
        perpartition=_partition_canonical_stats,
        aggregate=_merge_canonical_stats,
        split_every=8,
    )

    if client is not None:
        progress(aggregated)

    aggregated_result = aggregated.compute()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        aggregated_result.values(),
        key=lambda row: (row["media_alias"], row["year"]),
    )


concat_str = Aggregation(
    name="concat_str",
    chunk=lambda s: s.sum(),  # sum strings within each partition
    agg=lambda s: s.sum(),  # merge partitions
    finalize=lambda s: s.iloc[0] if len(s) > 0 else [],  # single final string
)


def freq(
    x: dict,
    cols: list[str] | None = None,
    for_can_cons: bool = False,
    col: list[str] | None = None,
) -> dict:
    """Compute the frequency dict of the given column or columns

    Args:
        x (dict): Dict corresponding to aggregated values for one title-year,
            which contains lists of values to count.
        cols (list[str], optional): List of keys (columns) with lists of values to count.
            Defaults to ["lang_fd"].

    Returns:
        dict: The statistics for the given title-year, with the value counts of the required columns.
    """
    if cols is None:
        cols = col if col is not None else ["lang_fd"]

    for col in cols:
        if col in x:
            value = x[col]

            # Try to parse as literal, for canonical-consolidated,
            # the data needs slight modifications to have a list format
            if for_can_cons:
                literal = literal_eval("[" + value[:-2] + "]")
            elif isinstance(value, list):
                literal = value
            elif value in (None, ""):
                literal = []
            elif isinstance(value, str):
                literal = literal_eval(value)
            else:
                literal = [value]

            x[col] = dict(Counter(literal))

    return x


def compute_stats_in_can_consolidated_bag(
    s3_can_cons_issues: Bag,
    client: Client | None = None,
    title: str | None = None,
    src_medium: str | None = None,
) -> list[dict[str, Any]]:
    """Computes number of issues and supports per alias from a Dask bag of consolidated canonical data.

    Args:
        s3_can_cons_issues (db.core.Bag): Bag with the contents of consolidated canonical
            files to compute statistics on.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.
        src_medium (str, optional): The source medium of this issue. Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match canonical DataStatistics keys.
    """

    print(f"{title} - Fetched all issues, gathering desired information.")
    logger.info("%s - Fetched all issues, gathering desired information.", title)

    def _new_can_cons_stats(alias: str, year: str) -> dict[str, Any]:
        counts = {
            "media_alias": alias,
            "year": year,
            "issues": 0,
            "content_items_out": 0,
            "lang_fd": Counter(),
        }
        if src_medium and src_medium == "audio":
            counts["audios"] = 0
        else:
            counts["pages"] = 0
            counts["images"] = 0
            counts["reocred_cis"] = 0
        return counts

    def _langs_for_issue(issue: dict[str, Any]) -> list[str]:
        langs = []
        for ci in issue["i"]:
            if "consolidated_lg" not in ci["m"]:
                if "tp" in ci["m"] and ci["m"]["tp"] == "image":
                    # images don't need a language
                    langs.append("N/A")
                else:
                    # some non-image CIs don't have a language and should
                    langs.append("Not defined")
            elif ci["m"]["consolidated_lg"] is None:
                langs.append("None")
            else:
                langs.append(ci["m"]["consolidated_lg"])
        return langs

    def _update_can_cons_stats(
        acc: dict[tuple[str, str], dict[str, Any]], issue: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        issue_id = issue["id"]
        alias, year = issue_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_can_cons_stats(alias, year))

        entry["issues"] += 1
        entry["content_items_out"] += len(issue["i"])
        entry["lang_fd"].update(_langs_for_issue(issue))

        if src_medium and src_medium == "audio":
            if "sm" not in issue or issue["sm"] != src_medium:
                log_src_medium_mismatch(issue_id, "canonical", src_medium, issue["sm"])
            entry["audios"] += len(set(issue["rr"]))
        else:
            if "sm" in issue and issue["sm"] != src_medium:
                log_src_medium_mismatch(issue_id, "canonical", src_medium, issue["sm"])
            entry["pages"] += len(set(issue["pp"]))
            entry["images"] += len([item for item in issue["i"] if item["m"]["tp"] == "image"])
            entry["reocred_cis"] += len(
                [
                    item
                    for item in issue["i"]
                    if "consolidated_reocr_applied" in item["m"]
                    and item["m"]["consolidated_reocr_applied"]
                ]
            )
        return acc

    def _partition_can_cons_stats(records):
        aggregated = {}
        for issue in records:
            _update_can_cons_stats(aggregated, issue)
        return aggregated

    def _merge_can_cons_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_can_cons_stats(values["media_alias"], values["year"])
                )
                entry["issues"] += values["issues"]
                entry["content_items_out"] += values["content_items_out"]
                entry["lang_fd"].update(values["lang_fd"])
                if src_medium and src_medium == "audio":
                    entry["audios"] += values["audios"]
                else:
                    entry["pages"] += values["pages"]
                    entry["images"] += values["images"]
                    entry["reocred_cis"] += values["reocred_cis"]
        return merged

    aggregated = s3_can_cons_issues.reduction(
        perpartition=_partition_can_cons_stats,
        aggregate=_merge_can_cons_stats,
        split_every=8,
    )

    if client is not None:
        progress(aggregated)

    aggregated_result = aggregated.compute()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        [
            {
                **{k: v for k, v in values.items() if k not in {"lang_fd"}},
                "lang_fd": dict(values["lang_fd"]),
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )


### DEFINITION of tunique ###


# define locally the nunique() aggregation function for dask
def chunk(s):
    """The function applied to the individual partition (map).
    Part of the ggregating function(s) implementing np.nunique()
    """
    return s.apply(lambda x: list(set(x)))


def agg(s):
    """The function which will aggregate the result from all the partitions (reduce).
    Part of the ggregating function(s) implementing np.nunique()
    """
    s = s._selected_obj
    # added apply(list) because in newer versions of pandas, it was ndarrays.
    return s.apply(list).groupby(level=list(range(s.index.nlevels))).sum()


def finalize(s):
    """The optional function that will be applied to the result of the agg_tu functions.
    Part of the ggregating function(s) implementing np.nunique()
    """
    return s.apply(lambda x: len(set(x)))


# aggregating function implementing np.nunique()
tunique = dd.Aggregation("tunique", chunk, agg, finalize)

### DEFINITION of tunique ###


def compute_stats_in_rebuilt_bag(
    rebuilt_articles: Bag,
    key: str = "",
    include_alias: bool = False,
    passim: bool = False,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, int | str]]:
    """Compute stats on a dask bag of rebuilt output content-items.

    Args:
        rebuilt_articles (db.core.Bag): Bag with the contents of rebuilt files.
        key (str, optional): Optionally title-year pair for on-the-fly computation.
            Defaults to "".
        include_alias (bool, optional): Whether to include the title in the groupby,
            not necessary for on-the-fly computation. Defaults to False.
        passim (bool, optional): True if rebuilt is in passim format. Defaults to False.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Union[int, str]]]: List of counts that match rebuilt or paassim
        DataStatistics keys.
    """
    # when called in the rebuilt, all the rebuilt articles in the bag
    # are from the same newspaper and year
    if title is None:
        title = key.split("-")[0]
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_rebuilt_stats(media_alias: str | None, year: str) -> dict[str, Any]:
        counts = {
            "year": year,
            "issues": set(),
            "content_items_out": 0,
        }
        if include_alias:
            counts["media_alias"] = media_alias
        if not passim:
            counts["ft_tokens"] = 0
        return counts

    def _update_rebuilt_stats(
        acc: dict[Any, dict[str, Any]], rebuilt_ci: dict[str, Any]
    ) -> dict[Any, dict[str, Any]]:
        counts = counts_for_rebuilt(
            rebuilt_ci,
            include_alias=include_alias,
            passim=passim,
        )
        media_alias = counts.get("media_alias")
        year = counts["year"]
        group_key = (media_alias, year) if include_alias else year
        entry = acc.setdefault(group_key, _new_rebuilt_stats(media_alias, year))

        entry["issues"].add(counts["issues"])
        entry["content_items_out"] += counts["content_items_out"]
        if not passim:
            entry["ft_tokens"] += counts["ft_tokens"]
        return acc

    def _partition_rebuilt_stats(records):
        aggregated = {}
        for rebuilt_ci in records:
            _update_rebuilt_stats(aggregated, rebuilt_ci)
        return aggregated

    def _merge_rebuilt_stats(partials):
        merged = {}
        for partial in partials:
            for group_key, values in partial.items():
                entry = merged.setdefault(
                    group_key,
                    _new_rebuilt_stats(values.get("media_alias"), values["year"]),
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"] += values["content_items_out"]
                if not passim:
                    entry["ft_tokens"] += values["ft_tokens"]
        return merged

    aggregated = rebuilt_articles.reduction(
        perpartition=_partition_rebuilt_stats,
        aggregate=_merge_rebuilt_stats,
        split_every=8,
    )

    msg = "Obtaining the yearly rebuilt statistics"
    if key != "":
        logger.info("%s for %s", msg, key)
    else:
        logger.info(msg)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated)

    aggregated_result = aggregated.compute()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        [
            {
                **(
                    {"media_alias": values["media_alias"]}
                    if include_alias
                    else {}
                ),
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": values["content_items_out"],
                **({"ft_tokens": values["ft_tokens"]} if not passim else {}),
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (
            row["media_alias"] if include_alias else "",
            row["year"],
        ),
    )


def compute_stats_in_entities_bag(
    s3_entities: Bag, client: Client | None = None, title: str | None = None
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of entities output content-items.

    A problem with the initial approach to explode and then sum the values of
    `content_items_out` and `ne_mentions` was found: the summmed counts were
    multiplied by the length of `ne_entities`:
    - when len(`ne_entities`)=0, the row was omitted altogether
    - when len(`ne_entities`)>1, there was an overcounting of the number of CIs.

    The fix is in two parts:
    - using tunique for the number of CIs (same logic as the Issue Ids).
    - aggregating separately the sum of `ne_mentions` BEFORE the explode to
    prevent duplication and ensure CIs with zero entities are still counted.

    Args:
        s3_entities (db.core.Bag): Bag with the contents of entity files.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match NE DataStatistics keys.
    """
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_entities_stats(alias: str, year: str) -> dict[str, Any]:
        return {
            "media_alias": alias,
            "year": year,
            "issues": set(),
            "content_items_out": set(),
            "ne_mentions": 0,
            "ne_entities": set(),
        }

    def _extract_entity_ids(ci: dict[str, Any]) -> set[str]:
        return {
            mention["wkd_id"]
            for mention in ci.get("nes", [])
            if "wkd_id" in mention and mention["wkd_id"] not in ["NIL", None]
        }

    def _update_entities_stats(
        acc: dict[tuple[str, str], dict[str, Any]], ci: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        ci_id = ci["id"] if "id" in ci else ci["ci_id"]
        alias, year = ci_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_entities_stats(alias, year))

        entry["issues"].add("-".join(ci_id.split("-")[:-1]))
        entry["content_items_out"].add(ci_id)
        entry["ne_mentions"] += len(ci.get("nes", []))
        entry["ne_entities"].update(_extract_entity_ids(ci))
        return acc

    def _partition_entities_stats(records):
        aggregated = {}
        for ci in records:
            _update_entities_stats(aggregated, ci)
        return aggregated

    def _merge_entities_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_entities_stats(values["media_alias"], values["year"])
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"].update(values["content_items_out"])
                entry["ne_mentions"] += values["ne_mentions"]
                entry["ne_entities"].update(values["ne_entities"])
        return merged

    aggregated = s3_entities.reduction(
        perpartition=_partition_entities_stats,
        aggregate=_merge_entities_stats,
        split_every=8,
    )

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated)

    aggregated_result = aggregated.compute()

    return sorted(
        [
            {
                "media_alias": values["media_alias"],
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": len(values["content_items_out"]),
                "ne_mentions": values["ne_mentions"],
                "ne_entities": len(values["ne_entities"]),
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )


def compute_stats_in_langident_bag(
    s3_langident: Bag,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of langident output content-items.

    Args:
        s3_langident (db.core.Bag): Bag of lang-id content-items.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match langident DataStatistics keys.
    """
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_langident_stats(alias: str, year: str) -> dict[str, Any]:
        return {
            "media_alias": alias,
            "year": year,
            "issues": set(),
            "content_items_out": 0,
            "images": 0,
            "lang_fd": Counter(),
        }

    def _update_langident_stats(
        acc: dict[tuple[str, str], dict[str, Any]], ci: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        ci_id = ci["id"]
        alias, year = ci_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_langident_stats(alias, year))

        entry["issues"].add("-".join(ci_id.split("-")[:-1]))
        entry["content_items_out"] += 1
        entry["images"] += 1 if ci["tp"] == "img" else 0
        entry["lang_fd"].update(["None" if ci["lg"] is None else ci["lg"]])
        return acc

    def _partition_langident_stats(records):
        aggregated = {}
        for ci in records:
            _update_langident_stats(aggregated, ci)
        return aggregated

    def _merge_langident_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_langident_stats(values["media_alias"], values["year"])
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"] += values["content_items_out"]
                entry["images"] += values["images"]
                entry["lang_fd"].update(values["lang_fd"])
        return merged

    aggregated = s3_langident.reduction(
        perpartition=_partition_langident_stats,
        aggregate=_merge_langident_stats,
        split_every=8,
    )

    if client is not None:
        progress(aggregated)

    aggregated_result = aggregated.compute()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        [
            {
                "media_alias": values["media_alias"],
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": values["content_items_out"],
                "images": values["images"],
                "lang_fd": dict(values["lang_fd"]),
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )


def compute_stats_in_text_reuse_passage_bag(
    s3_tr_passages: Bag,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of text-reuse passages.

    Args:
        s3_tr_passages (Bag): Text-reuse passages contained in one output file.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match text-reuse DataStatistics keys.
    """
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_text_reuse_stats(alias: str, year: str) -> dict[str, Any]:
        return {
            "media_alias": alias,
            "year": year,
            "issues": set(),
            "content_items_out": set(),
            "text_reuse_passages": 0,
            "text_reuse_clusters": set(),
        }

    def _update_text_reuse_stats(
        acc: dict[tuple[str, str], dict[str, Any]], passage: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        ci_id = passage["ci_id"]
        alias, year = ci_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_text_reuse_stats(alias, year))

        entry["issues"].add("-".join(ci_id.split("-")[:-1]))
        entry["content_items_out"].add(ci_id)
        entry["text_reuse_passages"] += 1
        entry["text_reuse_clusters"].add(passage["cluster_id"])
        return acc

    def _partition_text_reuse_stats(records):
        aggregated = {}
        for passage in records:
            _update_text_reuse_stats(aggregated, passage)
        return aggregated

    def _merge_text_reuse_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_text_reuse_stats(values["media_alias"], values["year"])
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"].update(values["content_items_out"])
                entry["text_reuse_passages"] += values["text_reuse_passages"]
                entry["text_reuse_clusters"].update(values["text_reuse_clusters"])
        return merged

    aggregated = s3_tr_passages.reduction(
        perpartition=_partition_text_reuse_stats,
        aggregate=_merge_text_reuse_stats,
        split_every=8,
    )

    if client is not None:
        progress(aggregated)

    aggregated_result = aggregated.compute()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        [
            {
                "media_alias": values["media_alias"],
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": len(values["content_items_out"]),
                "text_reuse_passages": values["text_reuse_passages"],
                "text_reuse_clusters": len(values["text_reuse_clusters"]),
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )


def compute_stats_in_topics_bag(
    s3_topics: Bag,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of topic modeling output content-items.

    Args:
        s3_topics (db.core.Bag): Bag with the contents of topics files.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match topics DataStatistics keys.
    """
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_topic_stats(alias: str, year: str) -> dict[str, Any]:
        return {
            "media_alias": alias,
            "year": year,
            "issues": set(),
            "content_items_out": set(),
            "topics": set(),
            "topics_fd": Counter(),
        }

    def _update_topic_stats(acc: dict[tuple[str, str], dict[str, Any]], ci: dict[str, Any]):
        ci_id = ci["ci_id"]
        alias, year = ci_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_topic_stats(alias, year))

        topics = [topic["t"] for topic in ci.get("topics", []) if "t" in topic]
        if not topics:
            topics = ["no-topic"]

        entry["issues"].add(ci_id.split("-i")[0])
        entry["content_items_out"].add(ci_id)
        entry["topics"].update(topics)
        entry["topics_fd"].update(topics)
        return acc

    def _partition_topic_stats(records):
        aggregated = {}
        for ci in records:
            _update_topic_stats(aggregated, ci)
        return aggregated

    def _merge_topic_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_topic_stats(values["media_alias"], values["year"])
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"].update(values["content_items_out"])
                entry["topics"].update(values["topics"])
                entry["topics_fd"].update(values["topics_fd"])
        return merged

    aggregated = s3_topics.reduction(
        perpartition=_partition_topic_stats,
        aggregate=_merge_topic_stats,
        split_every=8,
    )

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated)

    try:
        aggregated_result = aggregated.compute()
    except Exception as e:
        msg = f"{title} - Warning! the aggregated topics stats were empty!! {e}"
        print(msg)
        logger.warning(msg)
        return []

    if not aggregated_result:
        msg = f"{title} - Warning! the aggregated topics stats were empty!!"
        print(msg)
        logger.warning(msg)
        return []

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        [
            {
                "media_alias": values["media_alias"],
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": len(values["content_items_out"]),
                "topics": len(values["topics"]),
                "topics_fd": dict(values["topics_fd"]),
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )


def compute_stats_in_img_emb_bag(
    s3_emb_images: Bag,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, int | str]]:
    """Compute stats on a dask bag of image embedding output content-items.

    Args:
        s3_emb_images (db.core.Bag): Bag with the contents of the embedded images files.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Union[int, str]]]: List of counts that match image embeddings
        DataStatistics keys.
    """
    # when called in the rebuilt, all the rebuilt articles in the bag
    # are from the same newspaper and year
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_img_emb_stats(alias: str, year: str) -> dict[str, Any]:
        return {
            "media_alias": alias,
            "year": year,
            "issues": set(),
            "content_items_out": set(),
            "images": 0,
        }

    def _update_img_emb_stats(
        acc: dict[tuple[str, str], dict[str, Any]], ci: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        ci_id = ci["ci_id"]
        alias, year = ci_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_img_emb_stats(alias, year))

        entry["issues"].add("-".join(ci_id.split("-")[:-1]))
        entry["content_items_out"].add(ci_id)
        entry["images"] += 1
        return acc

    def _partition_img_emb_stats(records):
        aggregated = {}
        for ci in records:
            _update_img_emb_stats(aggregated, ci)
        return aggregated

    def _merge_img_emb_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_img_emb_stats(values["media_alias"], values["year"])
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"].update(values["content_items_out"])
                entry["images"] += values["images"]
        return merged

    aggregated = s3_emb_images.reduction(
        perpartition=_partition_img_emb_stats,
        aggregate=_merge_img_emb_stats,
        split_every=8,
    )

    if client is not None:
        progress(aggregated)

    aggregated_result = aggregated.compute()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        [
            {
                "media_alias": values["media_alias"],
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": len(values["content_items_out"]),
                "images": values["images"],
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )


def compute_stats_in_lingproc_bag(
    s3_lingprocs: Bag,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, int | str]]:
    """Compute stats on a dask bag of linguistic preprocessing output content-items.

    Args:
        s3_lingprocs (db.core.Bag): Bag with the contents of the lingproc files.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Union[int, str]]]: List of counts that match lingproc.
        DataStatistics keys.
    """
    # when called in the rebuilt, all the rebuilt articles in the bag
    # are from the same newspaper and year
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_lingproc_stats(alias: str, year: str) -> dict[str, Any]:
        return {
            "media_alias": alias,
            "year": year,
            "issues": set(),
            "content_items_out": 0,
        }

    def _update_lingproc_stats(
        acc: dict[tuple[str, str], dict[str, Any]], ci: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        ci_id = ci.get("ci_id", ci.get("id"))
        alias, year = ci_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_lingproc_stats(alias, year))

        entry["issues"].add("-".join(ci_id.split("-")[:-1]))
        entry["content_items_out"] += 1
        return acc

    def _partition_lingproc_stats(records):
        aggregated = {}
        for ci in records:
            _update_lingproc_stats(aggregated, ci)
        return aggregated

    def _merge_lingproc_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_lingproc_stats(values["media_alias"], values["year"])
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"] += values["content_items_out"]
        return merged

    aggregated = s3_lingprocs.reduction(
        perpartition=_partition_lingproc_stats,
        aggregate=_merge_lingproc_stats,
        split_every=8,
    )

    if client is not None:
        progress(aggregated)

    aggregated_result = aggregated.compute()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        [
            {
                "media_alias": values["media_alias"],
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": values["content_items_out"],
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )


def compute_stats_in_solr_text_ing_bag(
    s3_solr_ing_cis: Bag,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, int | str]]:
    """Compute stats on a dask bag of Solr text post-ingestion reports.

    Args:
        s3_solr_ing_cis (db.core.Bag): Bag with the CI ids and token lengths from Solr.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Union[int, str]]]: List of counts that match Solr text ingestion
        DataStatistics keys.
    """
    # when called in the rebuilt, all the rebuilt articles in the bag
    # are from the same newspaper and year
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_solr_stats(alias: str, year: str) -> dict[str, Any]:
        return {
            "media_alias": alias,
            "year": year,
            "issues": set(),
            "content_items_out": 0,
            "ft_tokens": 0,
        }

    def _update_solr_stats(
        acc: dict[tuple[str, str], dict[str, Any]], ci: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        ci_id = ci["id"]
        alias, year = ci_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_solr_stats(alias, year))

        entry["issues"].add("-".join(ci_id.split("-")[:-1]))
        entry["content_items_out"] += 1
        entry["ft_tokens"] += ci["content_length_i"]
        return acc

    def _partition_solr_stats(records):
        aggregated = {}
        for ci in records:
            _update_solr_stats(aggregated, ci)
        return aggregated

    def _merge_solr_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_solr_stats(values["media_alias"], values["year"])
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"] += values["content_items_out"]
                entry["ft_tokens"] += values["ft_tokens"]
        return merged

    aggregated = s3_solr_ing_cis.reduction(
        perpartition=_partition_solr_stats,
        aggregate=_merge_solr_stats,
        split_every=8,
    )

    if client is not None:
        progress(aggregated)

    aggregated_result = aggregated.compute()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        [
            {
                "media_alias": values["media_alias"],
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": values["content_items_out"],
                "ft_tokens": values["ft_tokens"],
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )


def compute_stats_in_ocrqa_bag(
    s3_ocrqas: Bag,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, int | str]]:
    """Compute stats on a dask bag of OCRQA outputs.

    Args:
        s3_ocrqas (db.core.Bag): Bag with the contents of the OCRQA files.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Union[int, str]]]: List of counts that match OCRQA output
        DataStatistics keys.
    """
    # when called in the rebuilt, all the rebuilt articles in the bag
    # are from the same newspaper and year
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_ocrqa_stats(alias: str, year: str) -> dict[str, Any]:
        return {
            "media_alias": alias,
            "year": year,
            "issues": set(),
            "content_items_out": 0,
            "avg_ocrqa_sum": 0.0,
            "avg_ocrqa_count": 0,
        }

    def _update_ocrqa_stats(
        acc: dict[tuple[str, str], dict[str, Any]], ci: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        ci_id = ci["ci_id"]
        alias, year = ci_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_ocrqa_stats(alias, year))

        entry["issues"].add("-".join(ci_id.split("-")[:-1]))
        entry["content_items_out"] += 1
        if ci["ocrqa"] is not None:
            entry["avg_ocrqa_sum"] += float(ci["ocrqa"])
            entry["avg_ocrqa_count"] += 1
        return acc

    def _partition_ocrqa_stats(records):
        aggregated = {}
        for ci in records:
            _update_ocrqa_stats(aggregated, ci)
        return aggregated

    def _merge_ocrqa_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_ocrqa_stats(values["media_alias"], values["year"])
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"] += values["content_items_out"]
                entry["avg_ocrqa_sum"] += values["avg_ocrqa_sum"]
                entry["avg_ocrqa_count"] += values["avg_ocrqa_count"]
        return merged

    aggregated = s3_ocrqas.reduction(
        perpartition=_partition_ocrqa_stats,
        aggregate=_merge_ocrqa_stats,
        split_every=8,
    )

    if client is not None:
        progress(aggregated)

    aggregated_result = aggregated.compute()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        [
            {
                "media_alias": values["media_alias"],
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": values["content_items_out"],
                "avg_ocrqa": (
                    round(values["avg_ocrqa_sum"] / values["avg_ocrqa_count"], 3)
                    if values["avg_ocrqa_count"] > 0
                    else None
                ),
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )


def compute_stats_in_langid_ocrqa_bag(
    s3_langid_ocrqas: Bag,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, int | str]]:
    """Compute stats on a dask bag of OCRQA outputs.

    Args:
        s3_langid_ocrqas (db.core.Bag): Bag with the contents of the OCRQA files.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Union[int, str]]]: List of counts that match OCRQA output
        DataStatistics keys.
    """
    # when called in the rebuilt, all the rebuilt articles in the bag
    # are from the same newspaper and year
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_langid_ocrqa_stats(alias: str, year: str) -> dict[str, Any]:
        return {
            "media_alias": alias,
            "year": year,
            "issues": set(),
            "content_items_out": 0,
            "images": 0,
            "lang_fd": Counter(),
            "avg_ocrqa_sum": 0.0,
            "avg_ocrqa_count": 0,
        }

    def _update_langid_ocrqa_stats(
        acc: dict[tuple[str, str], dict[str, Any]], ci: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        ci_id = ci["id"]
        alias, year = ci_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_langid_ocrqa_stats(alias, year))

        entry["issues"].add("-".join(ci_id.split("-")[:-1]))
        entry["content_items_out"] += 1
        entry["images"] += 1 if ci["tp"] == "img" else 0
        entry["lang_fd"].update(["None" if ci["lg"] is None else ci["lg"]])

        if ci["ocrqa"] is not None:
            entry["avg_ocrqa_sum"] += float(ci["ocrqa"])
            entry["avg_ocrqa_count"] += 1
        return acc

    def _partition_langid_ocrqa_stats(records):
        aggregated = {}
        for ci in records:
            _update_langid_ocrqa_stats(aggregated, ci)
        return aggregated

    def _merge_langid_ocrqa_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_langid_ocrqa_stats(values["media_alias"], values["year"])
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"] += values["content_items_out"]
                entry["images"] += values["images"]
                entry["lang_fd"].update(values["lang_fd"])
                entry["avg_ocrqa_sum"] += values["avg_ocrqa_sum"]
                entry["avg_ocrqa_count"] += values["avg_ocrqa_count"]
        return merged

    aggregated = s3_langid_ocrqas.reduction(
        perpartition=_partition_langid_ocrqa_stats,
        aggregate=_merge_langid_ocrqa_stats,
        split_every=8,
    )

    if client is not None:
        progress(aggregated)

    aggregated_result = aggregated.compute()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        [
            {
                "media_alias": values["media_alias"],
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": values["content_items_out"],
                "images": values["images"],
                "lang_fd": dict(values["lang_fd"]),
                "avg_ocrqa": (
                    round(values["avg_ocrqa_sum"] / values["avg_ocrqa_count"], 3)
                    if values["avg_ocrqa_count"] > 0
                    else None
                ),
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )


def compute_stats_in_doc_emb_bag(
    s3_doc_embeddings: Bag,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, int | str]]:
    """Compute stats on a dask bag of document embeddings.

    Args:
        s3_solr_ing_cis (db.core.Bag): Bag with the contents of doc embeddings files.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Union[int, str]]]: List of counts that match document embeddings
        DataStatistics keys.
    """
    # when called in the rebuilt, all the rebuilt articles in the bag
    # are from the same newspaper and year
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_doc_emb_stats(alias: str, year: str) -> dict[str, Any]:
        return {
            "media_alias": alias,
            "year": year,
            "issues": set(),
            "content_items_out": 0,
        }

    def _update_doc_emb_stats(
        acc: dict[tuple[str, str], dict[str, Any]], ci: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        ci_id = ci["ci_id"]
        alias, year = ci_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_doc_emb_stats(alias, year))

        entry["issues"].add("-".join(ci_id.split("-")[:-1]))
        entry["content_items_out"] += 1
        return acc

    def _partition_doc_emb_stats(records):
        aggregated = {}
        for ci in records:
            _update_doc_emb_stats(aggregated, ci)
        return aggregated

    def _merge_doc_emb_stats(partials):
        merged = {}
        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_doc_emb_stats(values["media_alias"], values["year"])
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"] += values["content_items_out"]
        return merged

    aggregated = s3_doc_embeddings.reduction(
        perpartition=_partition_doc_emb_stats,
        aggregate=_merge_doc_emb_stats,
        split_every=8,
    )

    if client is not None:
        progress(aggregated)

    aggregated_result = aggregated.compute()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    return sorted(
        [
            {
                "media_alias": values["media_alias"],
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": values["content_items_out"],
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )


def compute_stats_in_classif_img_bag(
    s3_classif_images: Bag,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of topic modeling output content-items.

    Args:
        s3_classif_images (db.core.Bag): Bag with the contents of the image classification files.
        client (Client | None, optional): Dask client. Defaults to None.
        title (str, optional): Media title for which the stats are being computed.
            Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match topics DataStatistics keys.
    """
    print(f"{title} - Fetched all files, gathering desired information.")
    logger.info("%s - Fetched all files, gathering desired information.", title)

    def _new_classif_img_stats(alias: str, year: str) -> dict[str, Any]:
        return {
            "media_alias": alias,
            "year": year,
            "issues": set(),
            "content_items_out": 0,
            "images": 0,
            "img_level0_class_fd": Counter(),
            "img_level1_class_fd": Counter(),
            "img_level2_class_fd": Counter(),
            "img_level3_class_fd": Counter(),
        }

    def _classes_for_image(ci: dict[str, Any]) -> dict[str, str]:
        level3_class = ci["level3_predictions"][0]["class"]
        non_image_fallback = "not_image" if level3_class == "not_image" else "not_inferred"

        return {
            "img_level0_class_fd": "image" if level3_class != "not_image" else "not_image",
            "img_level1_class_fd": (
                non_image_fallback
                if "level1_predictions" not in ci
                else ci["level1_predictions"][0]["class"]
            ),
            "img_level2_class_fd": (
                non_image_fallback
                if "level2_predictions" not in ci
                else ci["level2_predictions"][0]["class"]
            ),
            "img_level3_class_fd": level3_class,
        }

    def _update_classif_img_stats(
        acc: dict[tuple[str, str], dict[str, Any]], ci: dict[str, Any]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        ci_id = ci["ci_id"]
        alias, year = ci_id.split("-")[:2]
        key = (alias, year)
        entry = acc.setdefault(key, _new_classif_img_stats(alias, year))

        entry["issues"].add(ci_id.split("-i")[0])
        entry["content_items_out"] += 1
        entry["images"] += 1

        for class_key, class_value in _classes_for_image(ci).items():
            entry[class_key].update([class_value])
        return acc

    def _partition_classif_img_stats(records):
        aggregated = {}
        for ci in records:
            _update_classif_img_stats(aggregated, ci)
        return aggregated

    def _merge_classif_img_stats(partials):
        merged = {}
        counter_keys = [
            "img_level0_class_fd",
            "img_level1_class_fd",
            "img_level2_class_fd",
            "img_level3_class_fd",
        ]

        for partial in partials:
            for key, values in partial.items():
                entry = merged.setdefault(
                    key, _new_classif_img_stats(values["media_alias"], values["year"])
                )
                entry["issues"].update(values["issues"])
                entry["content_items_out"] += values["content_items_out"]
                entry["images"] += values["images"]
                for counter_key in counter_keys:
                    entry[counter_key].update(values[counter_key])
        return merged

    aggregated = s3_classif_images.reduction(
        perpartition=_partition_classif_img_stats,
        aggregate=_merge_classif_img_stats,
        split_every=8,
    )

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated)

    aggregated_result = aggregated.compute()

    return sorted(
        [
            {
                "media_alias": values["media_alias"],
                "year": values["year"],
                "issues": len(values["issues"]),
                "content_items_out": values["content_items_out"],
                "images": values["images"],
                "img_level0_class_fd": dict(values["img_level0_class_fd"]),
                "img_level1_class_fd": dict(values["img_level1_class_fd"]),
                "img_level2_class_fd": dict(values["img_level2_class_fd"]),
                "img_level3_class_fd": dict(values["img_level3_class_fd"]),
            }
            for values in aggregated_result.values()
        ],
        key=lambda row: (row["media_alias"], row["year"]),
    )
