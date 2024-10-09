"""Helper functions to used to compute and aggragate the statistics of manifests.
"""

import logging
from ast import literal_eval
from collections import Counter
from typing import Any
from dask import dataframe as dd
from dask.bag.core import Bag
from dask.distributed import progress, Client

logger = logging.getLogger(__name__)


def counts_for_canonical_issue(
    issue: dict[str, Any], include_np_yr: bool = False
) -> dict[str, int]:
    """Given the canonical representation of an issue, get its counts.

    Args:
        issue (dict[str, Any]): Canonical JSON representation of an issue.
        include_np_yr (bool, optional): Whether the newspaper title and year should
            be included in the returned dict for later aggregation. Defaults to False.

    Returns:
        dict[str, int]: Dict listing the counts for this issue, ready to be aggregated.
    """
    counts = (
        {
            "np_id": issue["id"].split("-")[0],
            "year": issue["id"].split("-")[1],
        }
        if include_np_yr
        else {}
    )
    counts.update(
        {
            "issues": 1,
            "pages": len(set(issue["pp"])),
            "content_items_out": len(issue["i"]),
            "images": len([item for item in issue["i"] if item["m"]["tp"] == "image"]),
        }
    )
    return counts


def counts_for_rebuilt(
    rebuilt_ci: dict[str, Any], include_np: bool = False, passim: bool = False
) -> dict[str, int | str]:
    """Define the counts for 1 given rebuilt content-item to match the count keys.

    Args:
        rebuilt_ci (dict[str, Any]): Rebuilt content-item from which to extract counts.
        include_np (bool, optional): Whether to include the title in resulting dict,
            not necessary for on-the-fly computation. Defaults to False.
        passim (bool, optional): True if rebuilt is in passim format. Defaults to False.

    Returns:
        dict[str, Union[int, str]]: Dict with rebuilt (passim) keys and counts for 1 CI.
    """
    split_id = rebuilt_ci["id"].split("-")
    counts = {"np_id": split_id[0]} if include_np else {}
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
    s3_canonical_issues: Bag, client: Client | None = None
) -> list[dict[str, Any]]:
    """Computes number of issues and pages per newspaper from a Dask bag of canonical data.

    Args:
        s3_canonical_issues (db.core.Bag): Bag with the contents of canonical files to
            compute statistics on.

    Returns:
        list[dict[str, Any]]: List of counts that match canonical DataStatistics keys.
    """

    print("Fetched all issues, gathering desired information.")
    logger.info("Fetched all issues, gathering desired information.")
    count_df = (
        s3_canonical_issues.map(
            lambda i: counts_for_canonical_issue(i, include_np_yr=True)
        )
        .to_dataframe(
            meta={
                "np_id": str,
                "year": str,
                "issues": int,
                "pages": int,
                "images": int,
                "content_items_out": int,
            }
        )
        .persist()
    )

    # cum the counts for all values collected
    aggregated_df = (
        count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": sum,
                "pages": sum,
                "content_items_out": sum,
                "images": sum,
            }
        )
        .reset_index()
    ).persist()

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    print("Finished grouping and aggregating stats by title and year.")
    logger.info("Finished grouping and aggregating stats by title and year.")
    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


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
    include_np: bool = False,
    passim: bool = False,
    client: Client | None = None,
) -> list[dict[str, int | str]]:
    """Compute stats on a dask bag of rebuilt output content-items.

    Args:
        rebuilt_articles (db.core.Bag): Bag with the contents of rebuilt files.
        key (str, optional): Optionally title-year pair for on-the-fly computation.
            Defaults to "".
        include_np (bool, optional): Whether to include the title in the groupby,
            not necessary for on-the-fly computation. Defaults to False.
        passim (bool, optional): True if rebuilt is in passim format. Defaults to False.
        client (Client | None, optional): Dask client. Defaults to None.

    Returns:
        list[dict[str, Union[int, str]]]: List of counts that match rebuilt or paassim
            DataStatistics keys.
    """
    # when called in the rebuilt, all the rebuilt articles in the bag
    # are from the same newspaper and year
    print("Fetched all files, gathering desired information.")
    logger.info("Fetched all files, gathering desired information.")

    # define the list of columns in the dataframe
    df_meta = {"np_id": str} if include_np else {}
    df_meta.update(
        {
            "year": str,
            "issues": str,
            "content_items_out": int,
        }
    )
    if not passim:
        df_meta.update(
            {
                "ft_tokens": int,
            }
        )

    rebuilt_count_df = (
        rebuilt_articles.map(
            lambda rf: counts_for_rebuilt(rf, include_np=include_np, passim=passim)
        )
        .to_dataframe(meta=df_meta)
        .persist()
    )

    gp_key = ["np_id", "year"] if include_np else "year"
    # agggregate them at the scale of the entire corpus
    # first groupby title, year and issue to also count the individual issues present
    if not passim:
        aggregated_df = rebuilt_count_df.groupby(by=gp_key).agg(
            {"issues": tunique, "content_items_out": sum, "ft_tokens": sum}
        )
    else:
        aggregated_df = rebuilt_count_df.groupby(by=gp_key).agg(
            {"issues": tunique, "content_items_out": sum}
        )

    # when titles are included, multiple titles and years will be represented
    if include_np:
        aggregated_df = aggregated_df.reset_index().persist()

    msg = "Obtaining the yearly rebuilt statistics"
    if key != "":
        logger.info("%s for %s", msg, key)
    else:
        logger.info(msg)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    return aggregated_df.to_bag(format="dict").compute()


def compute_stats_in_entities_bag(
    s3_entities: Bag, client: Client | None = None
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of entities output content-items.

    Args:
        s3_entities (db.core.Bag): Bag with the contents of entity files.
        client (Client | None, optional): Dask client. Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match NE DataStatistics keys.
    """
    count_df = (
        s3_entities.map(
            lambda ci: {
                "np_id": ci["id"].split("-")[0],
                "year": ci["id"].split("-")[1],
                "issues": "-".join(ci["id"].split("-")[:-1]),
                "content_items_out": 1,
                "ne_mentions": len(ci["nes"]),
                "ne_entities": sorted(
                    list(
                        set(
                            [
                                m["wkd_id"]
                                for m in ci["nes"]
                                if "wkd_id" in m and m["wkd_id"] not in ["NIL", None]
                            ]
                        )
                    )
                ),  # sorted list to ensure all are the same
            }
        ).to_dataframe(
            meta={
                "np_id": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
                "ne_mentions": int,
                "ne_entities": object,
            }
        )
        # .explode("ne_entities")
        # .persist()
    )

    count_df["ne_entities"] = count_df["ne_entities"].apply(
        lambda x: x if isinstance(x, list) else [x]
    )
    count_df = count_df.explode("ne_entities").persist()

    # cum the counts for all values collected
    aggregated_df = (
        count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
                "ne_mentions": sum,
                "ne_entities": tunique,
            }
        )
        .reset_index()
    ).persist()

    print("Finished grouping and aggregating stats by title and year.")
    logger.info("Finished grouping and aggregating stats by title and year.")

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


def compute_stats_in_langident_bag(
    s3_langident: Bag, client: Client | None = None
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of langident output content-items.

    Args:
        s3_langident (db.core.Bag): Bag of lang-id content-items.
        client (Client | None, optional): Dask client. Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match langident DataStatistics keys.
    """

    def freq(x, col="lang_fd"):
        x[col] = dict(Counter(literal_eval(x[col])))
        return x

    count_df = (
        s3_langident.map(
            lambda ci: {
                "np_id": ci["id"].split("-")[0],
                "year": ci["id"].split("-")[1],
                "issues": "-".join(ci["id"].split("-")[:-1]),
                "content_items_out": 1,
                "images": 1 if ci["tp"] == "img" else 0,
                "lang_fd": "None" if ci["lg"] is None else ci["lg"],
            }
        )
        .to_dataframe(
            meta={
                "np_id": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
                "images": int,
                "lang_fd": object,
            }
        )
        .persist()
    )

    # cum the counts for all values collected
    aggregated_df = (
        count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
                "images": sum,
                "lang_fd": list,
            }
        )
        .reset_index()
    ).persist()

    # Dask dataframes did not support using literal_eval
    agg_bag = aggregated_df.to_bag(format="dict").map(freq)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(agg_bag)

    return agg_bag.compute()


def compute_stats_in_solr_text_bag(
    s3_solr_text: Bag, client: Client | None = None
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of content-items formatted for Solr input.

    Args:
        s3_solr_text (db.core.Bag): Bag or Solr formatted content-items.
        client (Client | None, optional): Dask client. Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match solr text DataStatistics keys.
    """
    count_df = (
        s3_solr_text.map(
            lambda ci: {
                "np_id": ci["meta_journal_s"],
                "year": ci["meta_year_i"],
                "issues": ci["meta_issue_id_s"],
                "content_items_out": 1,
            }
        )
        .to_dataframe(
            meta={
                "np_id": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
            }
        )
        .persist()
    )

    # cum the counts for all values collected
    aggregated_df = (
        count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
            }
        )
        .reset_index()
    ).persist()

    print("Finished grouping and aggregating stats by title and year.")
    logger.info("Finished grouping and aggregating stats by title and year.")

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


def compute_stats_in_text_reuse_passage_bag(
    s3_tr_passages: Bag, client: Client | None = None
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of text-reuse passages.

    Args:
        s3_tr_passages (Bag): Text-reuse passages contained in one output file.
        client (Client | None, optional): Dask client. Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match text-reuse DataStatistics keys.
    """
    count_df = (
        s3_tr_passages.map(
            lambda passage: {
                "np_id": passage["ci_id"].split("-")[0],
                "year": passage["ci_id"].split("-")[1],
                "issues": "-".join(passage["ci_id"].split("-")[:-1]),
                "content_items_out": passage["ci_id"],
                "text_reuse_passages": 1,
                "text_reuse_clusters": passage["cluster_id"],
            }
        )
        .to_dataframe(
            meta={
                "np_id": str,
                "year": str,
                "issues": str,
                "content_items_out": str,
                "text_reuse_passages": int,
                "text_reuse_clusters": str,
            }
        )
        .persist()
    )

    aggregated_df = (
        count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": tunique,
                "text_reuse_passages": sum,
                "text_reuse_clusters": tunique,
            }
        )
        .reset_index()
        .sort_values("year")
    ).persist()

    print("Finished grouping and aggregating stats by title and year.")
    logger.info("Finished grouping and aggregating stats by title and year.")

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


def compute_stats_in_topics_bag(
    s3_topics: Bag, client: Client | None = None
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of topic modeling output content-items.

    Args:
        s3_topics (db.core.Bag): Bag with the contents of topics files.
        client (Client | None, optional): Dask client. Defaults to None.

    Returns:
        list[dict[str, Any]]: List of counts that match topics DataStatistics keys.
    """
    count_df = s3_topics.map(
        lambda ci: {
            "np_id": ci["id"].split("-")[0],
            "year": ci["id"].split("-")[1],
            "issues": ci["id"].split("-i")[0],
            "content_items_out": 1,
            "topics": sorted(
                list(
                    set(
                        [
                            t["t"]
                            for t in ci["topics"]
                            if "t" in t and t["t"] not in ["NIL", None]
                        ]
                    )
                )
            ),  # sorted list to ensure all are the same
        }
    ).to_dataframe(
        meta={
            "np_id": str,
            "year": str,
            "issues": str,
            "content_items_out": int,
            "topics": object,
        }
    )

    count_df["topics"] = count_df["topics"].apply(
        lambda x: x if isinstance(x, list) else [x]
    )
    count_df = count_df.explode("topics").persist()

    # cum the counts for all values collected
    aggregated_df = (
        count_df.groupby(by=["np_id", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,  #'count', # ???
                "topics": tunique,
            }
        )
        .reset_index()
    ).persist()

    print("Finished grouping and aggregating stats by title and year.")
    logger.info("Finished grouping and aggregating stats by title and year.")

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()
