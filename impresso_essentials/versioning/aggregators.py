"""Helper functions to used to compute and aggragate the statistics of manifests."""

import logging
from ast import literal_eval
from collections import Counter
from typing import Any
from dask import dataframe as dd
from dask.bag.core import Bag
from dask.distributed import progress, Client

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

    # prep the df's meta and aggregations based on the source medium
    df_meta = {
        "media_alias": str,
        "year": str,
        "issues": int,
        "content_items_out": int,
    }
    df_agg = {
        "issues": sum,
        "content_items_out": sum,
    }

    if src_medium and src_medium == "audio":
        df_meta["audios"] = int
        df_agg["audios"] = sum
    else:
        df_meta["pages"] = int
        df_meta["images"] = int
        df_agg["pages"] = sum
        df_agg["images"] = sum

    count_df = (
        s3_canonical_issues.map(
            lambda i: counts_for_canonical_issue(i, incl_alias_yr=True, src_medium=src_medium)
        )
        .to_dataframe(meta=df_meta)
        .persist()
    )

    # cum the counts for all values collected
    aggregated_df = (
        count_df.groupby(by=["media_alias", "year"]).agg(df_agg).reset_index()
    ).persist()

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

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

    # define the list of columns in the dataframe
    df_meta = {"media_alias": str} if include_alias else {}
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
            lambda rf: counts_for_rebuilt(rf, include_alias=include_alias, passim=passim)
        )
        .to_dataframe(meta=df_meta)
        .persist()
    )

    gp_key = ["media_alias", "year"] if include_alias else "year"
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
    if include_alias:
        aggregated_df = aggregated_df.reset_index().persist()

    msg = "Obtaining the yearly rebuilt statistics"
    if key != "":
        logger.info("%s for %s", msg, key)
    else:
        logger.info(msg)

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    return aggregated_df.to_bag(format="dict").compute()


def compute_stats_in_entities_bag(
    s3_entities: Bag, client: Client | None = None, title: str | None = None
) -> list[dict[str, Any]]:
    """Compute stats on a dask bag of entities output content-items.

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

    count_df = (
        s3_entities.map(
            lambda ci: {
                "media_alias": (
                    ci["id"].split("-")[0] if "id" in ci else ci["ci_id"].split("-")[0]
                ),
                "year": (ci["id"].split("-")[1] if "id" in ci else ci["ci_id"].split("-")[1]),
                "issues": (
                    "-".join(ci["id"].split("-")[:-1])
                    if "id" in ci
                    else "-".join(ci["ci_id"].split("-")[:-1])
                ),
                "content_items_out": 1,
                "ne_mentions": len(ci.get("nes", [])),
                "ne_entities": sorted(
                    list(
                        set(
                            [
                                m["wkd_id"]
                                for m in ci.get("nes", [])
                                if "wkd_id" in m and m["wkd_id"] not in ["NIL", None]
                            ]
                        )
                    )
                ),  # sorted list to ensure all are the same
            }
        ).to_dataframe(
            meta={
                "media_alias": str,
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
        lambda x: x if isinstance(x, list) else [x], meta=("ne_entities", "object")
    )
    count_df = count_df.explode("ne_entities").persist()

    # cum the counts for all values collected
    aggregated_df = (
        count_df.groupby(by=["media_alias", "year"])
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

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    try:
        test = aggregated_df.head()
    except Exception as e:
        msg = f"{title} - Warning! the aggregated_df was empty!! {e}"
        print(msg)
        logger.warning(msg)
        return {}

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


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

    def freq(x, col="lang_fd"):
        x[col] = dict(Counter(literal_eval(x[col])))
        return x

    count_df = (
        s3_langident.map(
            lambda ci: {
                "media_alias": ci["id"].split("-")[0],
                "year": ci["id"].split("-")[1],
                "issues": "-".join(ci["id"].split("-")[:-1]),
                "content_items_out": 1,
                "images": 1 if ci["tp"] == "img" else 0,
                "lang_fd": "None" if ci["lg"] is None else ci["lg"],
            }
        )
        .to_dataframe(
            meta={
                "media_alias": str,
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
        count_df.groupby(by=["media_alias", "year"])
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

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(agg_bag)

    return agg_bag.compute()


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

    count_df = (
        s3_tr_passages.map(
            lambda passage: {
                "media_alias": passage["ci_id"].split("-")[0],
                "year": passage["ci_id"].split("-")[1],
                "issues": "-".join(passage["ci_id"].split("-")[:-1]),
                "content_items_out": passage["ci_id"],
                "text_reuse_passages": 1,
                "text_reuse_clusters": passage["cluster_id"],
            }
        )
        .to_dataframe(
            meta={
                "media_alias": str,
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
        count_df.groupby(by=["media_alias", "year"])
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

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


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

    def flatten_lists(list_elem):
        final_list = []
        for str_list in list_elem:
            assert isinstance(
                str_list, str
            ), "Inside topic aggregator flatten_list, and provided list is not str!"
            if str_list == "[]":
                final_list.append("no-topic")
            else:
                for elem in literal_eval(str_list):
                    final_list.append(elem)

        return final_list

    def freq(x, col="topics_fd"):
        if col in x:
            x[col] = dict(Counter(literal_eval(x[col])))
        return x

    try:
        test = s3_topics.take(1, npartitions=-1)
    except Exception as e:
        msg = f"Warning! the contents of the topics files were empty!! {e}"
        print(msg)
        logger.warning(msg)
        return {}

    count_df = s3_topics.map(
        lambda ci: {
            "media_alias": ci["ci_id"].split("-")[0],
            "year": ci["ci_id"].split("-")[1],
            "issues": ci["ci_id"].split("-i")[0],
            "content_items_out": 1,
            "topics": sorted(
                [t["t"] for t in ci["topics"] if "t" in t]
            ),  # sorted list to ensure all are the same
        }
    ).to_dataframe(
        meta={
            "media_alias": str,
            "year": str,
            "issues": str,
            "content_items_out": int,
            "topics": object,
        }
    )

    count_df["topics"] = count_df["topics"].apply(
        lambda x: x if isinstance(x, list) else [x], meta=("topics", "object")
    )

    # cum the counts for all values collected
    aggregated_df = (
        count_df.explode("topics")
        .groupby(by=["media_alias", "year"])
        .agg({"issues": tunique, "content_items_out": sum, "topics": [tunique, list]})
    )

    aggregated_df.columns = aggregated_df.columns.to_flat_index()
    aggregated_df = (
        aggregated_df.reset_index()
        .rename(
            columns={
                ("media_alias", ""): "media_alias",
                ("year", ""): "year",
                ("issues", "tunique"): "issues",
                ("content_items_out", "sum"): "content_items_out",
                ("topics", "tunique"): "topics",
                ("topics", "list"): "topics_fd",
            }
        )
        .sort_values("year")
    )

    aggregated_df["topics_fd"] = aggregated_df["topics_fd"].apply(
        flatten_lists, meta=("topics_fd", "object")
    )

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    try:
        test = aggregated_df.head()
    except Exception as e:
        msg = f"{title} - Warning! the aggregated_df was empty!! {e}"
        print(msg)
        logger.warning(msg)
        return {}

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").map(freq).compute()


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

    # define the list of columns in the dataframe
    count_df = (
        s3_emb_images.map(
            lambda ci: {
                "media_alias": ci["ci_id"].split("-")[0],
                "year": ci["ci_id"].split("-")[1],
                "issues": "-".join(ci["ci_id"].split("-")[:-1]),
                "content_items_out": 1,
                "images": 1,
            }
        )
        .to_dataframe(
            meta={
                "media_alias": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
                "images": int,
            }
        )
        .persist()
    )

    aggregated_df = (
        count_df.groupby(by=["media_alias", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
                "images": sum,
            }
        )
        .reset_index()
        .sort_values("year")
    ).persist()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


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

    # define the list of columns in the dataframe
    count_df = (
        s3_lingprocs.map(
            lambda ci: {
                "media_alias": ci.get("ci_id", ci.get("id")).split("-")[0],
                "year": ci.get("ci_id", ci.get("id")).split("-")[1],
                "issues": "-".join(ci.get("ci_id", ci.get("id")).split("-")[:-1]),
                "content_items_out": 1,
            }
        )
        .to_dataframe(
            meta={
                "media_alias": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
            }
        )
        .persist()
    )

    aggregated_df = (
        count_df.groupby(by=["media_alias", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
            }
        )
        .reset_index()
        .sort_values("year")
    ).persist()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


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

    # define the list of columns in the dataframe
    count_df = (
        s3_solr_ing_cis.map(
            lambda ci: {
                "media_alias": ci["id"].split("-")[0],
                "year": ci["id"].split("-")[1],
                "issues": "-".join(ci["id"].split("-")[:-1]),
                "content_items_out": 1,
                "ft_tokens": ci["content_length_i"],
            }
        )
        .to_dataframe(
            meta={
                "media_alias": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
                "ft_tokens": int,
            }
        )
        .persist()
    )

    aggregated_df = (
        count_df.groupby(by=["media_alias", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
                "ft_tokens": sum,
            }
        )
        .reset_index()
        .sort_values("year")
    ).persist()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


def compute_stats_in_ocrqa_bag(
    s3_ocrqas: Bag,
    client: Client | None = None,
    title: str | None = None,
) -> list[dict[str, int | str]]:
    """Compute stats on a dask bag of OCRQA outputs.

    Args:
        s3_solr_ing_cis (db.core.Bag): Bag with the contents of the OCRQA files.
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

    # define the list of columns in the dataframe
    count_df = (
        s3_ocrqas.map(
            lambda ci: {
                "media_alias": ci["ci_id"].split("-")[0],
                "year": ci["ci_id"].split("-")[1],
                "issues": "-".join(ci["ci_id"].split("-")[:-1]),
                "content_items_out": 1,
                "avg_ocrqa": ci["ocrqa"],
            }
        )
        .to_dataframe(
            meta={
                "media_alias": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
                "avg_ocrqa": float,
            }
        )
        .persist()
    )

    aggregated_df = (
        count_df.groupby(by=["media_alias", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
                "avg_ocrqa": "mean",
            }
        )
        .reset_index()
        .sort_values("year")
    ).persist()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    aggregated_df["avg_ocrqa"] = aggregated_df["avg_ocrqa"].apply(
        lambda x: round(x, 3), meta=("avg_ocrqa", "float")
    )

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()


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

    # define the list of columns in the dataframe
    count_df = (
        s3_doc_embeddings.map(
            lambda ci: {
                "media_alias": ci["ci_id"].split("-")[0],
                "year": ci["ci_id"].split("-")[1],
                "issues": "-".join(ci["ci_id"].split("-")[:-1]),
                "content_items_out": 1,
            }
        )
        .to_dataframe(
            meta={
                "media_alias": str,
                "year": str,
                "issues": str,
                "content_items_out": int,
            }
        )
        .persist()
    )

    aggregated_df = (
        count_df.groupby(by=["media_alias", "year"])
        .agg(
            {
                "issues": tunique,
                "content_items_out": sum,
            }
        )
        .reset_index()
        .sort_values("year")
    ).persist()

    print(f"{title} - Finished grouping and aggregating stats by title and year.")
    logger.info("%s - Finished grouping and aggregating stats by title and year.", title)

    if client is not None:
        # only add the progress bar if the client is defined
        progress(aggregated_df)

    # return as a list of dicts
    return aggregated_df.to_bag(format="dict").compute()
