"""This module contains the definition of a data statistics class.

A DataStatstics object should be instantiated during each processing step of
the data preprocessing and augmentation of the Impresso project, and used to
progressively count the number of elements modified or added by the processing.
"""

import sys
import logging
from abc import ABC, abstractmethod
from typing import Any, Union, Optional

from impresso_essentials.utils import (
    SourceMedium,
    PARTNER_TO_MEDIA,
    DataStage,
    validate_stage,
    validate_granularity,
    validate_source,
)

if sys.version < "3.11":
    from typing_extensions import Self
else:
    from typing import Self

logger = logging.getLogger(__name__)

POSSIBLE_ACTIONS = ["addition", "modification"]
POSSIBLE_GRANULARITIES = ["corpus", "title", "year"]


class DataStatistics(ABC):
    """Count statistics computed on a specific portion and granularity of the data.

    Args:
        data_stage (DataStage | str): The stage of data the stats are computed on.
        granularity (str): The granularity of the statistics with respect to the data.
        element (str, optional): The specific element associated with the statistics.
            Defaults to "" (empty string).
        counts (dict[str, int | dict[str, int]] | None, optional): Initial counts for
            statistics. Defaults to None.

    Attributes:
        stage (DataStage): The stage of data the stats are computed on.
        granularity (str): The granularity of the statistics with respect to the data.
        element (str): The specific element associated with the statistics.
        count_keys (list[str]): The count keys for these statistics.
        counts (dict[str, int | dict[str, int]]): The count statistics computed on the
            specific data, can include frequency dicts.
    """

    def __init__(
        self,
        data_stage: Union[DataStage, str],
        granularity: str,
        element: str | None = None,
        source_medium: SourceMedium | str | None = None,
        provider: str | None = None,
        counts: Union[dict[str, Union[int, dict[str, int]]], None] = None,
    ) -> None:

        self.stage = validate_stage(data_stage)
        self.granularity = validate_granularity(granularity)
        self.element = element
        # already add the provider to the DataStats if possible
        self.provider = provider if provider and provider in PARTNER_TO_MEDIA else None
        self.source_medium = (
            validate_source(source_medium, return_value_str=True) if source_medium else None
        )
        self.count_keys = self._define_count_keys()

        if counts is not None and self._validate_count_keys(counts):
            self.counts = counts
        else:
            logger.debug("Initializing counts to 0 for %s.", self.element)
            self.counts = self.init_counts()

    @abstractmethod
    def _define_count_keys(self) -> list[str]:
        """Define the count keys for these specific statistics."""

    @abstractmethod
    def _validate_count_keys(self, new_counts: dict[str, Union[int, dict[str, int]]]) -> bool:
        """Validate the keys of new counts provided against defined count keys."""

    def init_counts(self) -> dict[str, Union[int, dict[str, int]]]:
        """Initialize a dict with all the keys associated to this object.

        Returns:
            dict[str, int | dict[str, int]]: A dict with all defined keys, and values
                initialized to 0 (or to empty frequency dicts).
        """
        return {k: 0 if "fd" not in k else {} for k in self.count_keys}

    def add_counts(
        self, new_counts: dict[str, Union[int, dict[str, int]]], replace: bool = False
    ) -> bool:
        """Add new counts to the existing counts if the new keys are validated.

        Args:
            new_counts (dict[str, int | dict[str, int]]): New counts to be added.

        Returns:
            bool: True if the counts were valid and could be added, False otherwise.
        """
        if self._validate_count_keys(new_counts):
            if replace:
                logger.debug(
                    "Replacing the counts by %s for %s. This will erase previous counts.",
                    new_counts,
                    self.element,
                )
                self.counts = new_counts
            else:
                for k, v in new_counts.items():
                    if k.endswith("_fd"):
                        # some fields can be frequency dicts
                        for v_k, v_f in v.items():
                            self.counts[k][v_k] = (
                                v_f if v_k not in self.counts[k] else self.counts[k][v_k] + v_f
                            )
                    else:
                        self.counts[k] += v
            return True

        return False

    def pretty_print(
        self, modif_date: Optional[str] = None, include_counts: bool = False
    ) -> dict[str, Any]:
        """Generate a dict representation of these statistics to add to a json.

        These stats are agnostic to the type of statistics they represent so the values
        of `self.counts` are excluded by default, to be included in child classes.
        The modification date can also be included (when granularity='year')

        Args:
            modif_date (Optional[str], optional): Last modification date of the
                corresponding elements. Defaults to None.
            include_counts (bool, optional): Whether to include the current counts with
                key "stats". Defaults to False.

        Returns:
            dict[str, Any]: A dict with the general information about these statistics.
        """
        stats_dict = {
            "stage": self.stage.value,
            "granularity": self.granularity,
        }

        # no element for the overall stats
        if self.granularity != "corpus":
            if self.element is not None:
                stats_dict["element"] = self.element
            else:
                logger.warning("Missing the element when pretty-printing!")

        # If a modification date is provided, add it.
        if modif_date is not None:
            stats_dict["last_modification_date"] = modif_date
            if self.granularity != "year":
                # if the granularity is not year, log a warning (unexpected behavior)
                logger.warning(
                    "'last_modification_date' field was added although granularity is %s",
                    self.granularity,
                )

        if include_counts:
            stats_dict["media_stats"] = {
                k: (v if "fd" not in k else {v_k: v_f for v_k, v_f in v.items() if v_f > 0})
                for k, v in self.counts.items()
                if "_fd" in k or v > 0
            }

        return stats_dict

    @abstractmethod
    def same_counts(self, other_stats: Union[dict[str, Any], Self]) -> bool:
        """Given another dict of stats, check whether the values are the same."""


class MediaStatistics(DataStatistics):
    """Count statistics computed on a specific portion and granularity of the data.

    Args:
        data_stage (DataStage | str): The stage of data the stats are computed on.
        granularity (str): The granularity of the statistics with respect to the data.
        element (str, optional): The specific element associated with the statistics.
            Defaults to "" (empty string).
        counts (dict[str, int] | None, optional): Initial counts for statistics.
            Defaults to None.

    Attributes:
        stage (DataStage): The stage of data the stats are computed on.
        granularity (str): The granularity of the statistics with respect to the data.
        element (str): The specific element associated with the statistics.
        count_keys (list[str]): The count keys for these statistics.
        counts (dict[str, int]): The count statistics computed on the specific data.
    """

    # All possible count keys for newspaper data.
    possible_count_keys = [
        "titles",
        "issues",
        "pages",
        "audios",
        "content_items_out",
        "ft_tokens",
        "images",
        "ne_mentions",
        "ne_entities",
        "embeddings_el",
        "topics",
        "topics_fd",  # '_fd' suffix signifies a frenquency dict
        "lang_fd",  # '_fd' suffix signifies a frenquency dict
        "text_reuse_clusters",
        "text_reuse_passages",
        "avg_ocrqa",
    ]

    stage_extra_keys = {
        # audios and pages don't need to be defined for the same mediums
        DataStage.CANONICAL: ["pages", "audios", "images"],
        DataStage.REBUILT: ["ft_tokens"],
        DataStage.ENTITIES: ["ne_entities", "ne_mentions"],
        DataStage.NEWS_AGENCIES: ["ne_entities", "ne_mentions"],
        DataStage.PASSIM: ["ft_tokens"],
        DataStage.LANGIDENT: ["images", "lang_fd"],
        DataStage.TEXT_REUSE: ["text_reuse_clusters", "text_reuse_passages"],
        DataStage.TOPICS: ["topics", "topics_fd"],
        DataStage.MYSQL_CIS: ["pages", "audios"],
        DataStage.EMB_IMAGES: ["images"],
        DataStage.EMB_DOCS: [],  # no additional keys
        DataStage.SOLR_TEXT: ["ft_tokens"],
        DataStage.LINGPROC: [],  # no additional keys
        DataStage.OCRQA: ["avg_ocrqa"],  # no additional keys
    }

    def _define_count_keys(self) -> list[str]:
        """Define the count keys to use for these specific statistics.

        TODO correct/update the count_keys

        Returns:
            list[str]: The count keys for this specific stage and granularity.
        """
        start_index = int(self.granularity != "corpus")
        # all counts should have 'content_items_out'
        count_keys = [self.possible_count_keys[4]]
        # add 'issues' and 'titles' (only if corpus granularity)
        count_keys.extend(self.possible_count_keys[start_index:2])

        # add the stage specific additional keys when relevant
        count_keys.extend(self.stage_extra_keys[self.stage])

        # For case DataStage.SOLR_TEXT, all keys are already added.
        #   keys: 'content_items_out', 'titles', 'issues'
        # For case DataStage.LINGPROC, all keys are already added.
        #   keys: 'content_items_out', 'titles', 'issues'

        # ensure that we are intializing the counts for the right medium
        # but only if it's defined (corpus-level stats could have both)
        if self.source_medium:
            if self.source_medium == "audio" and "pages" in count_keys:
                count_keys.remove("pages")
            elif "audios" in count_keys:
                count_keys.remove("audios")

        return count_keys

    def _validate_count_keys(self, new_counts: dict[str, Union[int, dict[str, int]]]) -> bool:
        """Validate the keys of new counts provided against defined count keys.

        Valid new counts shouldn't have keys absent from the defined `attr:count_keys`
        or non-integer values.

        Args:
            new_counts (dict[str, int | dict[str, int]]): New counts to validate

        Returns:
            bool: True if `new_counts` are valid, False otherwise.
        """
        if not all(k in self.count_keys for k in new_counts.keys()):
            warn_msg = (
                f"Provided value `counts`: {new_counts} has keys not present in "
                f"`count_keys`: {self.count_keys}. The counts provided won't be used."
            )
            logger.error(warn_msg)
            return False

        if self.source_medium:
            if self.source_medium == "audio" and "pages" in new_counts:
                warn_msg = (
                    f"Source medium is '{self.source_medium}' but 'pages' counts are defined!"
                )
                logger.error(warn_msg)
                return False
            if self.source_medium != "audio" and "audios" in new_counts:
                warn_msg = (
                    f"Source medium is '{self.source_medium}' but 'audios' counts are defined!"
                )
                logger.error(warn_msg)
                return False

        if not all(
            v >= 0 if "fd" not in k else all(fv > 0 for fv in v.values())
            for k, v in new_counts.items()
        ):
            logger.error("Provided count values are not all integers and will not be used.")
            return False

        # the provided counts were conforming
        return True

    def pretty_print(
        self, modif_date: Optional[str] = None, include_counts: bool = True
    ) -> dict[str, Any]:
        """Generate a dict representation of these statistics to add to a json.

        Args:
            modif_date (Optional[str], optional): Last modification date of the
                corresponding elements. Defaults to None.
            include_counts (bool, optional): Whether to include the current media
                counts with key "media_stats". Defaults to True.

        Returns:
            dict[str, Any]: A dict representation of these statistics.
        """
        stats_dict = super().pretty_print(modif_date=modif_date)
        # add the newspaper stats
        if include_counts:
            stats_dict["media_stats"] = {
                k: (v if "_fd" not in k else {v_k: v_f for v_k, v_f in v.items() if v_f > 0})
                for k, v in self.counts.items()
                if "_fd" in k or v > 0
            }

        return stats_dict

    def same_counts(self, other_stats: Union[dict[str, Any], Self]) -> bool:
        """Given another dict of stats, check whether the values are the same.

        Args:
            other_stats (Union[dict[str, Any], Self]): Dict with pretty-printed
                media statistics or other MediaStatistics object.

        Returns:
            bool: True if the values for the various fields of `media_stats` where the
                same, False otherwise.
        """
        if isinstance(other_stats, MediaStatistics) or isinstance(other_stats, DataStatistics):
            other_stats = other_stats.pretty_print()

        self_stats = self.pretty_print()
        if "media_stats" not in other_stats:
            # older manifests would have "nps_stats" instead of "media_stats"
            return self_stats["media_stats"] == other_stats["nps_stats"]

        return self_stats["media_stats"] == other_stats["media_stats"]
