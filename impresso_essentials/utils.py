#!/usr/bin/env python
# coding: utf-8

from collections import namedtuple
import sys
import json
import os
import logging
import pathlib
import time
from typing import Any, Generator, Optional
from datetime import timedelta, date
import signal
from contextlib import ExitStack, contextmanager
import jsonschema
import importlib_resources
import numpy as np

from dask.bag.core import Bag
from dask.diagnostics import ProgressBar

if sys.version < "3.11":
    # python 3.10
    from typing_extensions import Self
    from strenum import StrEnum
else:
    from typing import Self
    from enum import StrEnum

logger = logging.getLogger(__name__)

POSSIBLE_GRANULARITIES = ["corpus", "title", "year"]

# a simple data structure to represent input directories
# a `Document.zip` file is expected to be found in `IssueDir.path`
# TODO add src type and medium to all issueDirs?
# IssueDir = namedtuple("IssueDir", ["provider", "alias", "date", "edition", "path", "src_type", "src_medium"])
IssueDir = namedtuple("IssueDir", ["provider", "alias", "date", "edition", "path"])


class DataStage(StrEnum):
    """Enum all stages requiring a versioning manifest.

    Each member corresponds to a data stage and the associated string is used to name
    each generated manifest accordingly.
    """

    CANONICAL = "canonical"
    REBUILT = "rebuilt"
    PASSIM = "passim"
    EMB_WORDS = "emb-words"
    EMB_SENTS = "emb-sents"
    EMB_DOCS = "emb-docs"
    EMB_ENTITIES = "emb-entities"
    EMB_PARAGRAPHS = "emb-paragraphs"
    EMB_IMAGES = "emb-images"
    # EMBED_ARTICLES = "embeddings-article"
    ENTITIES = "entities"
    NEWS_AGENCIES = "newsagencies"
    LANGIDENT = "langident"
    LINGPROC = "lingproc"
    OCRQA = "ocrqa"
    TEXT_REUSE = "textreuse"
    TOPICS = "topics"
    SOLR_TEXT = "solr-text-ingestion"
    MYSQL_CIS = "mysql-ingestion"

    @classmethod
    def has_value(cls: Self, value: str) -> bool:
        """Check if enum contains given value

        Args:
            cls (Self): This DataStage class
            value (str): Value to check

        Returns:
            bool: True if the value provided is in this enum's values, False otherwise.
        """
        return value in cls._value2member_map_


class SourceType(StrEnum):
    """Enum all types of media sources in Impresso."""

    NP = "newspaper"
    # includes radio bulletins, radio audio broadcasts, and radio tapuscripts
    RB = "radio_broadcast"
    RM = "radio_magazine"
    RS = "radio_schedule"
    MG = "monograph"
    EC = "encyclopedia"

    @classmethod
    def has_value(cls: Self, value: str) -> bool:
        """Check if enum contains given value

        Args:
            cls (Self): This source type
            value (str): Value to check

        Returns:
            bool: True if the value provided is in this enum's values, False otherwise.
        """
        return value in cls._value2member_map_


class SourceMedium(StrEnum):
    """Enum all mediums of media sources in Impresso."""

    PT = "print"
    TPS = "typescript"
    AO = "audio"

    @classmethod
    def has_value(cls: Self, value: str) -> bool:
        """Check if enum contains given value

        Args:
            cls (Self): This source medium
            value (str): Value to check

        Returns:
            bool: True if the value provided is in this enum's values, False otherwise.
        """
        return value in cls._value2member_map_


########################################
###### ALIAS TO PROVIDER MAPPINGS ######

# changed to dict to include the partner/data origin
PARTNER_TO_MEDIA = {
    "SNL": [
        "BDC",
        "CDV",
        "DLE",
        "EDA",
        "EXP",
        "IMP",
        "JDF",
        "JDV",
        "LBP",
        "LCE",
        "LCG",
        "LCR",
        "LCS",
        "LES",
        "LNF",
        "LSE",
        "LSR",
        "LTF",
        "LVE",
        "EVT",
        "BLB",
        "BNN",
        "DFS",
        "DVF",
        "EZR",
        "FZG",
        "HRV",
        "LAB",
        "LLE",
        "MGS",
        "NTS",
        "NZG",
        "SGZ",
        "SRT",
        "WHD",
        "ZBT",
        "CON",
        "DTT",
        "FCT",
        "GAV",
        "GAZ",
        "LLS",
        "OIZ",
        "SAX",
        "SDT",
        "SMZ",
        "VDR",
        "VHT",
    ],
    "LeTemps": ["JDG", "GDL"],
    "NZZ": ["NZZ"],
    "SWA": ["arbeitgeber", "handelsztg"],
    "FedGaz": ["FedGazDe", "FedGazFr"],
    "BNL": [
        "actionfem",
        "armeteufel",
        "avenirgdl",
        "buergerbeamten",
        "courriergdl",
        "deletz1893",
        "demitock",
        "diekwochen",
        "dunioun",
        "gazgrdlux",
        "indeplux",
        "kommmit",
        "landwortbild",
        "lunion",
        "luxembourg1935",
        "luxland",
        "luxwort",
        "luxzeit1844",
        "luxzeit1858",
        "obermosel",
        "onsjongen",
        "schmiede",
        "tageblatt",
        "volkfreu1869",
        "waechtersauer",
        "waeschfra",
    ],
    "BNF": [
        "excelsior",
        "lafronde",
        "marieclaire",
        "oeuvre",
        "jdpl",
        "legaulois",
        "lematin",
        "lepji",
        "lepetitparisien",
        "oecaen",
        "oerennes",],
    # TODO add new titles
    "BCUL": [
        "ACI",
        "Castigat",
        "CL",
        "Croquis",
        "FAMDE",
        "FAN",
        # "feuilleP",  # (no OCR)
        # "feuillePMA",  # (no OCR)
        "GAVi",
        "AV",
        "JY2",
        "JV",
        "JVE",
        "JH",
        "OBS",
        "Bombe",
        "Cancoire",
        "Fronde",
        "Griffe",
        "Guepe1851",
        "Guepe1887",
        "RLA",
        "Charivari",
        "CharivariCH",
        "Grelot",
        "Moniteur",
        # "Moustique",  # (no OCR)
        "ouistiti",
        # "PDN",  # (no OCR)
        "PDL",
        "PJ",
        "TouSuIl",
        "VVS1",
        "MESSAGER",
        "PS",
        "NV",
        "ME",
        "MB",
        "NS",
        # "RN",  # (no OCR)
        "FAM",
        "FAV1",
        "EM",
        "esta",
        "PAT",
        "VVS",
        "NV1",
        "NV2",
        # "RN1",  # (no OCR)
        # "RN2",  # (no OCR)
    ],
    "BL": [
        "ANJO",
        "AHEC",
        "BLWJ",
        "BNER",
        "BGJO",
        "BTEP",
        "BFNP",
        "BELL",
        "BPDH",
        "WOJL",
        "BPHF",
        "BDPO",
        "BWNW",
        "BROR",
        "BGCH",
        "BQGA",
        "BBLT",
        "BRIF",
        "BRGA",
        "BRPT",
        "BRAD",
        "BRMW",
        "BRMG",
        "CNMR",
        "CKTC",
        "CPAD",
        "CHOR",
        "CHTI",
        "CLTP",
        "CWPG",
        "CBEP",
        "CWPR",
        "CLNW",
        "CMSN",
        "CHSO",
        "DGMH",
        "DNLN",
        "DPLT",
        "DRHE",
        "DHEX",
        "DYMR",
        "DCEA",
        "DJWN",
        "DDIS",
        "DUCR",
        "ELAD",
        "EAWN",
        "TEFP",
        "FFPR",
        "FBFC",
        "FWEX",
        "FONU",
        "FMNW",
        "FRJO",
        "GLCO",
        "GWHD",
        "GLSE",
        "GOTM",
        "HLCM",
        "HPTE",
        "HAGZ",
        "HTWD",
        "HLLN",
        "HWCH",
        "HOUR",
        "HUCE",
        "HLPA",
        "ICPG",
        "ILOL",
        "IMNW",
        "ISNT",
        "ILT53",
        "ILWT",
        "IREX",
        "IWOR",
        "ISTM",
        "JWRC",
        "JSMN",
        "KEAD",
        "LSCA",
        "LSIR",
        "LECH",
        "LVMR",
        "LSGA",
        "LWC",
        "LCPP",
        "LINP",
        "LPNGA",
        "LNDH",
        "LHPN",
        "LJGA",
        "LNLF",
        "LMNA",
        "LRNW",
        "MRTM",
        "MRTT",
        "MCLN",
        "MRHD",
        "MOPT",
        "NSCS",
        "NTRG",
        "NCCO",
        "NCGA",
        "NWTM",
        "NCEF",
        "NLRD",
        "NRWC",
        "NREC",
        "NRLR",
        "NRSR",
        "NWGZ",
        "OLEN",
        "OKJL",
        "JOJL",
        "PSEV",
        "PNPC",
        "PSHE",
        "PICT",
        "PITM",
        "PELL",
        "POTG",
        "PRPL",
        "RDNP",
        "RIOB",
        "COGE",
        "RYRK",
        "RUEX",
        "SWRJ",
        "SHPA",
        "SLAD",
        "SLTL",
        "SNSR",
        "SWME",
        "SPRT",
        "STEX",
        "SHSD",
        "STUE",
        "SUGA",
        "SUNW",
        "SMSD",
        "SHCA",
        "SURY",
        "SJWL",
        "SGHL",
        "TONI",
        "TALN",
        "AGE52",
        "AATA",
        "ALBN",
        "ALST",
        "AGMO",
        "ARGB",
        "ANWT",
        "AUBO",
        "BLOT",
        "BHFA",
        "BHCH",
        "BCE1",
        "BCL2",
        "BEHI",
        "BNWL",
        "BKNW",
        "BLSD",
        "BLHD",
        "BWTE",
        "BGFP",
        "BLMY",
        "BRBN",
        "BREM",
        "BREN",
        "BRLB",
        "BRLU",
        "BRNP",
        "BRPR",
        "BRST",
        "BRSS",
        "BRTB",
        "BNPT",
        "CCEX",
        "CSTT",
        "CGGA",
        "CHPN",
        "CHPL",
        "CHTR",
        "CHTT",
        "CICN",
        "CMGA",
        "CLDF",
        "CCWA",
        "CMCH",
        "CNSN",
        "CSMP",
        "CFTM",
        "COUR",
        "CGFG",
        "CCGZ",
        "CRWN",
        "DDEN",
        "TDAY",
        "DCWR",
        "TDIA",
        "DSNR",
        "ERTG",
        "EAST",
        "ECLA",
        "ECWP",
        "ENGL",
        "ERLN",
        "ESSD",
        "EVST",
        "EVTL",
        "EVT25",
        "EXLN",
        "EXPR",
        "FODE",
        "GEVP",
        "GLCH",
        "GCLN",
        "HMSA",
        "SOHD",
        "HBOV",
        "HOWL",
        "ILNP",
        "HPNW",
        "IWGZ",
        "ISWA",
        "IPJO",
        "IMTS",
        "KTGA",
        "LNPT",
        "LOPA",
        "LAGER",
        "LHTC",
        "LEMR",
        "LTIM",
        "LIAL",
        "LIVC",
        "LITG",
        "LCHH",
        "LNCH",
        "LCCR",
        "LDGS",
        "LEVP",
        "LFPR",
        "LIWL",
        "LJPN",
        "LNM1",
        "LNM2",
        "LNM3",
        "LONM",
        "LNPC",
        "LPNL",
        "LOPH",
        "LSCT",
        "LTLG",
        "LWI",
        "LLAD",
        "LSCR",
        "MATN",
        "MEXA",
        "MTPN",
        "MEWT",
        "MNTM",
        "MOGA",
        "MOMA",
        "NATN",
        "NTNL",
        "NTPR",
        "NTSD",
        "NGLB",
        "NWTS",
        "NECT",
        "TNEW",
        "NCRF",
        "NLON",
        "NWLT",
        "NDTM",
        "NOGU",
        "NOGN",
        "NUNT",
        "OBTM",
        "ODFW",
        "OPTE",
        "ORDA",
        "PADV",
        "PMGZ",
        "PLDM",
        "PATR",
        "PHCW",
        "PPLP",
        "PLTO",
        "PWRM",
        "PLNT",
        "POLL",
        "PLOB",
        "PDHD",
        "PMGU",
        "PORC",
        "POEX",
        "TPRS",
        "PNCH",
        "PUCA",
        "RADL",
        "RBLD",
        "REFM",
        "REPR",
        "SJCH",
        "SATR",
        "SHIN",
        "SHRE",
        "SGCV",
        "SSEX",
        "SHEP",
        "SDLN",
        "SOFR",
        "STGY",
        "SESD",
        "TSUN",
        "SCPR",
        "SEGL",
        "SMHE",
        "SSCH",
        "TMEW",
        "TIGA",
        "TNAJ",
        "THML",
        "TFPR",
        "TRBT",
        "TRSN",
        "TUNI",
        "UNIV",
        "VERL",
        "VIND",
        "WAEX",
        "WAHD",
        "WTCH",
        "WKNW",
        "WKAD",
        "WKCH",
        "WKEC",
        "WKGB",
        "WKIN",
        "WKIT",
        "WKJL",
        "WKML",
        "WKRV",
        "WSBN",
        "WGMC",
        "WENW",
        "WLTM",
        "WMTM",
        "WMTG",
        "WRLD",
        "WFSC",
        "YOHD",
        "TCDN",
        "TTLK",
        "TTK22",
        "TCAA",
        "TPRD",
        "WKTN",
        "WKTS",
        "WLSA",
        "WMCF",
        "WJBS",
        "WHEP",
        "WDEX",
        "WBGZ",
        "WRWA",
        "GNDL",
        "GLAD",
        "YOHP",
    ],
    "SWISSINFO": [
        "SOC_CJ",
        "SOC_CP",
        "SOC_SO",
        "SOC_TH",
        "SOC_VS",
    ],
    "INA": ["CFCE", "RDN"],
}

PARTNER_TO_COUNTRY = {
    "SNL": "CH",
    "LeTemps": "CH",
    "NZZ": "CH",
    "SWA": "CH",
    "FedGaz": "CH",
    "BNL": "LU",
    "BNF": "FR",
    "BCUL": "CH",
    "BL": "UK",
    "KB": "NL",
    "SWISSINFO": "CH",
    "RTS": "CH",
    "INA": "FR",
}
# flatten the known journals into a sorted list
ALL_MEDIA = sorted([j for part_j in PARTNER_TO_MEDIA.values() for j in part_j])
MEDIA_TO_COUNTRY = {
    alias: PARTNER_TO_COUNTRY[p] for p, p_aliases in PARTNER_TO_MEDIA.items() for alias in p_aliases
}
PARTNERS_WITHOUT_OLR = ["NZZ", "SWA", "FedGaz", "BCUL", "SWISSINFO", "INA"]

# values can either be a list of aliases or "all"
SOURCE_MEDIUMS_TO_PARTNERS_TO_MEDIA = {
    SourceMedium.PT: {
        "SNL": "all",
        "LeTemps": "all",
        "NZZ": "all",
        "SWA": "all",
        "FedGaz": "all",
        "BNL": "all",
        "BNF": "all",
        "BCUL": "all",
        "BL": "all",
        # "KB": {# SourceMedium.PT: [], # all KB NP titles should be listed},
    },
    SourceMedium.TPS: {
        "KB": ["ANP"],
        "SWISSINFO": "all",
    },
    SourceMedium.AO: {"RTS": "all", "INA": "all"},  # "RTS": [RTS_titles],
}
# define list of media aliases that are not print (newspapers)
NON_PRINT_MEDIA = {
    SourceMedium.TPS: ["SOC_CJ", "SOC_CP", "SOC_SO", "SOC_TH", "SOC_VS", "ANP"],
    # SourceMedium.AO: #[INA_titles],
}
PARTNERS_TO_SRC_MEDIUM_TO_MEDIA = {
    "SNL": {SourceMedium.PT: "all"},
    "LeTemps": {SourceMedium.PT: "all"},
    "NZZ": {SourceMedium.PT: "all"},
    "SWA": {SourceMedium.PT: "all"},
    "FedGaz": {SourceMedium.PT: "all"},
    "BNL": {SourceMedium.PT: "all"},
    "BNF": {SourceMedium.PT: "all"},
    "BCUL": {SourceMedium.PT: "all"},
    "BL": {SourceMedium.PT: "all"},
    "KB": {
        SourceMedium.TPS: ["ANP"],
        # SourceMedium.PT: [], # all KB NP titles should be listed
    },
    "SWISSINFO": {SourceMedium.TPS: "all"},
    "RTS": {SourceMedium.AO: "all"},
    "INA": {SourceMedium.AO: "all"},
}
PARTNERS_TO_SRC_TYPE_TO_MEDIA = {
    "SNL": {SourceType.NP: "all"},
    "LeTemps": {SourceType.NP: "all"},
    "NZZ": {SourceType.NP: "all"},
    "SWA": {SourceType.NP: "all"},
    "FedGaz": {SourceType.NP: "all"},
    "BNL": {SourceType.NP: "all"},
    "BNF": {SourceType.NP: "all"},
    "BCUL": {SourceType.NP: "all"},
    "BL": {SourceType.NP: "all"},
    "KB": {
        SourceType.RB: ["ANP"],
        # SourceType.NP: [], # all KB NP titles should be listed
    },
    "SWISSINFO": {SourceType.RB: "all"},
    "RTS": {SourceType.RB: "all"},
    "INA": {SourceType.RB: "all"},
}


def get_provider_for_alias(media_alias: str) -> str:
    """Get the provider for a given media alias.

    Args:
        media_alias (str): The media alias to get the provider for.

    Returns:
        str: The provider for the given media alias.
    """
    for provider, aliases in PARTNER_TO_MEDIA.items():
        if media_alias in aliases:
            return provider

    raise ValueError(
        f"Unknown media alias: {media_alias}! Please check the list of known media aliases."
    )


def get_src_info_for_alias(
    media_alias: str, provider: str | None = None, medium: bool | None = True
) -> str:
    """Get the provider for a given media alias.

    Args:
        media_alias (str): The media alias to get the provider for.
        provider (str | None, optional): The provider for the media. If None, the
            provider will be determined from the media alias. Defaults to None.

    Returns:
        str: The source medium for the given media alias.
    """
    if not provider:
        provider = get_provider_for_alias(media_alias)

    info_dict = PARTNERS_TO_SRC_MEDIUM_TO_MEDIA if medium else PARTNERS_TO_SRC_TYPE_TO_MEDIA

    for medium, aliases in info_dict[provider].items():
        # aliases can be a list of aliases or "all"
        if aliases == "all":
            return medium
        if media_alias in aliases:
            return medium

    raise ValueError(
        f"Could not find the source {'medium' if medium else 'type'} for {media_alias}!"
    )


##################################
###### VALIDATION FUNCTIONS ######


def validate_stage(data_stage: str, return_value_str: bool = False) -> DataStage | str | None:
    """Validate the provided data stage if it's in the DataStage Enum (key or value).

    Args:
        data_stage (str): Data stage key or value to validate.
        return_value_str (bool, optional): Whether to return the data stage's value if
            it was valid. Defaults to False.

    Raises:
        e: The provided str is neither a data stage key nor value.

    Returns:
        DataStage | str | None: The corresponding DataStage or value string if valid.
    """
    try:
        if DataStage.has_value(data_stage):
            stage = DataStage(data_stage)
        else:
            stage = DataStage[data_stage]
        return stage.value if return_value_str else stage
    except ValueError as e:
        err_msg = f"{e} \nProvided data stage '{data_stage}' is not a valid data stage."
        logger.critical(err_msg)
        raise e


def validate_granularity(value: str) -> Optional[str]:
    """Validate that the granularity value provided is valid.

    Statistics are computed on three granularity levels:
    corpus, title and year.
    TODO: add provider?

    Args:
        value (str): Granularity value to validate

    Raises:
        ValueError: The provided granularity isn't one of corpus, title and year.

    Returns:
        Optional[str]: The provided value, in lower case, or None if not valid.
    """
    lower = value.lower()
    if lower in POSSIBLE_GRANULARITIES:
        return lower
    # only incorrect granularity values will not be returned
    logger.critical("Provided granularity '%s' isn't valid.", lower)
    raise ValueError


def validate_source(
    source: str, return_value_str: bool = False, medium: bool = True
) -> SourceType | SourceMedium | str | None:
    """Validate the provided source type if it's in the SourceType Enum (key or value).

    Args:
        source (str): Source type or medium key or value to validate.
        return_value_str (bool, optional): Whether to return the source type or medium's value if
            it was valid. Defaults to False.
        medium (bool, optional): Whether to validate a source medium (True) or key (False).
            Defaults to True.

    Raises:
        e: The provided str is neither a source type key nor value.

    Returns:
        SourceType | str | None | SourceMedium: The SourceType or value string if valid.
    """
    src_class = SourceMedium if medium else SourceType
    try:
        if src_class.has_value(source):
            src = src_class(source)
        else:
            src = src_class[source]
        return src.value if return_value_str else src
    except ValueError as e:
        word = "medium" if medium else "type"
        err_msg = f"{e} \nProvided source {word} '{source}' is not a valid source {word}."
        logger.critical(err_msg)
        raise e


def validate_against_schema(
    json_to_validate: dict[str, Any],
    path_to_schema: str = "schemas/json/versioning/manifest.schema.json",
) -> None:
    """Validate a dict corresponding to a JSON against a provided JSON schema.

    Args:
        json (dict[str, Any]): JSON data to validate against a schema.
        path_to_schema (str, optional): Path to the JSON schema to validate against.
            Defaults to "impresso-schemas/json/versioning/manifest.schema.json".

    Raises:
        e: The provided JSON could not be validated against the provided schema.
    """
    file_manager = ExitStack()
    schema_path = get_pkg_resource(file_manager, path_to_schema)
    with open(os.path.join(schema_path), "r", encoding="utf-8") as f:
        json_schema = json.load(f)

    try:
        jsonschema.validate(json_to_validate, json_schema)
    except jsonschema.ValidationError as e:
        logger.error(
            "The provided JSON could not be validated against its schema: %s.",
            json_to_validate,
        )
        raise e


def user_confirmation(question: str, default: str | None = None) -> bool:
    """Ask a yes/no question via raw_input() and return their answer.

    Args:
        question (str): String question presented to the user.
        default (str | None, optional): Presumed answer if the user just hits <Enter>.
            Should be one of "yes", "no" and None. Defaults to None.

    Raises:
        ValueError: The default value provided is not valid.

    Returns:
        bool: User's answer to the asked question.
    """
    valid = {"yes": True, "y": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        err_msg = f"Invalid default answer: '{default}'"
        raise ValueError(err_msg)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        if choice in valid:
            return valid[choice]
        sys.stdout.write("Please respond with 'yes' or 'no' (or 'y' or 'n').\n")


def user_question(variable_to_confirm: str) -> None:
    """Ask the user if the identified variable is correct.

    Args:
        variable_to_confirm (str): Variable to be checked by the user.
    """
    answer = user_confirmation(
        f"\tIs the following the correct item to work with?\n  {variable_to_confirm}",
        None,
    )

    if not answer:
        logger.info("Variable not confirmed, exiting.")
        sys.exit()
    else:
        logger.info("Variable confirmed.")


def timestamp(ts_format: str = "%Y-%m-%dT%H:%M:%SZ", with_space: bool = False) -> str:
    """Return an iso-formatted timestamp.

    Args:
        ts_format (str, optional): Timestamp format to use for the returned timestamp.
            Defaults to "%Y-%m-%dT%H:%M:%SZ".
        with_space (bool, optional): Format the timestamp with spaces. If True, the
            format used will be "%Y-%m-%d %H:%M:%S". Defaults to False.

    Returns:
        str: Timestamp formatted according to a provided format.
    """
    if with_space:
        ts_format = "%Y-%m-%d %H:%M:%S"
    return time.strftime(ts_format, time.gmtime())


class Timer:
    """Basic timer"""

    def __init__(self):
        self.start = time.time()
        self.intermediate = time.time()

    def tick(self) -> str:
        """Perform a tick with the timer.

        Returns:
            str: Elapsed time since last tick in seconds.
        """
        elapsed_time = time.time() - self.intermediate
        self.intermediate = time.time()
        return str(timedelta(seconds=elapsed_time))

    def stop(self) -> str:
        """Stop the timer.

        Returns:
            str: Elapsed time since the start tick in seconds.
        """
        elapsed_time = time.time() - self.start
        return str(timedelta(seconds=elapsed_time))


def chunk(l_to_chunk: list, chunksize: int) -> Generator:
    """Yield successive n-sized chunks from list.

    Args:
        l_to_chunk (list): List to chunk down.
        chunksize (int): Size of each chunk.

    Yields:
        Generator: Each chunk of the list.
    """
    for i in range(0, len(l_to_chunk), chunksize):
        yield l_to_chunk[i : i + chunksize]


def get_pkg_resource(
    file_manager: ExitStack, path: str, package: str = "impresso_essentials"
) -> pathlib.PosixPath:
    """Return the resource at `path` in `package`, using a context manager.

    Note:
        The context manager `file_manager` needs to be instantiated prior to
        calling this function and should be closed once the package resource
        is no longer of use.

    Args:
        file_manager (contextlib.ExitStack): Context manager.
        path (str): Path to the desired resource in given package.
        package (str, optional): Package name. Defaults to "impresso_essentials".

    Returns:
        pathlib.PosixPath: Path to desired managed resource.
    """
    ref = importlib_resources.files(package) / path
    return file_manager.enter_context(importlib_resources.as_file(ref))


def init_logger(
    _logger: logging.RootLogger, level: int = logging.INFO, file: Optional[str] = None
) -> logging.RootLogger:
    """Initialises the root logger.

    Args:
        _logger (logging.RootLogger): Logger instance to initialise.
        level (int, optional): desired level of logging. Defaults to logging.INFO.
        file (str | None, optional): _description_. Defaults to None.

    Returns:
        logging.RootLogger: the initialised logger
    """
    # Initialise the logger
    _logger.setLevel(level)

    if file is not None:
        handler = logging.FileHandler(filename=file, mode="w")
    else:
        handler = logging.StreamHandler()

    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    _logger.addHandler(handler)
    _logger.info("Logger successfully initialised")

    return _logger


def bytes_to(bytes_nb: int, to_unit: str, bsize: int = 1024) -> float:
    """Convert bytes to the specified unit.

    Supported target units:
    - 'k' (kilobytes), 'm' (megabytes),
    - 'g' (gigabytes), 't' (terabytes),
    - 'p' (petabytes), 'e' (exabytes).

    Args:
        bytes_nb (int): The number of bytes to be converted.
        to_unit (str): The target unit for conversion.
        bsize (int, optional): The base size used for conversion (default is 1024).

    Returns:
        float: The converted value in the specified unit.

    Raises:
        KeyError: If the specified target unit is not supported.
    """
    units = {"k": 1, "m": 2, "g": 3, "t": 4, "p": 5, "e": 6}
    return float(bytes_nb) / (bsize ** units[to_unit])


def get_list_intersection(list1: list, list2: list) -> list:
    """Compute the intersection between two lists.

    Args:
        list1 (list): First list to intersect.
        list2 (list): First list to intersect.

    Returns:
        list: List of intersection of both arguments.
    """
    return list(set(list1).intersection(list2))


def partitioner(bag: Bag, path: str, nb_partitions: int) -> None:
    """
    Partition a Dask bag into n partitions and write each to a separate file.

    Args:
        bag (dask.bag.core.Bag): The Dask bag to be partitioned.
        path (str): Directory path where partitioned files will be saved.
        nb_partitions (int): Number of partitions to create.

    Returns:
        None: The function writes partitioned files to the specified path.
    """
    grouped_items = bag.groupby(lambda x: np.random.randint(500), npartitions=nb_partitions)
    items = grouped_items.map(lambda x: x[1]).flatten()
    path = os.path.join(path, "*.jsonl.bz2")
    with ProgressBar():
        items.to_textfiles(path)


def id_to_issuedir(canonical_id: str, issue_path: str, provider: str | None = None) -> IssueDir:
    """Instantiate an IssueDir object from a canonical ID and the path to the issue.

    Args:
        canonical_id (str): Canonical ID of the issue.
        issue_path (str): Local path to the issue files.
        provider (str | None, optional): Provider associated to that alias. Defaults to None,
            if not provided, will be deduced from the alias (slight overhead).

    Returns:
        IssueDir: IssueDir instance for the object
    """
    alias, year, month, day, edition = canonical_id.split("-")
    year = int(year)
    month = int(month)
    day = int(day)

    if not provider:
        provider = get_provider_for_alias(alias)

    return IssueDir(provider, alias, date(year, month, day), edition, issue_path)
    # return IssueDir(alias, date(year, month, day), edition, issue_path)


@contextmanager
def disable_interrupts():
    """Context manager to temporarily disable keyboard interrupts."""
    original_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, original_handler)
