import re
import pytest
from impresso_essentials.io.s3_path_parser import pattern


@pytest.mark.parametrize(
    "s3_path, expected",
    [
        # First case remains the same
        (
            "s3://01-processed-data-final/entities/embeddings/entities-ner-en_core_web_sm_v3.1.0-en_v1-0-0/Reuters/UK/UK-2021.jsonl.bz2",
            {
                "bucket": "01-processed-data-final",
                "stage_number": "01",
                "phase": "final",
                "processing_label": "entities",
                "processing_subtype_label": "embeddings",
                "run_id": "entities-ner-en_core_web_sm_v3.1.0-en_v1-0-0",
                "model_id": "ner-en_core_web_sm_v3.1.0-en",
                "task": "ner",
                "subtask": None,
                "model_specificity": "en_core_web_sm",
                "model_version": "v3.1.0",
                "model_major": "3",
                "model_minor": "1",
                "model_patch": "0",
                "lang": "en",
                "run_version": "v1-0-0",
                "run_major": "1",
                "run_minor": "0",
                "run_patch": "0",
                "provider_alias": "Reuters",
                "media_alias": "UK",
                "file_stem": "UK-2021",
                "year": "2021",
            },
        ),
        (
            "s3://02-processed-data-staging/langident/langident-lid-fasttext_v1.0.0-multilingual_v2-0-1/BBC/BBC-2020-langident.jsonl.bz2",
            {
                "bucket": "02-processed-data-staging",
                "stage_number": "02",
                "phase": "staging",
                "processing_label": "langident",
                "processing_subtype_label": None,
                "run_id": "langident-lid-fasttext_v1.0.0-multilingual_v2-0-1",
                "model_id": "lid-fasttext_v1.0.0-multilingual",
                "task": "lid",
                "subtask": None,
                "model_specificity": "fasttext",
                "model_version": "v1.0.0",
                "model_major": "1",
                "model_minor": "0",
                "model_patch": "0",
                "lang": "multilingual",
                "run_version": "v2-0-1",
                "run_major": "2",
                "run_minor": "0",
                "run_patch": "1",
                "provider_alias": None,
                "media_alias": "BBC",
                "file_stem": "BBC-2020-langident",
                "year": "2020",
            },
        ),
        (
            "s3://03-processed-data-sandbox/topics/topics-tm-lda_model-en_v3-2-4/EXP/EXP-2021-topics.jsonl.bz2",
            {
                "bucket": "03-processed-data-sandbox",
                "stage_number": "03",
                "phase": "sandbox",
                "processing_label": "topics",
                "processing_subtype_label": None,
                "run_id": "topics-tm-lda_model-en_v3-2-4",
                "model_id": "tm-lda_model-en",
                "task": "tm",
                "subtask": None,
                "model_specificity": "lda_model",
                "model_version": None,
                "model_major": None,
                "model_minor": None,
                "model_patch": None,
                "lang": "en",
                "run_version": "v3-2-4",
                "run_major": "3",
                "run_minor": "2",
                "run_patch": "4",
                "provider_alias": None,
                "media_alias": "EXP",
                "file_stem": "EXP-2021-topics",
                "year": "2021",
            },
        ),
        (
            "s3://42-processed-data-final/topics/topics-tm-bert_v3.0.0-en_v3-0-0/CNN/CNN-2024-topics.jsonl.bz2",
            {
                "bucket": "42-processed-data-final",
                "stage_number": "42",
                "phase": "final",
                "processing_label": "topics",
                "processing_subtype_label": None,
                "run_id": "topics-tm-bert_v3.0.0-en_v3-0-0",
                "model_id": "tm-bert_v3.0.0-en",
                "task": "tm",
                "subtask": None,
                "model_specificity": "bert",
                "model_version": "v3.0.0",
                "model_major": "3",
                "model_minor": "0",
                "model_patch": "0",
                "lang": "en",
                "run_version": "v3-0-0",
                "run_major": "3",
                "run_minor": "0",
                "run_patch": "0",
                "provider_alias": None,
                "media_alias": "CNN",
                "file_stem": "CNN-2024-topics",
                "year": "2024",
            },
        ),
    ],
)
def test_successful_s3_path_matches(s3_path, expected):
    match = re.match(pattern, s3_path, re.VERBOSE)
    assert match is not None, f"Pattern did not match for {s3_path}"
    for key, value in expected.items():
        assert (
            match.group(key) == value
        ), f"For {key}: expected {value}, got {match.group(key)}"


def test_failed_s3_path_match():
    path = "s3://42-processed-data-final/embeddings/images/image-embeddings/embeddings-resnet_dino_clip-v0-0-1/bnl/actionfem/actionfem-1927-image-embeddings.jsonl.bz2"
    match = re.match(pattern, path, re.VERBOSE)
    assert match is None, f"Pattern should not match for {path}"
