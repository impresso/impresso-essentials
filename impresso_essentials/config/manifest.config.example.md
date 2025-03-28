# Configuration file for Versioning Manifest generation

The script `impresso_commons/versioning/compute_manifest.py` allows to compute the versioning manifest
based on the contents of a s3 bucket (or bucket partition).
The configuration file should be used to provide all the necessary arguments to the script.
Below is an example for the case of NE-processing, as well as a description of each parameter.

## Example

```json
# mft_config.ner.json

{
    "data_stage": "entities",
    "output_bucket": "processing-canonical-data/ner_el/v02_feb_2024",
    "input_bucket": "rebuilt-data",
    "git_repository": "/local/path/to/NER-EL/repo",
    "newspapers": [
        "DLE", "BNN"
    ],
    "temp_directory": "/local/path/to/a/temp/file",
    "previous_mft_s3_path": "",
    "is_patch": false,
    "patched_fields": [],
    "push_to_git": true,
    "relative_git_path": "ner_el/v02_feb_2024",
    "only_counting": false,
    "model_id": "",
    "run_id": "",
    "notes": "",
    "file_extensions": ".jsonl.bz2",
    "compute_altogether": false,
    "check_s3_archives": true
}
```

## Arguments

- __*data_stage*__: (required) The data stage of the data to version with the manifest. Should be a valid data stage: one of : ```"canonical", "rebuilt", "evenized-rebuilt", "entities", "embeddings", "langident", "lingproc", "orcqa", "text-reuse", "topics", "solr-ingestion-text", "solr-ingestion-entities", "solr-ingestion-emb", "mysql-ingestion"```. The exact list is temporary and subject to change based on needs.
- __*output_bucket*__: (required) The S3 bucket (*including* partition if applicable) to read the data from and upload the created manifest to. This is the bucket (partition) that is to be versioned.
- __*input_bucket*__: (optional) The S3 bucket of the data that was used as *input* to the processing step that generated the data that is to be versioned.
  - For any text-mining task, this will correspond to either a bucket with rebuilt or evenized data.
  - Can be left empty (`""` or `null`) in the case of *canonical* processing, were the input data is not on S3.
- __*git_repository*__: (required) The path to the *local* (folder of the) git repository that was used to process the data to inside `output_bucket` to be versioned. Will be used to extract the last commit on the active branch.
- __*newspapers*__: (required) List of newspaper titles to consider for  the versioning, used for filtering the files within the bucket partition.
  - If left empty, *all* files within the bucket partition will be considered.
  - If the processing can only performed on the entire corpus at once, should be left empty.
- __*temp_directory*__: (required) Temporary directory to use to clone the `impresso/impresso-data-release` git repository (to push the generated manifest).
- __*previous_mft_s3_path*__: (optional) S3 path of the *previous* manifest to use, if it is not located inside `output_bucket`.
  - Should be left empty (`""` or `null`) if a versioning manifest for this specific `data_stage` is already present inside `output_bucket`.
  - The *previous* manifest is the most recent manifest that had been generated for this specific `data_stage`, and can sometimes be in another bucket.
- __*is_patch*__: (optional) Whether the update made to the current data to be versioned in `output_bucket` was a patch.
  - An update is considered to be a patch if only some of the output's properties were modified, and that the update was not performed by a re-ingestion/recomputation from the input data (rather the modification of the existing one).
  - If set to `true`, the version of the generated manifest will reflect with a patch increment.
  - Can be left to `false` or `null` by default.
- __*patched_fields*__: (optional) In the case `is_patch=true`, should be the list of individual properties modified or added to the data present in `output_bucket`.
  - Can be left empty if `is_patch=false`.
- __*push_to_git*__: (optional) Whether to push the generated manifest to the `impresso/impresso-data-release` git repository as it's uploaded to S3.
  - Can be set to `false` or `null` during experimentation or debugging to only upload to S3 and not push to git.
- __*relative_git_path*__: (optional) Only in the case the `data_stage` is a data processing stage; relative path where to push the generated manifest in the `impresso/impresso-data-release` git repository. It's only relevant if `push_to_git=true`, and defaults to the s3 partition where the data to version is on S3. 
  - Eg. the manifest computed on data located in `s3://42-processed-data-final/topics/tm-de-all-v2024.08.29/` would be pushed by default to the git at the following path: `data-processing/topics/tm-de-all-v2024.08.29/topics_v*-*-*.json`.
- __*only_counting*__: (optional) Whether the manifest is computed only to count & version the contents of an S3 bucket (`true`, after a copy from one bucket to the next for instance) or if it directly follows a processing (`false`). 
  - When `only_counting=true`, the update information for media titles for which the statistics did not change since the last manifest will not be updated (`last_modification_date`, `update_type`, `update_level`, `updated years`, `updated_fields`, `code_git_commit`).
  - Conversely, when `only_counting=false`, they will all be updated as if the data had been modified or recomputed since the last manifest computation.
  - This parameter allows to maintain update information through manifest iterations, even if the entire contents of a bucket are taken into consideration.
  - Can be set to `false` or `null` by default.
- __*model_id*__ : (optional) Model ID, according to the Impresso internal naming conventions, if applicable.
- __*run_id*__ : (optional) Run ID, according to the Impresso internal naming conventions, if applicable.
- __*notes*__ : (optional) Note for the manifest, about the processing.
  - Can be left empty (`""` or `null`), in which case a generic note with the processed titles will be used.
- __*file_extensions*__: (required) The extension of the files to consider within `output_bucket`, *including* the first `.` (`.jsonl.bz2` instead of `jsonl.bz2`).
  - Is necessary for the `fixed_s3fs_glob` function used internally, and allows to ensure only desired files are considered (eg. if `.txt` or `.json` files are present in the bucket).
- __*compute_altogether*__: (optional) Whether the statistics should be computed on the entire dataset at once (eg. for text-reuse). If the output data to version is organized by title, should be `false`. Set to `false` by default.
- __*check_s3_archives*__: (optional) Set to True to check if there are any corrupted S3 archives before starting the computation of the manifest. This will add an overhead to the processing, so it's set to `false` by default.