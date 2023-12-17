# PySpark ETL Project

This project involves using PySpark, preferably on Databricks, to process a publicly available resume dataset. The key objectives include loading data into Delta Tables, extracting and transforming information, and performing data validation checks.

## Dataset

The dataset used in this project can be found at [resume_corpus](https://github.com/florex/resume_corpus).

## Requirements

### 1. Load Normalized Classes into Delta Table

- **Objective**: Load normalized classes into a Delta Table named `normalized_classes` (saved as a Delta Table in Parquet format).
- **Details**:
  - Store the list of job titles in the `job_titles` column.
  - Store the job label in the `job_label` column.
  - Ensure that the `normalized_classes` table is z-ordered on the `job_label` column (if you are on Databricks).

### 2. Load Resume Corpus into Delta Table

- **Objective**: Load `resume_corpus` as resumes and their labels into a Delta Table called `resume_corpus` (saved as a Delta Table in Parquet format).
- **Details**:
  - Create a unique `resume_id`.
  - Extract job titles found in HTML span tags into `job_title`.
  - Ensure associated labels are added as `job_labels`.
  - Add `created_at` and `modified_at` fields as Timestamps.
  - Ensure that the `resume_corpus` table is partitioned on `job_labels`.

### 3. Validation Job

- **Objective**: Create a job validating that the `job_title` you have extracted actually maps to `job_labels` using `normalized_classes` and `resume_corpus` data tables. The results should be written to a third Delta Table called `resume_validation` (saved as a Delta Table in Parquet format).
- **Details**:
  - Include the `resume_id` from `resume_corpus`.
  - Include the `validation_outcome` as a Boolean to indicate the validation result.
  - Add `created_at` and `modified_at` fields as Timestamps.

## Testing and Documentation

- Both ETL processes (1 and 2 above) and the validation code (3 above) will need to have Unit Test coverage of 80 percent or above.
- Code and Unit tests must be submitted and available on Github. Do not include Parquet files or the original dataset as part of your submission. Clearly indicate DBFS or any other folder that the dataset needs to be in for reproducibility.
- We value concise and well-documented code that adheres to PySpark and Python best practices.
- **Bonus**: Present working code and passing unit tests in an unlisted Youtube video.
