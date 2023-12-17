import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, input_file_name, regexp_extract, current_timestamp, col, split
from pyspark.sql.types import StringType, ArrayType, BooleanType

# Initialize Spark session
spark = SparkSession.builder.appName("Resume Validation and Processing.").getOrCreate()

# _________________ ETL Job 1:
# Load normalized classes into a Delta Table called normalized_classes
# Column 1 : List of job titles would go to `job_titles` column
# Column 2 : Job label would go to `job_label` column
# Condition 1 : Ensure that normalized_classes table is z-ordered on job_label column
# (if you are on Databricks) : Not using databricks - skipping
normalized_class_df = spark.read.text("resume_corpus/normalized_classes.txt") \
    .select(split("value", ":").getItem(0).alias("job_titles"),
            split("value", ":").getItem(1).alias("job_label"))

# Checkpointing and saving to local directory
normalized_class_df.toPandas().to_csv("normalized_class_df.csv", index=False)


# _________________ ETL Job 2:
# Load resume_corpus as resumes and their labels into a Delta Table called resume_corpus
# Column 1 : Create a unique `resume_id`
# Column 2 : Extract job titles found in HTML span tags to `job_title`
# Column 3 : Ensure associated labels are added as `job_labels`
# Column 4 : Ensure that created_at and `modified_at` fields are added as Timestamp
# Condition 1 : Ensure that resume_corpus table is partitioned on job_labels
def extract_job_titles(html_content):
    job_titles = re.findall(r'<span class="hl">(.*?)</span>', html_content)
    return job_titles


# create a title extraction udf, which extracts the job title array from the <span>
# Register the function as a UDF
extract_job_titles_udf = udf(extract_job_titles, ArrayType(StringType()))

# Read the resume text files into a DataFrame and apply the UDF
resume_df = spark.read.text("resume_corpus/resume/*.txt") \
    .withColumn("resume_id", regexp_extract(input_file_name(), '(\d+).txt', 1)) \
    .withColumn("job_titles", extract_job_titles_udf("value")) \
    .withColumn("created_at", current_timestamp()) \
    .withColumn("modified_at", current_timestamp()) \
    .select("resume_id", "job_titles", "created_at", "modified_at")

label_df = spark.read.text("resume_corpus/resume/*.lab") \
    .withColumn("resume_id", regexp_extract(input_file_name(), '(\d+).lab', 1)) \
    .withColumnRenamed("value", "job_label")

resume_corpus_df = resume_df.join(label_df, "resume_id")
resume_corpus_df.toPandas().to_csv("resume_corpus_df.csv", index=False)

# _________________ ETL Job 3:
# Create a job validating that job_title you have extracted actually
# maps to job_labels by using normalized_classes and resume_corpus data
# table, results written to a third Delta Table called resume_validation
# Ensure that it has the `resume_id` from resume_corpuse
# Ensure that it has the `validation_outcome` as a Boolean as to validation result
# Ensure that `created_at` and `modified_at` fields are added as Timestamp - (get current timestamp)
job_labels_list = normalized_class_df.select("job_label") \
    .distinct() \
    .rdd.flatMap(lambda x: x) \
    .map(lambda x: x.lower()) \
    .collect()

# broadcast list of job labels available in normalized class file
broadcasted_job_labels = spark.sparkContext.broadcast(job_labels_list)


# this will compare extracted job_titles from html <span> it will concatenate and match with list of job labels
# e.g. HTML extracted span tag [Java] [Developer] -> Java_Developer == from list of job_tiles in normalized class
# We can do more flexible and intelligent comparison, but doing stricter comparison.
def validate_job_titles(job_titles):
    job_label_list = broadcasted_job_labels.value
    concatenated_title = '_'.join(job_titles).lower().replace(' ', '_')
    return concatenated_title in job_label_list


# create a validation udf
validate_job_titles_udf = udf(validate_job_titles, BooleanType())
validation_df = resume_corpus_df.withColumn("validation_outcome", validate_job_titles_udf(col("job_titles")))
validation_df = validation_df.withColumn("modified_at", current_timestamp())
validation_df = validation_df.select("resume_id", "created_at", "modified_at", 'job_label', 'validation_outcome')
validation_df.show()

# Filter the DataFrame to show only rows where validation_outcome is True
true_validation_df = validation_df.filter(col("validation_outcome") == True)

# Show the filtered DataFrame
true_validation_df.show()
true_validation_df.toPandas().to_csv("validation_df.csv", index=False)
