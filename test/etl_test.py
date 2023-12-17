import re
import unittest

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, ArrayType, BooleanType


def extract_job_titles_mock(html_content):
    pattern = r"<span class='hl'>(.*?)</span>"
    return re.findall(pattern, html_content)


extract_job_titles_udf_mock = udf(extract_job_titles_mock, ArrayType(StringType()))


# Both ETL (1,2 above) and the validation code (3 above) will need to have Unit Test coverage of 80 percent or above
class PySparkETLTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("PySparkETLTests") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_extract_job_titles(self):
        data = [("1", "<span class='hl'>Developer</span> <span class='hl'>Engineer</span>")]
        df = self.spark.createDataFrame(data, ["resume_id", "html_content"])
        result_df = df.withColumn("job_titles", extract_job_titles_udf_mock(col("html_content"))).drop("html_content")
        expected_data = [("1", ["Developer", "Engineer"])]
        expected_df = self.spark.createDataFrame(expected_data, ["resume_id", "job_titles"])
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_validate_job_titles(self):
        def validate_job_titles_mock(job_titles):
            job_labels_list = ['java_developer', 'business_analyst']
            concatenated_title = '_'.join(job_titles).lower().replace(' ', '_')
            return concatenated_title in job_labels_list

        validate_job_titles_udf_mock = udf(validate_job_titles_mock, BooleanType())
        resume_data = [("1", ["Java", "Developer"]),
                       ("2", ["Business", "Analyst"]),
                       ("3", ["Administrator", "Manager"])]
        resume_df = self.spark.createDataFrame(resume_data, ["resume_id", "job_titles"])
        result_df = resume_df.withColumn("validation_outcome", validate_job_titles_udf_mock(col("job_titles")))
        expected_data = [("1", ["Java", "Developer"], True),
                         ("2", ["Business", "Analyst"], True),
                         ("3", ["Administrator", "Manager"], False)]
        expected_df = self.spark.createDataFrame(expected_data, ["resume_id", "job_titles", "validation_outcome"])
        self.assertEqual(result_df.collect(), expected_df.collect())


if __name__ == '__main__':
    unittest.main()
