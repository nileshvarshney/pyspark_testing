__author__ = "nilesh varshney"

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session(request):
    spark_session = (SparkSession.builder.master("local[1]")
                        .appName("pyspark_testing").getOrCreate()
                     )
    yield spark_session


