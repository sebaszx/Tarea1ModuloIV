import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark_session():
    """A fixture to create a Spark Context to reuse across tests."""
    s= SparkSession.builder.appName('tarea1').master('local').getOrCreate()
    yield s
    s.stop()
