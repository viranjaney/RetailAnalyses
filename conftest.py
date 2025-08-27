import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.ConfigReader import get_app_config
from lib.DataManipulation import filter_closed_orders, join_orders_customers, count_orders_state


@pytest.fixture
def spark():
    "Create a Spark session for testing"
    spark_session = get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()


@pytest.fixture
def expected_results(spark):
    "Gives the expected results dataframe"
    results_schema = "state string, count long"
    return spark.read \
        .format("csv") \
        .schema(results_schema) \
        .load("data/test_result/state_aggregate.csv")

