import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.ConfigReader import get_app_config
from lib.DataManipulation import count_orders_state, filter_orders_generic

def test_read_app_config(spark):
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"


def test_read_customers_df(spark):
    customers_count = read_customers(spark, "LOCAL").count()
    assert customers_count == 12435

@pytest.mark.skip()
def test_read_orders_df(spark):
    orders_count = read_orders(spark, "LOCAL").count()
    assert orders_count == 68884

# @pytest.mark.transformations()
# def test_count_order_states(spark, expected_results):
#     customers_df = read_customers(spark, "LOCAL")
#     actual_results = count_orders_state(customers_df)
#     assert actual_results.collect() == expected_results.collect()

@pytest.mark.skip()
def test_check_closeD_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df, "CLOSED").count()
    assert filtered_count == 7556

@pytest.mark.skip()
def test_check_pending_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df, "PENDING_PAYMENT").count()
    assert filtered_count == 15030

@pytest.mark.skip()
def test_check_complete_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df, "COMPLETE").count()
    assert filtered_count == 22900

@pytest.mark.parametrize(
        "status,count",
        [
            ("CLOSED", 7556),
            ("PENDING_PAYMENT", 15030),
            ("COMPLETE", 22900),
            ("PENDING", 13000),
            ("PROCESSING", 16000),
            ("ON_HOLD", 5000),
            ("CANCELED", 3000),
            ("REFUNDED", 2000),
            ("FAILED", 1398)
        ]
)

def test_check_count(spark, status, count):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df, status).count()
    assert filtered_count == count