import pyspark.sql.types as t
from pyspark.sql import DataFrame

from atc.etl import Extractor, Loader, Orchestrator, EtlBase
from atc.etl.types import dataset_group
from atc.spark import Spark


class OrdersExtractor(Extractor):
    def __init__(self):
        super().__init__(dataset_key="orders")

    def read(self) -> DataFrame:
        spark = Spark.get()
        return spark.createDataFrame(
            Spark.get().sparkContext.parallelize(
                [
                    (1, "Guitar", 50),
                    (2, "Telescope", 200),
                    (3, "Tablet", 100),
                ]
            ),
            t._parse_datatype_string(
                """
                id INTEGER,
                product STRING,
                price INTEGER
            """
            ),
        )


class PaymentsExtractor(Extractor):
    def __init__(self):
        super().__init__(dataset_key="payments")

    def read(self) -> DataFrame:
        spark = Spark.get()
        return spark.createDataFrame(
            Spark.get().sparkContext.parallelize(
                [
                    (45, 1, 50),
                    (46, 2, 200),
                    (47, 3, 150),
                ]
            ),
            t._parse_datatype_string(
                """
                id INTEGER,
                order_id INTEGER,
                charged_amount INTEGER
            """
            ),
        )


class ReconcilingTransformer(EtlBase):
    def etl(self, inputs: dataset_group) -> dataset_group:
        orders = inputs["orders"]
        payments = inputs["payments"]

        df = orders.join(payments, orders.id == payments.order_id, "left").select(
            orders.id.alias("order_id"),
            payments.id.alias("payment_id"),
            orders.product,
            orders.price,
            payments.charged_amount,
        )

        dispatch = df.filter(df.price == df.charged_amount)
        service_follow_up = df.filter(~(df.price == df.charged_amount))

        return {"dispatch": dispatch, "service_follow_up": service_follow_up}


class DispatchLoader(Loader):
    def __init__(self):
        super().__init__("dispatch")

    def save(self, df: DataFrame) -> None:
        assert df.count() == 2
        print("Orders ready to dispatch:")
        df.show()


class CustomerServiceLoader(Loader):
    def __init__(self):
        super().__init__("service_follow_up")

    def save(self, df: DataFrame) -> None:
        assert df.count() == 1
        print("Orders that need follow-up:")
        df.show()


print("ETL Orchestrator using multiple different loaders")
etl = (
    Orchestrator()
    .extract_from(OrdersExtractor())
    .extract_from(PaymentsExtractor())
    .transform_with(ReconcilingTransformer())
    .load_into(DispatchLoader())
    .load_into(CustomerServiceLoader())
)
result = etl.execute()
