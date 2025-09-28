from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable
import time
from random import Random

# Constants
RETRY_DELAY_MINIMUM_SECS = 30.0
RETRY_DELAY_MAXIMUM_SECS = 60.0
RANDOM = Random()

# Function to open delta table with retry
def open_delta_table(spark: SparkSession, path: str) -> DeltaTable:
    """Opens a Delta Table, retrying once to work around transient errors"""
    try:
        return DeltaTable.forPath(spark, path)
    except Exception as e:
        print(f"Delta open failed: {e}, retrying...")
        delay = RANDOM.uniform(RETRY_DELAY_MINIMUM_SECS, RETRY_DELAY_MAXIMUM_SECS)
        time.sleep(delay)
        return DeltaTable.forPath(spark, path)

# Function to enable liquid clustering
def liquid_cluster(spark: SparkSession, delta_table_path: str, delta_table_name: str, clustering_cols: list[str]) -> None:
    """Enables liquid clustering on a Delta table and optimizes it"""
    clustering_cols_str = ", ".join(clustering_cols)

    # Create/open delta table
    delta_table = open_delta_table(spark, delta_table_path)

    # Register table if not exists
    (
        DeltaTable.createIfNotExists(spark)
        .tableName(delta_table_name)
        .location(output_path)
        .addColumn("name", "STRING")
        .addColumn("date", "DATE")
        .addColumn("amount", "INT")
        .execute()
    )

    # Enable liquid clustering
    spark.sql(f"ALTER TABLE {delta_table_name} CLUSTER BY ({clustering_cols_str})")

    # Optimize table
    delta_table.optimize().executeCompaction()

# main
spark = (
    SparkSession.builder
    .appName("ClusteringDeltaLake")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Sample Data
data = [
    ("Alice", "2025-09-10", 100),
    ("Bob", "2025-09-11", 200),
    ("Charlie", "2025-09-10", 150),
    ("David", "2025-09-12", 300)
]

columns = ["name", "date", "amount"]

df = spark.createDataFrame(data, columns).withColumn("date", to_date(col("date")))
df.show()

# Table path & name
output_path = "s3://gluescriptsworkflow/output/"
table_name = "my_liqclu_table"

# To remove old table conflicts
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

df.write.format("delta").mode("overwrite").save(output_path)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    USING DELTA
    LOCATION '{output_path}'
""")

# Apply liquid clustering
liquid_cluster(spark, output_path, table_name, ["date"])