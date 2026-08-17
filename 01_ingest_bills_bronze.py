# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Landing new bill files
# MAGIC This step does one simple job: notice when a new PDF lands in the
# MAGIC Volume, and write down what we know about it — the file path, which
# MAGIC vendor folder it came from, and when we first saw it. That's all.
# MAGIC
# MAGIC It deliberately does **not** open or parse the PDF yet. Reading the
# MAGIC actual bill content happens one step later, in Silver. Keeping this
# MAGIC step "dumb" means it's fast, cheap, and never breaks just because one
# MAGIC PDF happens to be malformed — a broken file still lands here fine,
# MAGIC and gets flagged later during parsing instead of stalling discovery.

# COMMAND ----------
from pyspark.sql import functions as F, DataFrame

# COMMAND ----------
# MAGIC %md ### Parameters

# COMMAND ----------
dbutils.widgets.text("source_path", "/Volumes/bills/raw/incoming_bills/")
dbutils.widgets.text("bronze_table", "bills.bronze.bronze_bill_files")

SOURCE_PATH     = dbutils.widgets.get("source_path")
BRONZE_TABLE    = dbutils.widgets.get("bronze_table")
CHECKPOINT_PATH = f"{SOURCE_PATH.rstrip('/')}/_checkpoints/bronze_bill_files/"
SCHEMA_LOC      = f"{CHECKPOINT_PATH}_schema/"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_TABLE.rsplit('.', 1)[0]}")

# COMMAND ----------
# MAGIC %md ### BronzeFileLandingPipeline
# MAGIC A single, small class with one job: turn a batch of newly-arrived
# MAGIC files into rows of metadata. There's only one sensible way to do this
# MAGIC job, so it stays one plain class rather than a Strategy interface —
# MAGIC that pattern earns its keep in Silver instead, where the parsing
# MAGIC technique is genuinely something we might want to swap out later.

# COMMAND ----------
class BronzeFileLandingPipeline:
    """Registers newly-arrived PDF files into a metadata-only Delta table.
    Never reads the PDF's actual content — just notices the file exists.
    """

    def __init__(self, bronze_table: str):
        self._bronze_table = bronze_table

    def register_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            print(f"[batch {batch_id}] no new files")
            return

        file_metadata_df = (
            batch_df
            .withColumn("file_name", F.element_at(F.split(F.col("path"), "/"), -1))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .select(
                F.col("path").alias("file_path"),
                "file_name",
                F.col("bill_type").alias("utility_type"),
                "vendor_id",
                F.col("length").alias("file_size_bytes"),
                "ingestion_timestamp",
            )
        )

        file_metadata_df.write.format("delta").mode("append").saveAsTable(self._bronze_table)
        print(f"[batch {batch_id}] registered {file_metadata_df.count()} new files -> {self._bronze_table}")

# COMMAND ----------
# MAGIC %md ### Run

# COMMAND ----------
pipeline = BronzeFileLandingPipeline(BRONZE_TABLE)

raw_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)
    .load(SOURCE_PATH)
)

(raw_stream.writeStream
    .foreachBatch(pipeline.register_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
    .awaitTermination())
