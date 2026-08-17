# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — Parsing bills with `ai_parse_document` (no-code OCR)
# MAGIC Reads the files Bronze registered, and for each one calls Databricks'
# MAGIC native `ai_parse_document` — a single function call that reads the
# MAGIC PDF, whether it's a clean digital document or a scanned image, and
# MAGIC hands back its text blocks, tables, and figures already structured.
# MAGIC No custom OCR code, no regex, no pdfplumber — the model does the
# MAGIC reading; we just store what it found.
# MAGIC
# MAGIC The output is stored as **one row per element** (a paragraph, a
# MAGIC table, a figure...), matching how `ai_parse_document` naturally
# MAGIC breaks a page down — not one row per bill. Turning this into one
# MAGIC clean row per bill (the actual dollar amounts and dates) is a
# MAGIC separate, later step (Gold), not this one.
# MAGIC
# MAGIC **Honest trade-off worth knowing:** the source reference this follows
# MAGIC also recommends cross-checking critical fields against a plain
# MAGIC pdfplumber text pass, since `ai_parse_document` has been observed to
# MAGIC occasionally mistranscribe names/identifiers. That cross-check is
# MAGIC intentionally left out here, since it would mean writing custom
# MAGIC parsing code — which conflicts with the "no regex / no code" goal for
# MAGIC this notebook. This is a real accuracy trade-off, not an oversight.

# COMMAND ----------
from abc import ABC, abstractmethod

from pyspark.sql import functions as F, DataFrame

# COMMAND ----------
# MAGIC %md ### Parameters

# COMMAND ----------
dbutils.widgets.text("bronze_table", "bills.bronze.bronze_bill_files")
dbutils.widgets.text("silver_table", "bills.silver.silver_bill_parsed")
dbutils.widgets.text("checkpoint_path", "/Volumes/bills/raw/incoming_bills/_checkpoints/silver_bill_parsed/")

BRONZE_TABLE    = dbutils.widgets.get("bronze_table")
SILVER_TABLE    = dbutils.widgets.get("silver_table")
CHECKPOINT_PATH = dbutils.widgets.get("checkpoint_path")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_TABLE.rsplit('.', 1)[0]}")

# COMMAND ----------
# MAGIC %md ### DocumentStructureParser — Strategy pattern
# MAGIC One interface, one implementation today (`AIDocumentStructureParser`).
# MAGIC Kept as an interface on purpose: the parsing *technique* is exactly
# MAGIC the kind of thing that might change later — a newer model version, or
# MAGIC a different engine entirely — without the rest of this notebook
# MAGIC needing to change at all.

# COMMAND ----------
class DocumentStructureParser(ABC):
    @abstractmethod
    def parse(self, files_df: DataFrame) -> DataFrame:
        """files_df has a binary `content` column. Returns one row per
        structural element (text block, table, figure...) found across
        all documents in the batch."""
        raise NotImplementedError


class AIDocumentStructureParser(DocumentStructureParser):
    """Calls ai_parse_document once per document, then explodes its
    `elements` array so each element gets its own row. This is the
    "no-code OCR" step: one function call reads both digital and scanned
    PDFs, with no custom extraction logic at all.

    Output shape of ai_parse_document (Databricks documented schema,
    version 2.0):
        document.elements[] -- each with id, type, content, confidence,
                                bbox, description
    """

    def parse(self, files_df: DataFrame) -> DataFrame:
        parsed_df = files_df.withColumn(
            "parsed", F.expr("ai_parse_document(content, map('version', '2.0'))")
        )

        # One PDF -> many elements. posexplode turns the elements array
        # inside the parsed VARIANT into one row per element, keeping each
        # element's position (element_index) alongside it.
        exploded_df = parsed_df.select(
            "file_path", "utility_type", "vendor_id",
            F.posexplode(
                F.expr("TRY_CAST(parsed:document:elements AS ARRAY<VARIANT>)")
            ).alias("element_index", "element"),
        )

        return exploded_df.select(
            "file_path", "utility_type", "vendor_id", "element_index",
            F.expr("CAST(element:type AS STRING)").alias("element_type"),
            F.expr("CAST(element:content AS STRING)").alias("content"),
            F.expr("CAST(element:confidence AS DOUBLE)").alias("confidence"),
            F.expr("CAST(element:bbox[0]:coord AS ARRAY<INT>)").alias("bbox_coord"),
            F.expr("CAST(element:bbox[0]:page_id AS INT)").alias("page_id"),
            F.expr("CAST(element:description AS STRING)").alias("description"),
        )

# COMMAND ----------
class ParserFactory:
    """The only place that maps a parser name to a class — swapping in a
    different structural parser later is a one-line registry change,
    not a rewrite of this notebook."""
    _registry = {"ai": AIDocumentStructureParser}
    _active = "ai"

    @classmethod
    def get_parser(cls) -> DocumentStructureParser:
        return cls._registry[cls._active]()

# COMMAND ----------
# MAGIC %md ### SilverParsingPipeline
# MAGIC Reads newly-registered files from Bronze, re-reads each one's actual
# MAGIC bytes from the Volume (Bronze only stored metadata, not content),
# MAGIC parses them, and writes the exploded element rows to Silver.

# COMMAND ----------
class SilverParsingPipeline:
    def __init__(self, parser: DocumentStructureParser, silver_table: str):
        self._parser = parser
        self._silver_table = silver_table

    def process_batch(self, bronze_batch_df: DataFrame, batch_id: int) -> None:
        if bronze_batch_df.isEmpty():
            print(f"[batch {batch_id}] no new files to parse")
            return

        new_files_df = bronze_batch_df.select("file_path", "utility_type", "vendor_id")
        file_paths = [row["file_path"] for row in new_files_df.collect()]

        files_with_content_df = (
            spark.read.format("binaryFile").load(file_paths)
            .withColumnRenamed("path", "file_path")
            .join(new_files_df, on="file_path")
        )

        parsed_elements_df = self._parser.parse(files_with_content_df)

        parsed_elements_df.write.format("delta").mode("append").saveAsTable(self._silver_table)
        print(f"[batch {batch_id}] wrote {parsed_elements_df.count()} parsed elements -> {self._silver_table}")

# COMMAND ----------
# MAGIC %md ### Run
# MAGIC Streams incrementally off the Bronze Delta table itself — only files
# MAGIC Bronze registered since Silver's last run get parsed here.

# COMMAND ----------
pipeline = SilverParsingPipeline(ParserFactory.get_parser(), SILVER_TABLE)

bronze_stream = spark.readStream.table(BRONZE_TABLE)

(bronze_stream.writeStream
    .foreachBatch(pipeline.process_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
    .awaitTermination())
