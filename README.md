# EnergyCAP – Databricks Utility Bill Data Processing Pipeline

A Databricks-based utility bill data processing pipeline designed to ingest, process, and transform utility bill documents into structured, business-ready data using the **Medallion Architecture**.

The project processes utility bill PDFs from multiple vendors and utility types such as electricity, gas, and water. The pipeline uses **Databricks Auto Loader, PySpark, Delta Lake, Unity Catalog Volumes, and document parsing** to move data through Bronze, Silver, and Gold layers.

---

## 1. Project Objective

The objective of this project is to build an automated and scalable utility bill processing pipeline similar to an EnergyCAP-style data processing system.

The pipeline is designed to:

* Ingest utility bill PDF files incrementally.
* Track incoming files and their metadata.
* Identify utility type and vendor from the file path.
* Parse the actual contents of utility bill documents.
* Transform unstructured document information into structured data.
* Prepare processed data for analytics and reporting.
* Maintain a clear separation between raw, processed, and business-ready data.

---

## 2. Business Problem

Utility companies and organizations may receive large numbers of utility bills from multiple vendors.

These bills can contain information such as:

* Account information
* Building/store information
* Billing period
* Utility type
* Meter information
* Consumption
* Charges
* Taxes
* Total amount
* Invoice details

The information is often stored inside PDF documents and may not initially be available in a structured tabular format.

Manually processing hundreds or thousands of documents can be:

* Time-consuming
* Difficult to maintain
* Error-prone
* Difficult to scale

This project automates the initial ingestion and document-processing workflow using Databricks.

---

# 3. High-Level Architecture

The project follows the **Medallion Architecture**.

```text
                    UNITY CATALOG
                         |
                         v
              bills.raw.incoming_bills
                         |
              +----------+----------+
              |                     |
              v                     v
       Utility Bill PDFs       buildings.csv
              |
              v
       DATBRICKS AUTO LOADER
              |
              v
        +-------------+
        |   BRONZE    |
        |             |
        | File Metadata|
        +-------------+
              |
              v
        +-------------+
        |   SILVER    |
        |             |
        | PDF Parsing |
        | Structured  |
        | Data        |
        +-------------+
              |
              v
        +-------------+
        |    GOLD     |
        |             |
        | Business-   |
        | Ready Data  |
        +-------------+
              |
              v
       Analytics / Reporting
```

---

# 4. Databricks Storage Setup

The project is implemented using the **Databricks Free Edition** environment.

Because the Free Edition environment used for this project does not provide the required direct connection to an external AWS S3 bucket, a **Unity Catalog Volume** is used as the project's source storage location.

The Volume provides a managed location inside Databricks where the incoming utility bill files can be stored and accessed by the notebooks.

---

## 4.1 Unity Catalog Hierarchy

The storage hierarchy is:

```text
Catalog
   |
   └── bills
        |
        └── raw
             |
             └── incoming_bills
```

The corresponding Volume path is:

```text
/Volumes/bills/raw/incoming_bills/
```

This is the primary source location used by the pipeline.

---

# 5. Creating the Catalog

A Catalog is the top-level container in Unity Catalog.

A catalog named `bills` was created for the project.

```text
bills
```

---

# 6. Creating the Schema

A schema was created inside the `bills` catalog.

The schema is named:

```text
raw
```

The resulting hierarchy is:

```text
bills
└── raw
```

The `raw` schema is used for source-related data and storage.

---

# 7. Creating the Volume

A Volume named `incoming_bills` was created inside the `raw` schema.

The complete Volume path is:

```text
/Volumes/bills/raw/incoming_bills/
```

This Volume acts as the source location for incoming utility bill files.

---

# 8. Input File Organization

The utility bill PDFs are organized using a partition-style directory structure.

```text
bill_type=electricity/vendor_id=CPC001/
bill_type=electricity/vendor_id=REC002/

bill_type=gas/vendor_id=MGU001/
bill_type=gas/vendor_id=NGS002/

bill_type=water/vendor_id=AMW001/
```

The folder structure contains metadata directly in the file path.

For example:

```text
bill_type=electricity/vendor_id=CPC001/
```

represents:

```text
Bill Type  → electricity
Vendor ID  → CPC001
```

This allows the pipeline to associate files with their utility type and vendor without depending entirely on information extracted from the PDF itself.

---

# 9. Final Storage Structure

The final Volume structure is:

```text
/Volumes/bills/raw/incoming_bills/

├── buildings.csv
│
├── bill_type=electricity/
│   ├── vendor_id=CPC001/
│   └── vendor_id=REC002/
│
├── bill_type=gas/
│   ├── vendor_id=MGU001/
│   └── vendor_id=NGS002/
│
└── bill_type=water/
    └── vendor_id=AMW001/
```

The source location contains:

* 361 utility bill PDFs
* 5 vendor folders
* Multiple utility types
* `buildings.csv` reference data

---

# 10. buildings.csv

A reference file named `buildings.csv` is stored directly at the root of the Volume.

```text
/Volumes/bills/raw/incoming_bills/buildings.csv
```

It is intentionally kept outside the vendor-specific folders.

The file contains reference information required to map stores or locations to their corresponding `building_id`.

Conceptually, the file provides a mapping such as:

```text
Store / Location
       |
       v
building_id
```

This reference data can later be used during Silver-layer processing to enrich the parsed utility bill data.

---

# 11. Medallion Architecture

The pipeline is divided into three logical processing layers.

| Layer  | Purpose              | Main Processing                |
| ------ | -------------------- | ------------------------------ |
| Bronze | Raw ingestion        | File discovery and metadata    |
| Silver | Data processing      | PDF parsing and transformation |
| Gold   | Business consumption | Business-ready structured data |

The separation of these layers provides:

* Better data organization
* Easier debugging
* Reprocessing capability
* Improved maintainability
* Clear separation of responsibilities
* Better scalability

---

# 12. Bronze Layer

The Bronze layer is responsible for detecting and registering incoming utility bill files.

It focuses on **file-level information** rather than extracting business information from the PDF.

---

## 12.1 Bronze Responsibilities

The Bronze layer:

* Reads incoming PDF files.
* Detects newly arriving files.
* Uses Databricks Auto Loader.
* Captures file metadata.
* Extracts utility type from the path.
* Extracts vendor ID from the path.
* Records ingestion information.
* Writes the results to a Delta table.

Typical metadata includes:

```text
File Path
File Name
File Size
Utility Type
Vendor ID
Ingestion Timestamp
```

---

# 13. Bronze Processing Flow

```text
Incoming PDF Files
        |
        v
Databricks Auto Loader
        |
        v
Detect New Files
        |
        v
Read File Metadata
        |
        v
Extract Path Information
        |
        v
Create Bronze DataFrame
        |
        v
Bronze Delta Table
```

The Bronze layer therefore acts as the first structured representation of incoming files.

---

# 14. Why Auto Loader Is Used

Databricks Auto Loader is used to support incremental file ingestion.

Instead of repeatedly processing every file in the source directory, Auto Loader keeps track of files that have already been processed.

Conceptually:

```text
New File
   |
   v
Auto Loader detects it
   |
   v
File is processed
   |
   v
Processing state is maintained
```

When additional utility bills are added to the source location, the pipeline can detect and process the newly arriving files.

This makes the ingestion process more suitable for an ongoing data pipeline.

---

# 15. Checkpointing

The Bronze ingestion process uses a checkpoint because it is implemented as a streaming/incremental process.

The checkpoint location is:

```text
/Volumes/bills/raw/incoming_bills/_checkpoints/bronze_bill_files/
```

The checkpoint maintains the processing state of the streaming query.

Conceptually:

```text
Source Files
     |
     v
Auto Loader
     |
     +------> Checkpoint
     |
     v
Bronze Table
```

The checkpoint helps the streaming process maintain information about its progress and previously processed files.

---

# 16. Auto Loader Schema Location

A separate schema location is configured for Auto Loader.

```text
/Volumes/bills/raw/incoming_bills/_checkpoints/bronze_bill_files/_schema/
```

This location is used to maintain schema-related information for the Auto Loader process.

Separating the checkpoint and schema information helps keep the streaming ingestion state organized.

---

# 17. Silver Layer

The Silver layer is responsible for processing the actual contents of the utility bill documents.

Unlike Bronze, which focuses primarily on file metadata, Silver works with the document itself.

The PDF documents are passed through document parsing logic to extract meaningful information.

---

## 17.1 Silver Responsibilities

The Silver layer is responsible for:

* Accessing the source PDF.
* Parsing the document.
* Extracting document elements.
* Processing extracted text.
* Processing tables.
* Identifying relevant document information.
* Transforming unstructured information into structured records.
* Applying validation and transformation logic.
* Enriching data using reference information such as `buildings.csv`.

---

# 18. Document Parsing

Utility bills are unstructured documents.

A PDF can contain different types of information, including:

```text
Text
Tables
Figures
Pages
Document Structure
```

The parsing process converts this document information into data that can be processed programmatically.

Conceptually:

```text
PDF Document
     |
     v
Document Parser
     |
     v
Document Elements
     |
     +---- Text
     |
     +---- Tables
     |
     +---- Page Information
     |
     +---- Document Structure
     |
     v
Structured Data
```

---

# 19. Silver Processing Flow

```text
Bronze File Metadata
        |
        v
Identify PDF
        |
        v
Access Source Document
        |
        v
Parse PDF
        |
        v
Extract Document Elements
        |
        v
Transform Data
        |
        v
Validate / Enrich Data
        |
        v
Silver Data
```

---

# 20. Bronze vs Silver

The main difference between the Bronze and Silver layers is the level of processing.

| Bronze                 | Silver                        |
| ---------------------- | ----------------------------- |
| File-oriented          | Document-oriented             |
| Detects files          | Processes file contents       |
| Stores metadata        | Stores structured information |
| Minimal transformation | Multiple transformations      |
| Uses Auto Loader       | Uses document parsing         |
| Raw ingestion layer    | Cleansed/processed layer      |

For example:

### Bronze

```text
file_name = bill_001.pdf
file_path = /Volumes/.../vendor_id=CPC001/bill_001.pdf
bill_type = electricity
vendor_id = CPC001
file_size = ...
ingestion_timestamp = ...
```

### Silver

```text
vendor_id
building_id
account_number
billing_period
consumption
unit
subtotal
tax
total_amount
```

The exact Silver schema depends on the information available in the utility bill documents.

---

# 21. Gold Layer

The Gold layer is the final business-oriented layer of the pipeline.

Its purpose is to provide clean and structured data that can be consumed by downstream applications.

Potential Gold-layer data can include:

```text
Building
Vendor
Utility Type
Billing Period
Consumption
Usage Unit
Charges
Taxes
Total Amount
```

---

## 21.1 Gold Use Cases

The Gold layer can support:

* Utility cost analysis
* Consumption analysis
* Vendor comparison
* Building-level reporting
* Monthly bill analysis
* Energy usage trends
* Business dashboards
* Analytics
* Downstream applications

---

# 22. End-to-End Data Flow

```text
                         UNITY CATALOG
                              |
                              v
                 bills.raw.incoming_bills
                              |
                 +------------+------------+
                 |                         |
                 v                         v
          Utility Bill PDFs         buildings.csv
                 |
                 v
        DATBRICKS AUTO LOADER
                 |
                 v
          +--------------+
          |    BRONZE    |
          |              |
          | File Metadata|
          +--------------+
                 |
                 v
          +--------------+
          |    SILVER    |
          |              |
          | PDF Parsing  |
          | Transformation|
          +--------------+
                 |
                 v
          +--------------+
          |     GOLD     |
          |              |
          | Business     |
          | Ready Data   |
          +--------------+
                 |
                 v
        Analytics / Reporting
```

---

# 23. Project Structure

```text
EnergyCAP/
│
├── 01_ingest_bills_bronze.py
├── 02_silver_bills.py
├── schemas.py
└── README.md
```

---

# 24. Project Files

## 24.1 `01_ingest_bills_bronze.py`

This notebook/script contains the Bronze-layer ingestion logic.

Responsibilities include:

* Configuring the source path.
* Configuring the Bronze Delta table.
* Configuring checkpoint location.
* Configuring Auto Loader schema location.
* Reading incoming PDF files.
* Detecting newly arriving files.
* Extracting file metadata.
* Extracting vendor ID and bill type from the file path.
* Writing metadata to the Bronze Delta table.

---

## 24.2 `02_silver_bills.py`

This notebook/script contains the Silver-layer document-processing logic.

Responsibilities include:

* Reading incoming bill documents.
* Accessing the source PDFs.
* Parsing PDF documents.
* Extracting document information.
* Transforming extracted information.
* Structuring the extracted data.
* Applying validation logic.
* Enriching data using reference datasets.

---

## 24.3 `schemas.py`

This module contains shared schema definitions used by the project.

Keeping schemas in a separate module helps:

* Avoid duplicate schema definitions.
* Improve code maintainability.
* Keep notebooks cleaner.
* Standardize data structures.
* Make future schema changes easier.

---

# 25. Object-Oriented Design

The processing logic is intended to follow a modular and object-oriented structure.

Instead of placing all processing logic directly inside notebooks, responsibilities can be separated into reusable classes.

A conceptual structure is:

```text
BaseProcessor
     |
     +------------------+
     |                  |
BronzeProcessor   SilverProcessor
     |                  |
     v                  v
File Ingestion     Document Parsing
```

Additional components can handle specific responsibilities such as:

```text
FileMetadataExtractor
DocumentParser
DataTransformer
DataValidator
ReferenceDataLoader
```

This approach provides:

* Separation of concerns
* Reusable components
* Easier testing
* Easier debugging
* Better maintainability
* Cleaner notebooks
* Easier future expansion

---

# 26. Separation of Responsibilities

The project follows the principle that each component should have a clear responsibility.

For example:

```text
AutoLoader
    |
    +--> File Ingestion

MetadataExtractor
    |
    +--> Path / File Metadata

DocumentParser
    |
    +--> PDF Parsing

Transformer
    |
    +--> Data Transformation

Validator
    |
    +--> Data Validation

ReferenceDataLoader
    |
    +--> buildings.csv Processing
```

This prevents one notebook or class from becoming responsible for the entire pipeline.

---

# 27. Technologies Used

| Technology             | Purpose                                   |
| ---------------------- | ----------------------------------------- |
| Databricks             | Data engineering platform                 |
| Python                 | Application and pipeline logic            |
| PySpark                | Distributed data processing               |
| Apache Spark           | Processing engine                         |
| Delta Lake             | Reliable table storage                    |
| Databricks Auto Loader | Incremental file ingestion                |
| Unity Catalog          | Data governance and organization          |
| Unity Catalog Volumes  | Source file storage                       |
| Medallion Architecture | Layered data processing                   |
| PDF Document Parsing   | Extracting information from utility bills |

---

# 28. Prerequisites

Before running the project, the following should be available:

* Databricks workspace
* Unity Catalog enabled
* Access to create Catalogs, Schemas, and Volumes
* Databricks notebooks
* PySpark environment
* Source utility bill PDFs
* `buildings.csv`
* Required document parsing libraries or Databricks-supported document processing capabilities

---

# 29. Setup

## Step 1 – Create Catalog

Create the catalog:

```text
bills
```

## Step 2 – Create Schema

Create the schema:

```text
bills.raw
```

## Step 3 – Create Volume

Create:

```text
bills.raw.incoming_bills
```

## Step 4 – Upload Source Files

Upload the utility bill PDFs using the following structure:

```text
bill_type=<utility_type>/vendor_id=<vendor_id>/
```

Example:

```text
bill_type=electricity/vendor_id=CPC001/
```

## Step 5 – Upload Reference Data

Upload:

```text
buildings.csv
```

to:

```text
/Volumes/bills/raw/incoming_bills/
```

## Step 6 – Run Bronze Processing

Run:

```text
01_ingest_bills_bronze.py
```

This creates the Bronze file metadata layer.

## Step 7 – Run Silver Processing

Run:

```text
02_silver_bills.py
```

This processes the actual bill documents and generates structured Silver-layer data.

## Step 8 – Gold Processing

The processed Silver data can then be transformed into business-ready Gold datasets for reporting and analytics.

---

# 30. Expected Pipeline Behavior

When a new PDF is added to the source Volume:

```text
New PDF
   |
   v
Auto Loader detects file
   |
   v
Bronze metadata created
   |
   v
Silver processing identifies document
   |
   v
PDF content is parsed
   |
   v
Structured information generated
   |
   v
Data becomes available for Gold processing
```

The pipeline is therefore designed to support incremental processing rather than requiring all files to be manually processed every time.

---

# 31. Data Quality Considerations

The pipeline should validate important fields before data moves into the business-ready layer.

Potential validation checks include:

* Required fields are present.
* Vendor ID is valid.
* Utility type is recognized.
* Billing period is valid.
* Consumption values are numeric.
* Charges are numeric.
* Total amount is valid.
* Building mapping exists.
* Duplicate documents are handled appropriately.

Example:

```text
PDF
 |
 v
Extract Data
 |
 v
Validate
 |
 +---- Invalid ---> Error / Quarantine
 |
 +---- Valid -----> Silver
```

---

# 32. Error Handling

Document processing can fail because of:

* Corrupted PDF files
* Unsupported document formats
* Missing information
* Unexpected document layouts
* Parsing errors
* Invalid reference data
* Missing building mappings

The pipeline should therefore be designed so that an individual bad document does not unnecessarily stop processing of all other valid documents.

Failed documents can be identified and handled separately for investigation and reprocessing.

---

# 33. Scalability

The architecture is designed with scalability in mind.

As the number of utility bills increases:

```text
361 files
   |
   v
Thousands of files
   |
   v
Millions of records
```

Spark and Databricks can distribute data-processing workloads across compute resources.

Auto Loader also supports incremental file ingestion, which reduces the need to repeatedly scan and process the complete source dataset.

---

# 34. Reprocessing Strategy

The layered architecture makes it possible to reprocess data without necessarily starting the entire pipeline from scratch.

For example:

```text
Raw Files
    |
    v
Bronze
    |
    v
Silver
    |
    v
Gold
```

If Silver processing logic changes, Bronze metadata can remain available and the Silver processing logic can be rerun.

This separation helps during:

* Development
* Debugging
* Schema changes
* Parser improvements
* Data-quality corrections

---

# 35. Current Dataset

The current project dataset contains:

```text
Total Utility Bill PDFs : 361
Vendor Folders          : 5
Utility Types           : Electricity, Gas, Water
Reference File          : buildings.csv
```

Vendor examples include:

```text
CPC001
REC002
MGU001
NGS002
AMW001
```

---

# 36. Current Pipeline Status

The current implementation focuses on:

```text
Storage Setup
      |
      v
Bronze Ingestion
      |
      v
Silver Document Processing
      |
      v
Structured Data
```

The Gold layer is intended to provide the final business-ready representation of the processed utility bill data.

---

# 37. Future Enhancements

Potential future improvements include:

* Complete Gold-layer implementation.
* Additional utility bill vendors.
* Support for additional utility types.
* Improved document parsing.
* Automated data-quality checks.
* Error/quarantine handling.
* Automated pipeline scheduling.
* Data lineage and monitoring.
* Dashboard integration.
* Vendor-level analytics.
* Building-level consumption analytics.
* Automated anomaly detection.
* Improved schema evolution handling.
* Unit testing for transformation logic.
* CI/CD integration.
* Databricks Asset Bundles for deployment.
* Workflow-based orchestration.

---

# 38. Key Design Principles

The project follows the following principles:

### Separation of Concerns

Each pipeline component has a clearly defined responsibility.

### Incremental Processing

New files should be processed incrementally instead of repeatedly processing the entire source.

### Layered Architecture

Bronze, Silver, and Gold layers separate raw ingestion, processing, and business consumption.

### Reusability

Common logic such as schemas, parsing, validation, and metadata extraction should be reusable.

### Maintainability

Processing logic should be organized into modular components instead of placing all logic inside a single notebook.

### Scalability

The pipeline uses Spark and Databricks capabilities to support larger data volumes.

---

# 39. Summary

The EnergyCAP project implements a Databricks-based utility bill processing pipeline using a Medallion Architecture.

The overall process is:

```text
Utility Bill PDFs
        |
        v
Unity Catalog Volume
        |
        v
Auto Loader
        |
        v
Bronze
(File Metadata)
        |
        v
Silver
(PDF Parsing + Structured Data)
        |
        v
Gold
(Business-Ready Data)
        |
        v
Analytics / Reporting
```

The project demonstrates how Databricks can be used to build a structured and scalable data-processing pipeline for unstructured utility bill documents.

The combination of **Unity Catalog Volumes, Auto Loader, PySpark, Delta Lake, document parsing, and Medallion Architecture** provides the foundation for an automated EnergyCAP-style utility data processing solution.
