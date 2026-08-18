EnergyCAP

Databricks-based utility bill data processing pipeline for EnergyCAP.

1. Databricks Storage Setup

Before building the data pipeline, the required storage structure was created inside Databricks.

Why Databricks Volumes Were Used

The Databricks environment used for this project is the Free Edition. Since this environment cannot directly connect to an external AWS S3 bucket, Databricks' built-in file storage was used instead.

A Unity Catalog Volume was created to store the incoming utility bill files.

The storage hierarchy is:

Catalog
   |
   └── bills
        |
        └── raw
             |
             └── incoming_bills

The final Volume path is:

/Volumes/bills/raw/incoming_bills/

This is the source location used by the project notebooks.

Step 1: Creating the Catalog

A Catalog is the top-level container in Unity Catalog.

A catalog named bills was created for this project.

bills

Step 2: Creating the Schema

A Schema is created inside the Catalog.

A schema named raw was created to contain the unprocessed source files.

bills.raw

The hierarchy is:

bills
└── raw

Step 3: Creating the Volume

A Volume was created inside the raw schema.

The Volume is named:

incoming_bills

The complete Volume path is:

/Volumes/bills/raw/incoming_bills/

This Volume is used as the source location for the incoming utility bill files.

Step 4: Creating the Folder Structure

The utility bill PDFs were organized using the following folder structure:

bill_type=electricity/vendor_id=CPC001/
bill_type=electricity/vendor_id=REC002/
bill_type=gas/vendor_id=MGU001/
bill_type=gas/vendor_id=NGS002/
bill_type=water/vendor_id=AMW001/

The folder structure provides information about the bill type and vendor directly from the file path.

For example:

bill_type=electricity/vendor_id=CPC001/

indicates:

Bill Type → electricity
Vendor ID → CPC001

This information can therefore be associated with the file without having to extract it from the PDF content itself.

Step 5: Uploading the Utility Bill PDFs

The utility bill PDFs were uploaded into their corresponding vendor folders.

The PDFs were organized according to their utility type and vendor.

The final Volume contains:

361 utility bill PDFs

5 vendor folders

Utility type and vendor information represented in the folder structure

Step 6: Uploading buildings.csv

A reference file named buildings.csv was uploaded directly into the root of the incoming_bills Volume.

It is not stored inside the vendor folders.

The file contains the reference information required to map stores to their corresponding building_id.

The resulting structure is:

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

Final Storage Setup

The final source location for the project is:

/Volumes/bills/raw/incoming_bills/

This Volume contains the 361 utility bill PDFs organized by bill type and vendor, along with the buildings.csv reference file.

The project notebooks use this location as the source for processing.

2. Pipeline Overview

The project processes utility bill documents using a Medallion Architecture in Databricks.

The pipeline is organized into:

Bronze Layer

Silver Layer

Gold Layer

The overall flow is:

Incoming Utility Bill Files
            |
            v
       Bronze Layer
            |
            | File Metadata
            v
       Silver Layer
            |
            | Parsed and Processed Data
            v
        Gold Layer
            |
            | Business-Ready Data
            v
      Analytics / Reporting

3. Bronze Layer

The Bronze layer is responsible for detecting incoming utility bill PDF files and registering their file-level metadata.

Databricks Auto Loader is used to detect and process newly arriving files incrementally.

The Bronze layer does not read the actual business information inside the PDF.

Instead, it records information about the files, such as:

File path

File name

Utility type

Vendor ID

File size

Ingestion timestamp

Bronze Processing Flow

Incoming PDF Files
        |
        v
Databricks Auto Loader
        |
        v
Detect New Files
        |
        v
Extract File Metadata
        |
        v
Bronze Delta Table

Bronze Table

The file metadata is stored in the Bronze Delta table.

The Bronze layer therefore provides a reliable record of which files have entered the pipeline and basic information about those files.

4. Checkpointing

The Bronze ingestion process uses a checkpoint location because it is implemented as a streaming/incremental process.

The checkpoint maintains the processing state of the streaming pipeline.

The checkpoint path is created under the source Volume:

/Volumes/bills/raw/incoming_bills/_checkpoints/bronze_bill_files/

The purpose of the checkpoint is to allow the streaming process to maintain its processing state as new files arrive.

5. Schema Location

A separate schema location is configured for the Auto Loader process.

The schema location is:

/Volumes/bills/raw/incoming_bills/_checkpoints/bronze_bill_files/_schema/

This location is used to maintain schema-related information for the Auto Loader process.

6. Silver Layer

The Silver layer processes the actual contents of the utility bill documents.

Unlike the Bronze layer, the Silver layer works with the actual PDF documents rather than only their file metadata.

The documents are passed through document parsing logic to extract information from their contents.

The parsed document can contain elements such as:

Text

Tables

Figures

Page information

Document structure

The extracted information is then transformed into a structured format for further processing.

Silver Processing Flow

Bronze File Information
        |
        v
Access PDF
        |
        v
Parse Document
        |
        v
Extract Document Elements
        |
        v
Transform and Structure Data
        |
        v
Silver Data

7. Gold Layer

The Gold layer is intended to contain business-ready data derived from the processed Silver-layer information.

The Gold layer can be used for:

Analytics

Reporting

Business insights

Downstream applications

The Gold layer represents the final stage of the Medallion Architecture where processed data can be prepared for business consumption.

8. Project Files

EnergyCAP/
│
├── 01_ingest_bills_bronze.py
├── 02_silver_bills.py
├── schemas.py
└── README.md

01_ingest_bills_bronze.py

Contains the Bronze-layer ingestion logic.

Responsibilities include:

Configuring the source path

Configuring the Bronze table

Configuring checkpoint and schema locations

Using Databricks Auto Loader

Detecting incoming PDF files

Creating file metadata

Writing metadata to the Bronze Delta table

02_silver_bills.py

Contains the Silver-layer processing logic.

Responsibilities include:

Accessing the incoming bill documents

Parsing PDF documents

Processing parsed document information

Transforming the extracted information into structured data

schemas.py

Contains shared schema definitions used by the project for structured data processing.

9. Technologies

The project uses:

Databricks

Python

PySpark

Apache Spark

Delta Lake

Databricks Auto Loader

Unity Catalog

Unity Catalog Volumes

Medallion Architecture

10. End-to-End Data Flow

The complete project flow is:

                     UNITY CATALOG
                          |
                          v
                bills.raw.incoming_bills
                          |
              ┌───────────┴───────────┐
              |                       |
              v                       v
       Utility Bill PDFs        buildings.csv
              |
              v
       Databricks Auto Loader
              |
              v
          BRONZE LAYER
              |
              | File Metadata
              |
              v
        Bronze Delta Table
              |
              v
          SILVER LAYER
              |
              | Read / Parse PDF
              |
              v
       Structured Document Data
              |
              v
           GOLD LAYER
              |
              | Business-Ready Data
              |
              v
       Analytics / Reporting
