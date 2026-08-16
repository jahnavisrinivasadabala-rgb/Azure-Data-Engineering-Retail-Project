# Retail Data Engineering Pipeline on Azure

## Project Overview

This project demonstrates an end-to-end retail data engineering pipeline built using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, and Unity Catalog.

The pipeline ingests retail CSV data into a Landing layer, processes it through a Medallion Architecture, applies data-quality and referential-integrity rules in Silver, and creates business-oriented Gold datasets for sales analysis.

## Architecture

```text
Retail CSV Sources
       |
       v
Azure Data Factory
       |
       v
ADLS Gen2 - Landing
       |
       v
Azure Databricks
       |
       +----------------+
       |                |
       v                v
   Bronze            Silver
   Delta              Delta
       |                |
       |                +--> Data cleansing
       |                +--> Validation
       |                +--> Referential integrity
       |                +--> Deduplication
       |                |
       +----------------+
                |
                v
              Gold
                |
        +-------+-------+
        |               |
     Fact Sales     Dimensions
        |               |
        +-------+-------+
                |
                v
        Business Analysis
```

## Technologies

- Azure Data Factory
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Azure Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Spark SQL
- GitHub

## Data Flow

### 1. Landing

Azure Data Factory Copy Data activity copies the source retail CSV files into the ADLS Gen2 Landing container.

### 2. Bronze

The Bronze layer reads the Landing CSV files using explicit PySpark schemas and writes the data as Delta files to the Bronze container. Bronze Delta locations are registered as external Unity Catalog tables.

The source datasets include:

- Customers
- Orders
- Order Items
- Products
- Stores
- Employees

### 3. Silver

The Silver layer reads the Bronze Unity Catalog tables and prepares trusted datasets for downstream analytics.

Transformations and validations include:

- Removing duplicate records
- Trimming text fields
- Standardizing location and status/payment fields
- Validating required IDs
- Validating positive quantities and salaries
- Validating product prices
- Validating customer and store references
- Validating order and order-item references
- Casting fields to appropriate data types

Invalid records and quarantine processing are planned as a follow-up enhancement.

### 4. Gold

The Gold layer creates a business-oriented dimensional model.

#### Fact Sales

`fact_sales` is defined at the **order-item grain**: one row represents one product line within an order.

The current fact logic:

- Joins Orders and Order Items using `order_id`
- Keeps completed orders for sales analysis
- Calculates `sales_amount = quantity * selling_price`

#### Dimensions

- `dim_product`
- `dim_customer`
- `dim_store`
- `dim_employee`

This follows a star-schema style design where the fact table contains transaction keys and measures while dimensions contain descriptive business attributes.

## Project Structure

```text
Azure-Data-Engineering-Retail-Project/
|
+-- Databricks/
|   +-- Bronze_Project2.py
|   +-- Silver_Project2.ipynb
|   +-- Gold_Project2.ipynb
|   +-- UPLOAD_NOTE.md
|
+-- Project1Copydata.json
+-- customers.csv
+-- README.md
+-- Screenshots/
```

## Key Data Engineering Concepts Demonstrated

- Azure Data Factory ingestion
- ADLS Gen2 storage layers
- Medallion Architecture
- Explicit PySpark schema definition
- CSV ingestion using `format("csv")` and `option("header", "true")`
- Delta Lake writes
- Unity Catalog external tables
- PySpark DataFrame transformations
- `join`, `left_semi`, and `left_anti` joins
- Deduplication
- Data-quality validation
- Referential integrity checks
- Fact and dimension modeling
- Business aggregations

## Current Status

Completed:

- ADF ingestion to Landing
- Bronze Delta ingestion
- Unity Catalog Bronze tables
- Silver cleansing and validation
- Gold fact-sales foundation
- Gold product, customer, store, and employee dimensions

Planned enhancements:

- Quarantine/error-record framework
- Additional Gold business KPIs
- Incremental loading and MERGE-based processing
- ADF triggers and more metadata-driven orchestration
- Performance optimization and monitoring

## Repository

The Databricks implementation is available in the `Databricks` directory, while the ADF pipeline definition is maintained in the repository as JSON.
