# 🚗 Car Sales — End-to-End Data Engineering Project

An end-to-end **Azure Data Engineering project** that implements an automated **incremental data pipeline** using Azure Data Factory, Azure Data Lake Storage, and Databricks.

The project follows the **Medallion Architecture (Bronze → Silver → Gold)** and transforms raw car sales data into an analytics-ready **Star Schema**.

## 🏗️ Architecture

```text
Source Data
    │
    ▼
SQL Database
    │
    ▼
Azure Data Factory
    │
    │ Incremental Load
    ▼
ADLS Gen2
    │
    ▼
┌───────────────┐
│ Bronze Layer  │  Raw Data
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ Silver Layer  │  Cleaned & Transformed Data
└───────┬───────┘
        │
        ▼
┌───────────────┐
│  Gold Layer   │  Star Schema
└───────┬───────┘
        │
        ▼
   Analytics Ready
```

## 🔄 Pipeline Workflow

1. **Source:** Car sales data is stored in a SQL database.
2. **Incremental Ingestion:** Azure Data Factory uses a **watermark table** to identify newly added records.
3. **Bronze:** Only new records are loaded into the Bronze layer in ADLS Gen2.
4. **Silver:** Databricks/PySpark cleans and transforms the Bronze data.
5. **Gold:** The transformed data is structured into a dimensional Star Schema.
6. **Orchestration:** Azure Data Factory automatically executes the notebooks in the required sequence.
7. **Watermark Update:** After successful processing, the latest timestamp is stored for the next pipeline run.

### ⏱️ Incremental Loading

Instead of processing the complete dataset on every run:

```text
Previous Watermark
       │
       ▼
Find records > watermark
       │
       ▼
Process only new records
       │
       ▼
Update watermark
       │
       ▼
Next Pipeline Run
```

This reduces unnecessary processing, execution time, and compute usage.

## ⭐ Gold Layer — Star Schema

The Gold layer contains a dimensional model consisting of:

* **Fact Sales**
* **Dim Branch**
* **Dim Date**
* **Dim Dealer**
* **Dim Model**

```text
                  Dim Branch
                       │
                       ▼
Dim Dealer ─────► Fact Sales ◄───── Dim Model
                       ▲
                       │
                   Dim Date
```

## 🛠️ Technologies

| Technology                       | Purpose                   |
| -------------------------------- | ------------------------- |
| **Azure Data Factory**           | Ingestion & orchestration |
| **Azure Data Lake Storage Gen2** | Data storage              |
| **Azure Databricks**             | Data transformation       |
| **PySpark**                      | Distributed processing    |
| **Delta Lake**                   | Data storage & processing |
| **SQL Database**                 | Source system             |
| **GitHub**                       | Version control           |

## 📂 Project Structure

```text
Car-Sales-End-to-End-Data-Engineering-Project/
│
├── Databricks Notebooks/
│   ├── schema_setup.py
│   ├── silver notebook.py
│   ├── gold branch dim notebook.py
│   ├── gold date dim notebook.py
│   ├── gold dealer dim notebook.py
│   ├── gold model dim notebook.py
│   └── gold fact notebook.py
│
├── Datasets/
├── Linked Services/
├── Pipelines/
├── Factory/
├── Workflow/
└── Screenshot/
```

## 🎯 Key Concepts Demonstrated

* End-to-end Azure data engineering
* Medallion Architecture
* Incremental data ingestion
* Watermark-based processing
* PySpark transformations
* Delta Lake
* Dimensional modelling & Star Schema
* Azure Data Factory orchestration
* Automated pipeline execution

## 📈 Outcome

The incremental pipeline was successfully implemented and executed. It identifies fresh records using the watermark, processes only the new data through the Bronze, Silver, and Gold layers, and updates the watermark with the latest timestamp for the next run.

The result is an **automated, repeatable, and scalable data engineering workflow**.

## 👩‍💻 Author

**Ishika**

🔗 [GitHub Repository](https://github.com/Ishika411/Car-Sales-End-to-End-Data-Engineering-Project)

