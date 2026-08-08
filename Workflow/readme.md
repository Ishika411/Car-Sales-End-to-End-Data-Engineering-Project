# 🧩 Pipeline Execution Logic

A typical pipeline execution follows this sequence:

```text
START
  │
  ▼
Read Watermark
  │
  ▼
Query SQL Source
  │
  ▼
Are there new records?
  │
  ├── NO ──► End
  │
  └── YES
        │
        ▼
   Load Bronze
        │
        ▼
 Run Silver Notebook
        │
        ▼
 Run Gold Dimensions
        │
        ▼
 Run Gold Fact
        │
        ▼
Determine Latest Timestamp
        │
        ▼
Update Watermark
        │
        ▼
       END
```

This makes the pipeline repeatable and minimizes manual intervention.

---

# ✅ Incremental Pipeline Result

The incremental pipeline was successfully executed.

During execution, the pipeline:

1. Read the existing watermark.
2. Identified newly available source records.
3. Processed only those new records.
4. Loaded the records into the Bronze layer.
5. Transformed the data through the Silver layer.
6. Updated the Gold dimensional model.
7. Updated the watermark with the latest timestamp.

The next pipeline execution can therefore use the newly stored watermark as its starting point.

```text
Previous Run
     │
     ▼
Watermark = T1
     │
     ▼
Process records > T1
     │
     ▼
Latest Record = T2
     │
     ▼
Watermark = T2
     │
     ▼
Next Run
     │
     ▼
Process records > T2
```

---

# 📊 Key Data Engineering Concepts Demonstrated

This project demonstrates several important concepts used in modern data engineering:

### Data Ingestion

Moving data from a relational source system into cloud storage.

### Incremental Loading

Processing only new records instead of repeatedly processing the entire dataset.

### Watermarking

Maintaining the latest processed timestamp to identify new records during subsequent runs.

### Medallion Architecture

Separating raw, cleaned, and analytics-ready data into Bronze, Silver, and Gold layers.

### Distributed Processing

Using PySpark and Databricks to process and transform data.

### Dimensional Modelling

Designing a Star Schema consisting of fact and dimension tables.

### Pipeline Orchestration

Automating the execution sequence and dependencies between ingestion and transformation activities.

### Cloud Data Lake

Using Azure Data Lake Storage as the scalable storage layer.

### Infrastructure Configuration

Maintaining Azure Data Factory deployment artifacts through ARM templates.

---

# 💡 What I Learned

Through this project, I gained practical experience with:

* Designing an end-to-end data pipeline.
* Working with Azure Data Factory.
* Working with Azure Data Lake Storage.
* Developing PySpark transformations in Databricks.
* Implementing Medallion Architecture.
* Designing a Star Schema.
* Understanding incremental data ingestion.
* Implementing watermark-based processing.
* Orchestrating dependent data processing tasks.
* Structuring cloud data engineering projects.
* Version-controlling pipeline artifacts using GitHub.

---

## ⭐ Summary

This project demonstrates the complete journey of data:

```text
                 SOURCE
                   │
                   ▼
             SQL DATABASE
                   │
                   ▼
        AZURE DATA FACTORY
                   │
          Incremental Load
                   │
                   ▼
        ┌──────────────────┐
        │  BRONZE / ADLS   │
        │    Raw Data      │
        └────────┬─────────┘
                 │
                 ▼
        ┌──────────────────┐
        │ SILVER / DATABRICKS│
        │ Cleaned Data     │
        └────────┬─────────┘
                 │
                 ▼
        ┌──────────────────┐
        │ GOLD / DATABRICKS│
        │   Star Schema    │
        └────────┬─────────┘
                 │
                 ▼
          ANALYTICS READY
```

This demonstrates how a manually executed data-processing workflow can be transformed into an **automated, repeatable, and scalable cloud data engineering pipeline**.
