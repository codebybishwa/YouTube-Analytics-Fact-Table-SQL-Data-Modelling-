# YouTube Analytics Fact Table (SQL Data Modelling & ETL Pipeline)

This project builds a **production-ready fact table** by integrating YouTube channel-level and video-level data from multiple dimension tables.  
It demonstrates **advanced SQL**, **ETL design**, **dimensional modelling**, and **business feature engineering** for analytics use-cases.

---

## Project Overview

The goal of this project is to create a unified dataset that supports analysis on:

- Channel performance  
- Video performance  
- Content quality  
- Duration buckets  
- View segmentation  
- Engagement metrics


---

## ✅ Key Features

### 🔹 1. **Dimensional Modelling (Fact + Dimension join)**
The pipeline unifies raw data from:

- `src_youtube_channel_list`
- `dim_src_all_youtube_channel_metadata`
- `dim_src_all_youtube_videos`
- `dim_src_all_youtube_videos_metadata`
- `dim_src_youtube_video_stats`

This creates a well-structured star-schema-style fact table.

---

### 🔹 2. **Data Deduplication using Window Functions**
Channel metadata is deduplicated using:

```sql
ROW_NUMBER() OVER (PARTITION BY channelId ORDER BY channelViewCount DESC)

This ensures only the highest-quality metadata per channel is used.
```

---

### 🔹 3. Business Feature Engineering

Two powerful segmentation columns were added:

1. Video Duration Buckets

   - Short (<1 min)
   - Medium (1–5 min)
   - Long (5–15 min)
   - Extended (15–30 min)
   - Very Long (30+ min)

2. Video Views Buckets

    - Very Low
    - Low
    - Medium
    - High
    - Viral (2M+ views)

These buckets help in trend analysis and content strategy insights.

### 🔹 4. Data Cleaning & Type Casting

The pipeline standardizes:

- Duration
- Counts
- Quality
- Channel metadata
- Engagement metrics

Using consistent types: INT64, FLOAT64, and standardized timestamps.

### 🔹 5. Built Using BigQuery SQL

The project uses BigQuery-specific features:

- QUALIFY
- FLOAT64
- CTEs
- Window functions


## Tech Stack

1. SQL (BigQuery dialect)
2. ETL / Data Modelling
3. Fact/Dimension architecture
4. Analytics Engineering
