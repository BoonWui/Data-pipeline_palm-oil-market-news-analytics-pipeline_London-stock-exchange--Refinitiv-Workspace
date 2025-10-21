# Data-pipeline_palm-oil-market-news-analytics-pipeline_London-stock-exchange--Refinitiv-Workspace
Palm Oil Market News Data Pipeline Automated ETL pipeline that extracts palm oil and biofuel-related market news from the LSEG/Refinitiv API, filters and cleans the data, and stores it in Parquet format for downstream analysis or visualization.

Palm Oil Market News Analytics Pipeline

An automated data pipeline for collecting, filtering, and storing palm oil and biofuel-related market news from LSEG Refinitiv (Reuters) APIs, optimized for large-scale research and sentiment modeling.

🚀 Features

Automated LSEG Data Fetching
Fetches historical and current palm oil–related headlines and stories via the official LSEG Data API.

Smart Filtering
Removes irrelevant or duplicate articles (e.g., company earnings, technicals, and unrelated commodities).

Company Exclusion Support
Supports advanced queries to exclude or include company-related topics via LSEG’s RIC and COMPANY fields.

Automatic Rate-Limiting
Dynamically manages API request limits (e.g., 3,000 requests/day) to avoid quota lockouts.

Structured Storage
Saves clean, structured .parquet files by date range for scalable analytics.

Future-Ready for AI
Easily extendable with FinBERT or Llama3 for contextual sentiment tagging (Bullish / Bearish / Neutral).





Palm Oil Market News Data Pipeline
Automated ETL pipeline that extracts palm oil and biofuel-related market news from the LSEG/Refinitiv API, filters and cleans the data, and stores it in Parquet format for downstream analysis or visualization.

Features:

Handles multi-year historical fetching

Auto-resumes across time windows

Applies keyword and company exclusion filters

Respects API rate limits

Runs daily at 6:30 PM automatically
