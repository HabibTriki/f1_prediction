# Formula 1 Big Data Prediction Project

This repository implements a Kappa architecture pipeline for ingesting, processing, and visualizing Formula 1 data. It uses Apache Kafka for streaming, Apache Spark for processing, PostgreSQL as a serving database, and Power BI for reporting.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Repository Structure](#repository-structure)
4. [Prerequisites](#prerequisites)
5. [Installation and Setup](#installation-and-setup)
6. [Running the Pipeline](#running-the-pipeline)
7. [Power BI Integration](#power-bi-integration)
8. [Development Workflow](#development-workflow)
9. [Contributing](#contributing)
10. [License](#license)

---

## Project Overview

The goal of this project is to collect real-time and batch data from multiple Formula 1 sources (RSS feeds, APIs, telemetry library, social media, weather) and deliver insights via predictive models and dashboards. The pipeline follows the Kappa architecture pattern, in which:

- Data flows continuously through a streaming layer (Kafka → Spark).
- All processing happens in streaming mode; there is no separate batch layer.
- A data lake stores raw events, while a serving layer provides clean tables for reporting.

Predictive models run on processed streams to generate forecasts such as race outcomes, pit stop strategies, or track conditions.

---

## Architecture

```text
Sources (RSS, FastF1, Twitter, OpenF1, OpenWeather)
          ↓
    Kafka Topics (rss_news, telemetry, tweets, weather)
          ↓
Spark Structured Streaming Jobs
          ↓
  ┌───────────────┬───────────────┐
  │  Data Lake    │  Serving DB   │
  │ (MongoDB/S3)  │ (PostgreSQL)  │
  └───────────────┴───────────────┘
          ↓               ↓
  Model Training    Power BI Reports
```

1. **Ingestion**: Python scripts publish raw data into Kafka topics.
2. **Streaming**: Spark processes events in real time (parsing, cleaning, aggregating).
3. **Data Lake**: Raw events are persisted for auditing and reprocessing.
4. **Serving Database**: Structured tables in PostgreSQL support fast queries.
5. **Visualization**: Power BI connects to PostgreSQL, building dashboards and reports.

---

## Repository Structure

```
f1-prediction/
├── config/           # Configuration files and templates
├── docs/             # Architecture diagrams and documentation
├── ingestion/        # Kafka producer scripts for each data source
├── models/           # Machine learning training and evaluation code
├── notebooks/        # Jupyter notebooks for exploration and prototyping
├── processing/       # Spark streaming jobs and ETL scripts
├── utils/            # Shared utilities (parsers, logging, helpers)
├── visualization/    # Power BI files and dashboard definitions
├── .env              # Environment variables (not committed)
├── .gitignore        # Files and folders to ignore in Git
├── docker-compose.yml# Local development stack (Kafka, Postgres)
├── requirements.txt  # Python dependencies
└── README.md         # Project overview and setup instructions
```

---

## Prerequisites

- **Python 3.8+**
- **Docker & Docker Compose**
- **Power BI Desktop** (for report development)
- **Git**

---

## Installation and Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/f1-prediction.git
   cd f1-prediction
   ```

2. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate    # Linux/macOS
   venv\Scripts\activate     # Windows
   ```

3. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Copy and edit the environment template:
   ```bash
   cp .env.example .env
   # Edit .env to add real API keys and credentials
   ```

5. Start local services:
   ```bash
   docker-compose up -d
   ```

---

## Running the Pipeline

1. Publish raw data to Kafka (example for RSS feeds):
   ```bash
   python ingestion/rss_ingestor.py
   ```

2. Launch Spark streaming jobs (example for RSS processing):
   ```bash
   python processing/stream_processor.py
   ```

3. Verify data in PostgreSQL (connect using psql or GUI):
   ```sql
   SELECT * FROM f1.rss_articles LIMIT 10;
   ```

4. Run any additional ingestion or processing scripts in similar fashion.

---

## Power BI Integration

1. Open Power BI Desktop.
2. Choose **Get Data → PostgreSQL database**.
3. Fill in connection details (`localhost`, `f1_insights`, credentials).
4. Import or use DirectQuery for real-time data.
5. Build visuals, apply filters, and save the `.pbix` file in `visualization/`.

---

## Development Workflow

- **Feature Branches**: Create a new branch for each feature or bugfix:
  ```bash
  git checkout -b feature/new-source-ingestion
  ```

- **Commit Messages**: Use present-tense, imperative style:
  ```
  Add Twitter ingestion script
  ```

- **Pull Requests**: Open a PR against `main`, include description and testing steps.

- **Code Reviews**: Assign reviewers, address feedback, squash and merge.

---

## Contributing

Contributions are welcome. Please:

1. Fork the repository.
2. Create a feature branch.
3. Commit your changes.
4. Push to your fork and submit a pull request.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

