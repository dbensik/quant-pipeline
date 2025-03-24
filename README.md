# Quant Pipeline

## Overview

**Quant Pipeline** is an end-to-end project designed to fetch, clean, store, and analyze financial data using Python. The project integrates data collection from yfinance, data cleaning with pandas, visualization with matplotlib, and data storage in an SQLite database. An API layer (via FastAPI) and a CLI runner are also included to demonstrate a full-stack quantitative analysis workflow.

## Folder Structure
quant-pipeline/
├── cli/
│   └── run_pipeline.py         # CLI runner to execute the pipeline
├── data_pipeline/
│   ├── init.py             # Package initializer
│   └── data_pipeline.py        # Contains the DataPipeline class
├── data/                       # (Optional) Folder for storing additional data files
├── notebooks/
│   └── 01_data_collection_and_cleaning.ipynb  # Notebook for data fetching and cleaning
├── tests/                      # (Optional) Unit tests for your project
├── setup.py                    # Setup script for packaging the project
├── pyproject.toml              # PEP 517 configuration file
└── README.md                   # This file

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/your-username/quant-pipeline.git
   cd quant-pipeline

2.	**Create and activate your conda environment:**

    ```bash
    conda create -n quant-pipeline-env python=3.11
    conda activate quant-pipeline-env
    ```

3.	**Install dependencies in editable mode:**

    ```bash
    pip install -e .
    ```

## Usage

1. **Running the Notebook**

	1. Navigate to the notebooks folder:
        ```bash
        cd notebooks
        ```

    2. Start Jupyter Notebook:
        ```bash
        jupyter notebook
        ```
        
    Then open 01_data_collection_and_cleaning.ipynb to run and view your data pipeline workflow.

2. **Running the CLI Runner**

    1. From the project root, run:
        ```bash
       python cli/run_pipeline.py
       ```

    This will execute the full pipeline:
	•	Fetch and clean data.
	•	Save the data to an SQLite database (quant_pipeline.db).
	•	Execute a sample SQL query (if implemented) and plot the closing price.

3. **Database Inspection**

    1. You can inspect the SQLite database using the SQLite shell:
       ```bash
       sqlite3 quant_pipeline.db
       ```

    2. Once in the shell, list tables with:
      ```sql
      .tables
      ```
          
    3. And view the schema for the price_data table with:
        ```sql
        PRAGMA table_info(price_data);
        ```

4. **Contributing**

    Feel free to fork the repository and submit pull requests for improvements, bug fixes, or additional features.

5. **License**

[MIT License](https://opensource.org/license/mit)
    