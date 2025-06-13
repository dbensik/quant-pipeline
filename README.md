# Quant Pipeline
Quant Pipeline is an end-to-end modular framework for developing, backtesting, and deploying systematic trading strategies using both traditional and machine learning approaches.

## Overview

**Quant Pipeline** 
The project is organized into several key components:

- **Alpha Models (alpha_models/):**  
  Contains traditional strategy modules. This folder includes:
  - `index_rebalancing.py`: Placeholder for an Index Rebalancing Strategy.
  - `pairs_trading.py`: Placeholder for a Pairs Trading Strategy.
  - `basket_trading.py`: Placeholder for a Basket Trading Strategy.

- **API Layer (api_layer/):**  
  Contains code to expose the pipeline’s functionality via an API (using FastAPI, for example).

- **Backtesting (backtesting/):**  
  Contains the backtesting framework (e.g., `backtester.py`) to simulate historical performance of the strategies.

- **CLI (cli/):**  
  Contains the CLI runner (`run_pipeline.py`) to execute the complete pipeline from the command line.

- **Dashboard App (dashboard_app/):**  
  Contains code for a dashboard interface to display key metrics and visualizations (e.g., using Dash or Streamlit).

- **Data Pipeline (data_pipeline/):**  
  Contains the core functionality for fetching and cleaning market data:
  - `__init__.py`: Package initializer.
  - `data_pipeline.py`: Defines the `DataPipeline` class for data fetching, cleaning, and saving.

- **Data (data/):**  
  An optional folder for storing additional data files if needed.

- **Machine Learning Models (ml_models/):**  
  Contains machine learning components for predictive modeling:
  - `eda.py`: Updated module to load data from CSV or the database and perform EDA.
  - `model_training.py`: Contains routines for training and evaluating ML models.
  - `signal_generation.py`: Converts model outputs into actionable trading signals.

- **Notebooks (notebooks/):**  
  Contains Jupyter notebooks for interactive work and research:
  - `01_data_collection_and_cleaning.ipynb`: Notebook for data fetching and cleaning.
  - `02_alpha_research.ipynb`: Notebook for alpha research and signal generation.

- **Project Setup Files:**
  - `environment.yml`: Conda environment specification.
  - `pyproject.toml`: PEP 517 configuration file for modern build systems.
  - `setup.py`: Setup script for packaging the project.
  - `.gitignore`: Specifies files and directories to exclude from version control.

- **Database:**
  - `quant_pipeline.db`: The SQLite database file (created at runtime).

- **Tests (tests/):**  
  An optional folder for unit tests to ensure your code behaves as expected.


## Folder Structure
```
quant-pipeline/
├── alpha_models/              # Traditional strategy modules
│   ├── index_rebalancing.py      # New file for Index Rebalancing Strategy
│   ├── pairs_trading.py          # New file for Pairs Trading Strategy
│   └── basket_trading.py         # New file for Basket Trading Strategy
├── api_layer/
├── backtesting/               # Backtesting framework (optional)
│   └── backtester.py
├── cli/
│   └── run_pipeline.py         # CLI runner to execute the pipeline
├── dashboard_app/
├── data_pipeline/
│   ├── init.py             # Package initializer
│   └── data_pipeline.py        # Contains the DataPipeline class
├── data/                       # (Optional) Folder for storing additional data files
├── environment.yml
├── ml_models/                 # Machine learning components
│   ├── eda.py                  # Updated to load data from CSV or DB
│   ├── model_training.py       # Contains model training routines
│   └── signal_generation.py    # Converts model outputs to trading signals
├── notebooks/
│   └── 01_data_collection_and_cleaning.ipynb  # Notebook for data fetching and cleaning
|   └── 02_alpha_research.ipynp # Notebook for alpha and signal generation research
├── pyproject.toml              # PEP 517 configuration file
├── quant_pipeline.db
├── README.md                   # This file
├── setup.py                    # Setup script for packaging the project
└── tests/                      # (Optional) Unit tests for your project
    └── __main__.py
```


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
    
