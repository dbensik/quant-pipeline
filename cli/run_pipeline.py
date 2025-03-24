from data_pipeline.data_pipeline import DataPipeline
import matplotlib.pyplot as plt
import sys

def main():
    # Instantiate the pipeline
    pipeline = DataPipeline(
        ticker="SPY",
        start_date="2018-01-01",
        end_date="2024-12-31"
    )
    
    # Run the pipeline steps
    pipeline.fetch_data()
    pipeline.clean_data()
    pipeline.save_data(db_path="quant_pipeline.db", table_name="price_data")
    
    # Plot the cleaned data
    plt.figure(figsize=(10,6))
    plt.plot(pipeline.data.index, pipeline.data['Close'], label='Close Price')
    plt.title("SPY Closing Price Over Time")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    
    # Only display the plot if not running in a Jupyter/IPython environment
    if 'ipykernel' not in sys.modules:
        plt.show()
    
    print("Pipeline run complete!")

if __name__ == "__main__":
    main()