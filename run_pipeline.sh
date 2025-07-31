#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "--- Wrapper script started ---"

# Find the directory where this script is located
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

# IMPORTANT: Activate your conda environment.
# Replace '/opt/anaconda3' with your actual anaconda installation path if different.
# You can find it by running 'conda info --base' in your terminal.
source /opt/anaconda3/etc/profile.d/conda.sh
conda activate quant-pipeline-env

echo "--- Conda environment activated. Running Python script... ---"

# Run the actual Python pipeline script using its full path
# The "$@" allows passing arguments from the dashboard to the python script in the future
python "$SCRIPT_DIR/cli/run_pipeline.py" "$@"

echo "--- Python script finished. Wrapper script complete. ---"