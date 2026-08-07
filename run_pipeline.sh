#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "--- Wrapper script started ---"

# Find the directory where this script is located
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

# Change to the script directory (project root) to ensure imports work correctly
cd "$SCRIPT_DIR"

# Environment activation — Poetry only (conda fallback removed 2026-07-31 after
# confirming the Poetry flow works on this machine).
if command -v poetry &> /dev/null && VENV_PATH="$(poetry env info --path 2> /dev/null)" && [ -n "$VENV_PATH" ] && [ -f "$VENV_PATH/bin/activate" ]; then
    # shellcheck disable=SC1091
    source "$VENV_PATH/bin/activate"
    echo "--- Poetry environment activated ($VENV_PATH). ---"
else
    echo "ERROR: Poetry environment not found."
    echo "  Fix: cd into the project root and run 'poetry install'"
    echo "  (install Poetry first if needed: brew install poetry)"
    exit 1
fi

# Check if the first argument is "api" to run the API server
if [ "$1" == "api" ] || [ "$1" == "gateway" ]; then
    echo "--- Starting GraphQL Gateway... ---"
    shift
    # Run strawberry server (default port 8000)
    strawberry dev services.graphql_gateway.schema --host 127.0.0.1 --port 8000
elif [ "$1" == "dashboard" ] || [ "$1" == "ui" ]; then
    echo "--- Starting Streamlit Dashboard... ---"
    shift
    # Add the current directory to PYTHONPATH so that 'dashboard_app' module can be found
    export PYTHONPATH=$PYTHONPATH:$(pwd)
    # Run Streamlit
    streamlit run dashboard_app/dashboard.py "$@"
elif [ "$1" == "grpc" ]; then
    echo "--- Starting gRPC Server... ---"
    python -m services.grpc_service.server
elif [ "$1" == "verify" ]; then
    echo "--- Running Verification / GraphQL Client... ---"
    python verify_all.py
elif [ "$1" == "all" ]; then
    echo "--- Starting ALL Services (gRPC, API, Dashboard) + Verification ---"
    
    # Function to handle script exit (kill background processes)
    cleanup() {
        echo "Stopping all services..."
        kill 0
    }
    # Trap SIGINT (Ctrl+C) and EXIT
    trap cleanup SIGINT EXIT

    echo "1. Starting gRPC Server..."
    python -m services.grpc_service.server &
    
    echo "2. Starting GraphQL Gateway..."
    strawberry dev services.graphql_gateway.schema --host 127.0.0.1 --port 8000 &

    echo "3. Starting Streamlit Dashboard..."
    export PYTHONPATH=$PYTHONPATH:$(pwd)
    streamlit run dashboard_app/dashboard.py &

    echo "--- Services launched in background. Waiting 5s for startup... ---"
    sleep 5

    echo "--- Running Verification ---"
    python verify_all.py

    echo "--- All systems go! Press Ctrl+C to stop. ---"
    wait
else
    echo "--- Running Data Pipeline... ---"
    # Run the pipeline as a module to ensure imports work correctly
    # The "$@" allows passing arguments (like --full-backfill)
    python -m cli.run_pipeline "$@"
fi

echo "--- Process finished. Wrapper script complete. ---"