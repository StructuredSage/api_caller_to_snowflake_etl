#!/bin/bash

# Source the environment variables from .envdata
source /serverPath/.envdata

# Navigate to script's directory
cd /serverPath/snowflake

# Activate the virtual environment
source .venv/bin/activate

# Run python script with all arguments
python main.py "$@"
status=$? # Save the exit status of the script

# Deactivate the virtual environment
deactivate

# Exit with the saved status
exit $status