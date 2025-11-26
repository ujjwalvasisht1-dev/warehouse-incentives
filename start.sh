#!/bin/bash
# Start script for Warehouse Incentive System

echo "Starting Warehouse Incentive System..."
echo ""

# Check if database exists
if [ ! -f "incentives.db" ]; then
    echo "Database not found. Running setup..."
    python3 setup.py
    echo ""
fi

# Start Flask application
echo "Starting Flask application..."
echo "Access the application at: http://localhost:5001"
echo ""
python3 app.py

