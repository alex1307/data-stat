#!/bin/bash

# Define paths and database credentials
CSV_DIR="/Users/matkat/Software"
DB_USER="admin"
DB_NAME="vehicles"
DB_HOST="localhost"
DB_PORT="5432"
DB_PASSWORD="1234"

# Function to clean CSV files by removing all occurrences of double quotes (")
clean_csv() {
    sed -i '' 's/"//g' "$1"
}

# Exporting data from PostgreSQL to CSV files
export PGPASSWORD=$DB_PASSWORD



echo "Generating Vehicles.csv..."
psql -U $DB_USER -d $DB_NAME -h $DB_HOST -p $DB_PORT -c "\copy (SELECT * FROM \"views\".\"Vehicles\") TO '$CSV_DIR/Vehicles.csv' WITH (FORMAT CSV, HEADER, DELIMITER ';')"
clean_csv "$CSV_DIR/Vehicles.csv"

echo "Generating Prices.csv..."
psql -U $DB_USER -d $DB_NAME -h $DB_HOST -p $DB_PORT -c "\copy (SELECT * FROM \"views\".\"Prices\") TO '$CSV_DIR/Prices.csv' WITH (FORMAT CSV, HEADER, DELIMITER ';')"
clean_csv "$CSV_DIR/Prices.csv"

echo "Generating EstimatedPrices.csv..."
psql -U $DB_USER -d $DB_NAME -h $DB_HOST -p $DB_PORT -c "\copy (SELECT * FROM \"views\".\"EstimatedPrices\") TO '$CSV_DIR/EstimatedPrices.csv' WITH (FORMAT CSV, HEADER, DELIMITER ';')"
clean_csv "$CSV_DIR/EstimatedPrices.csv"

echo "CSV generation and cleanup complete."