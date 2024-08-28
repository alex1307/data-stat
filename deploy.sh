#!/bin/bash

# Define the remote server details and SSH key path
REMOTE_USER="root"
REMOTE_HOST="68.183.3.134"
SSH_KEY_PATH="~/.ssh/digital_ocean_ssh_key.key"
REMOTE_DIR="/vehicle-app/resources/"
LOCAL_DIR="resources/*.csv"
SERVICE_NAME="vehicle-data"

perl -pi -e 's/"//g' resources/Prices.csv
perl -pi -e 's/"//g' resources/Vehicles.csv
perl -pi -e 's/"//g' resources/EstimatedPrices.csv

# Check if the --all parameter is provided
if [[ $1 == "--all" ]]; then
    # Execute additional commands
    echo "Building the project with cargo..."
    TARGET_CC=x86_64-linux-musl-gcc cargo build --release --target x86_64-unknown-linux-musl

    echo "Copying the binary to the remote server..."
    sleep 2
    scp -i $SSH_KEY_PATH -r target/x86_64-unknown-linux-musl/release/vehicle-data $REMOTE_USER@$REMOTE_HOST:/vehicle-app/
fi

# Copy CSV files to the remote server
echo "Copying CSV files to the remote server..."
scp -i $SSH_KEY_PATH -r $LOCAL_DIR $REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR

# SSH into the remote machine and execute the commands
echo "Restarting the $SERVICE_NAME service on the remote server..."
ssh -i $SSH_KEY_PATH $REMOTE_USER@$REMOTE_HOST << EOF
  cd /
  sudo systemctl stop $SERVICE_NAME
  sudo systemctl daemon-reload
  sudo systemctl start $SERVICE_NAME
  exit
EOF

echo "Deployment and service restart completed on $REMOTE_HOST."