#!/bin/bash

# Define the remote server details and SSH key path
REMOTE_USER="root"
REMOTE_HOST="68.183.3.134"
SSH_KEY_PATH="~/.ssh/digital_ocean_ssh_key.key"
REMOTE_DIR="/vehicle-app/resources/"
LOCAL_DIR="resources/*.csv"

# Define the service name
SERVICE_NAME="vehicle-data"

# Copy CSV files to the remote server
scp -i $SSH_KEY_PATH -r $LOCAL_DIR $REMOTE_USER@$REMOTE_HOST:$REMOTE_DIR

# SSH into the remote machine and execute the commands
ssh -i $SSH_KEY_PATH $REMOTE_USER@$REMOTE_HOST << EOF
  cd /
  sudo systemctl stop $SERVICE_NAME
  sudo systemctl daemon-reload
  sudo systemctl start $SERVICE_NAME
  exit
EOF

echo "CSV files have been copied and service $SERVICE_NAME has been restarted on $REMOTE_HOST."