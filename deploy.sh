#!/bin/bash

TARGET_CC=x86_64-linux-musl-gcc cargo build --release --target x86_64-unknown-linux-musl
echo "Copying files to remote server"
sleep 5
scp -r target/x86_64-unknown-linux-musl/release/vehicle-data root@68.183.3.134:/vehicle-app/
scp -r resources/VehicleEstimatedPrice.csv root@68.183.3.134:/vehicle-app/resources/
