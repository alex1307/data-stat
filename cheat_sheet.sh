#!/bin/bash

hey -n 10000 -c 25 -m POST -T 'application/json' -A 'application/json' -D ~/filter.json http://localhost:3000/json

TARGET_CC=x86_64-linux-musl-gcc cargo build --release --target x86_64-unknown-linux-musl

ssh -i ~/.ssh/digital_ocean_ssh_key.key root@68.183.3.134
digitalocean

cargo update indexmap@2.6.0 --precise 2.5.0

scp -r src root@68.183.3.134:/vehicle-ui
cargo run -- --cert-dir /Users/matkat/Software/ehomeho.com
psql -U admin -d vehicles -h localhost -p 5432 -W 1234
\copy (SELECT * FROM "views"."Vehicles") TO '/Users/matkat/Software/Vehicles.csv' with (FORMAT CSV, HEADER,DELIMITER ';')
\copy (SELECT * FROM "views"."Prices") TO '/Users/matkat/Software/Prices.csv' with (FORMAT CSV, HEADER,DELIMITER ';');
\copy (SELECT * FROM "views"."EstimatedPrices") TO '/Users/matkat/Software/EstimatedPrices.csv' with (FORMAT CSV, HEADER,DELIMITER ';');



cargo update indexmap@2.6.0 --precise 2.5.0