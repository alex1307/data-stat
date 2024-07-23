#!/bin/bash

hey -n 10000 -c 25 -m POST -T 'application/json' -A 'application/json' -D ~/filter.json http://localhost:3000/json

TARGET_CC=x86_64-linux-musl-gcc cargo build --release --target x86_64-unknown-linux-musl

ssh -i ~/.ssh/digital_ocean_ssh_key.key root@68.183.3.134
digitalocean

scp -r src root@68.183.3.134:/vehicle-ui
cargo run -- --cert-dir /Users/matkat/Software/ehomeho.com