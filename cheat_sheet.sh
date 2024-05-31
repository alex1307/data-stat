#!/bin/bash

hey -n 10000 -c 25 -m POST -T 'application/json' -A 'application/json' -D ~/filter.json http://localhost:3000/json