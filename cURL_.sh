#!/bin/bash
curl -H "Content-Type: application/json" -X POST http://0.0.0.0:80/SLA_CLI -d $1 -i