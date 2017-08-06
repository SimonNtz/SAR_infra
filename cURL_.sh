#!/bin/bash
curl -H "Content-Type: application/json" -X POST http://0.0.0.0:80/SLA_CLI -d $1 -i
curl -H "Content-Type: application/json" -X POST http://0.0.0.0:80/SLA_CLI -d {SLA: {requirements: [], product_list:[S1A_IW_GRDH_1SDV_20151226T182813_20151226T182838_009217_00D48F_5D5F], result: {}}
curl -H Content-Type: application/json -X POST http://0.0.0.0:80/SLA_CLI -d '{SLA: {requirements: [], product_list:[S1A_IW_GRDH_1SDV_20151226T182813_20151226T182838_009217_00D48F_5D5F], result: {}}'
curl -H Content-Type: application/json -X POST http://0.0.0.0:80/SLA_INIT -d '{product:S1A_IW_GRDH_1SDV_20151226T182813_20151226T182838_009217_00D48F_5D5F,
specs_vm: {mapper:[2,4,50], reducer:[1,0.5,10]}}'
