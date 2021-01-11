#!/bin/bash

python /etl/src/central_financial.py
python /etl/src/remittances.py
python /etl/src/users.py
python /etl/src/cash_ops.py