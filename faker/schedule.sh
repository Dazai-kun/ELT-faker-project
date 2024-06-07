#!/usr/bin/env bash
echo "Set jobs for faker-generator"

cd /home/huymonkey/on-premise-data-pipeline
source .venv/bin/activate

echo "Start to generate data"
python /home/huymonkey/on-premise-data-pipeline/faker/UserGenerator.py >> /home/huymonkey/on-premise-data-pipeline/monitoring/logs/user_logs.txt
python /home/huymonkey/on-premise-data-pipeline/faker/ProductGenerator.py >> /home/huymonkey/on-premise-data-pipeline/monitoring/logs/product_logs.txt
python /home/huymonkey/on-premise-data-pipeline/faker/TransactionGenerator.py >> /home/huymonkey/on-premise-data-pipeline/monitoring/logs/transaction_logs.txt