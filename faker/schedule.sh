#!/usr/bin/env bash
echo "Set jobs for faker-generator"

cd /home/admin/my-first-elt-project
source .venv/bin/activate

echo "Start to generate data"
python /home/admin/my-first-elt-project/faker/UserGenerator.py >> /home/admin/my-first-elt-project/monitoring/logs/user_logs.txt
python /home/admin/my-first-elt-project/faker/ProductGenerator.py >> /home/admin/my-first-elt-project/monitoring/logs/product_logs.txt
python /home/admin/my-first-elt-project/faker/TransactionGenerator.py >> /home/admin/my-first-elt-project/monitoring/logs/transaction_logs.txt