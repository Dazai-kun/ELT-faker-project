from ETLPipeline import PostgresDatabaseConnection
from FakerGeneratorInterface import FakerGeneratorInterface
from ETLPipeline.utils.logger import logger
from datetime import datetime
import random
import string

class TransactionGenerator(FakerGeneratorInterface):
    def __init__(self, connection: PostgresDatabaseConnection):
        self.conn = connection

    def numberUpdatedRecord(self) -> int:
        pass

    def numberInsertedRecord(self) -> int:
        row_count = random.randint(5, 10)
        return row_count

    def update(self, updated_row_count: int) -> str:
        pass

    def insert(self, inserted_row_count: int) -> str:
        logger.info("Start to generate new transactions...")
        cur_manager = self.conn.connect()
        values = []
        with cur_manager as cur:
            # Count the current number of products in database
            cur.execute(f"SELECT * FROM {self.conn.database}.public.products;")
            products_record = cur.fetchall()
            products_count = len(products_record)
            for row in range(inserted_row_count):
                # Create a random number of products a customer buy
                no_of_product_item = random.randint(1, 3)
                # Create a transaction id for new record
                transaction_id = random.choice(string.ascii_letters.upper()) + random.choice(string.ascii_letters.upper()) + str(random.randint(0, 9)) + str(random.randint(0, 9)) + str(random.randint(0, 9)) + str(random.randint(0, 9)) + str(random.randint(0, 9)) + str(random.randint(0, 9))
                values.append(transaction_id)
                # Create transaction date
                transaction_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Process logic for each product a customer buy
                total_amount_list = []
                transaction_detail_records = []
                for i in range(no_of_product_item):
                    # Choose a random product_id for creating new transaction
                    product_random = products_record[random.randint(0, products_count - 1)]
                    product_id = product_random[0]
                    # Create random quantity of that product
                    quantity = random.randint(1, 3)
                    # Calculate total price of that product by multiply the price to quantity bought by customer
                    total_amount_for_each_product = round(quantity * product_random[2], 2)
                    total_amount_list.append(total_amount_for_each_product)
                    # Create record of transaction_detail for a transaction
                    record = (transaction_id, product_id, quantity, total_amount_for_each_product)
                    transaction_detail_records.append(str(record))
                total_amount = sum(total_amount_list)
                cash_received = random.randint(round(total_amount, 0), round(total_amount + 100, 0))
                change_due = cash_received - total_amount
                cur.execute(f"SELECT id FROM {self.conn.database}.public.users ORDER BY RANDOM() LIMIT 1;")
                user_id = cur.fetchone()[0]
                transaction_record = str((transaction_id, transaction_date, total_amount, cash_received, change_due, user_id))
                # Insert new transaction
                cur.execute(f"INSERT INTO {self.conn.database}.public.transactions (id, transaction_date, total_amount, cash_received, change_due, user_id) VALUES {str(transaction_record)};")
                logger.info("New transaction is inserted!")
                # Insert transaction_detail
                cur.execute(f"INSERT INTO {self.conn.database}.public.transaction_detail (transaction_id, product_id, quantity, total_amount) VALUES {', '.join(transaction_detail_records)};")
                logger.info("New transaction_detail is inserted!")
        print(f"There are {inserted_row_count} new transactions and their ids: {values}")
        return logger.info("New transaction is inserted!")
    

if __name__ == "__main__":
    connection = PostgresDatabaseConnection("localhost", 5432, "oltp", "oltp", "oltp")
    transaction_generator = TransactionGenerator(connection)
    no_inserted_row = transaction_generator.numberInsertedRecord()
    transaction_generator.insert(no_inserted_row)