from ETLPipeline import PostgresDatabaseConnection
from FakerGeneratorInterface import FakerGeneratorInterface
from ETLPipeline.utils.logger import logger
import random

class ProductGenerator(FakerGeneratorInterface):
    def __init__(self, connection: PostgresDatabaseConnection):
        self.conn = connection

    def numberUpdatedRecord(self) -> int:
        rate = random.uniform(0.01, 0.02)
        cur_manager = self.conn.connect()
        with cur_manager as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self.conn.database}.public.products;")
            row_count = int(cur.fetchone()[0]) * 1.0
            return round(rate * row_count)
        
    def numberInsertedRecord(self) -> int:
        pass
        
    def update(self, updated_row_count: int) -> str:
        logger.info("Start to update products unit price...")
        cur_manager = self.conn.connect()
        updated_products = []
        with cur_manager as cur:
            cur.execute(f"SELECT * FROM {self.conn.database}.public.products;")
            records = cur.fetchall()
            df_count = len(records)
            for row in range(updated_row_count):
                random_row = random.randint(0, df_count)
                random_rate = random.uniform(0, 0.3)
                id = records[random_row][0]
                unit_price = round(records[random_row][2] - (records[random_row][2] * random_rate), 2)
                cur.execute(f"UPDATE {self.conn.database}.public.products SET unit_price = {unit_price} WHERE id = '{id}';")
                updated_products.append(id)
        print(f"There are {updated_row_count} updated products and their ids: {updated_products}")
        return logger.info("Product unit price is updated!")
    
    def insert(self, inserted_row_count: int) -> str:
        pass

if __name__ == "__main__":
    connection = PostgresDatabaseConnection("localhost", 5432, "oltp", "oltp", "oltp")
    product_generator = ProductGenerator(connection)
    no_updated_row = product_generator.numberUpdatedRecord()
    product_generator.update(no_updated_row)