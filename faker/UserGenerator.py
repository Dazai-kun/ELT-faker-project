from FakerGeneratorInterface import FakerGeneratorInterface
from ETLPipeline import PostgresDatabaseConnection
from ETLPipeline.utils.logger import logger
from faker import Faker
from datetime import datetime
import random
import string

class UserGenerator(FakerGeneratorInterface):
    def __init__(self, connection: PostgresDatabaseConnection):
        self.conn = connection

    def numberUpdatedRecord(self) -> int:
        rate = random.uniform(0.05, 0.10)
        cur_manager = self.conn.connect()
        with cur_manager as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self.conn.database}.public.users;")
            row_count = int(cur.fetchone()[0]) * 1.0
            return round(rate * row_count)
        
    def numberInsertedRecord(self) -> int:
        rate = random.uniform(0.10, 0.15)
        cur_manager = self.conn.connect()
        with cur_manager as cur:
            cur.execute(f"SELECT COUNT(*) FROM {self.conn.database}.public.users;")
            row_count = int(cur.fetchone()[0]) * 1.0
            return round(rate * row_count)
        
    def update(self, updated_row_count: int) -> str:
        logger.info("Start to update users email...")
        cur_manager = self.conn.connect()
        with cur_manager as cur:
            cur.execute(f"SELECT * FROM {self.conn.database}.public.users;")
            df = cur.fetchall()
            df_count = len(df)
            faker = Faker()
            updated_id = []
            for row in range(updated_row_count):
                random_row = random.randint(0, df_count)
                id = df[random_row][0]
                email = faker.email()
                cur.execute(f"UPDATE {self.conn.database}.public.users SET email = '{email}' WHERE id = '{id}';")
                updated_id.append(id)
        print(f"There are {updated_row_count} updated users and their ids: {updated_id}")
        return logger.info("User email is updated!")
    
    def insert(self, inserted_row_count: int) -> str:
        logger.info("Start to generate new users...")
        cur_manager = self.conn.connect()
        faker = Faker()
        values = []
        with cur_manager as cur:
            for row in range(inserted_row_count):
                id = random.choice(string.ascii_letters.upper()) + random.choice(string.ascii_letters.upper()) + str(random.randint(0, 9)) + str(random.randint(0, 9)) + str(random.randint(0, 9)) + str(random.randint(0, 9))
                name = faker.name()
                email = faker.email()
                created_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                record = (id, name, email, created_at)
                values.append(str(record))
            query = f"INSERT INTO {self.conn.database}.public.users (id, name, email, created_at) VALUES {', '.join(values)};"
            cur.execute(query)
        return logger.info("New users are inserted!")
    
if __name__ == "__main__":
    connection = PostgresDatabaseConnection("localhost", 5432, "oltp", "oltp", "oltp")
    user_generator = UserGenerator(connection)
    no_updated_row = user_generator.numberUpdatedRecord()
    no_inserted_row = user_generator.numberInsertedRecord()
    user_generator.update(no_updated_row)
    user_generator.insert(no_inserted_row)