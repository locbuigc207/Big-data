import os
import logging
from postgresql_client import PostgresSQLClient
from dotenv import load_dotenv


load_dotenv()
logging.basicConfig(level=logging.DEBUG)

def main():
    logging.info("Initializing PostgreSQL client....")
    pc = PostgresSQLClient(
        post=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    try:
        logging.info("Dropping existing tables")
        pc.drop_all()

        logging.info("Creating tables")
        pc.create_all()

        logging.info("Succesfully created tables")

    except Exception as e:
        logging.error(f"Failed to create table: {e}")

    
if __name__ == "__main__":
    main()
