from postgresql_client import PostgresSQLClient
from models import LMSEvent
from dotenv import load_dotenv
import logging
import pandas as pd
import os
from time import sleep


load_dotenv()

SAMPLE_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "data", ""
)

def load_data():
    try:
        logging.info(f"Loading data from {SAMPLE_DATA_PATH}")
        df = pd.read_parquet(SAMPLE_DATA_PATH, engine="fastparquet")
        records = []
        for _, row in df.iterrows():
            records.append(row)
        logging.info(f"Succesfully loaded {len(records)} records from {SAMPLE_DATA_PATH}")

        return records
        
    except Exception as e:
        logging.error(f"Error loading data {str(e)}")
        raise




def main():
    logging.info("Inserting data....")
    pc = PostgresSQLClient(
        post=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    logging.info("Successfully connected to PostgresSQL database")

    records = load_data()
    valid_records = 0
    invalid_records = 0

    session = pc.get_session()

    
    batch_size = 100
    current_batch = []

    def commit_batch(batch):
        nonlocal valid_records, invalid_records
        try:
            session.bulk_save_objects(batch)
            session.commit()
            valid_records += len(batch)
            logging.info(f"Processed {valid_records} valid records")
            sleep(0.5)
        except Exception as e:
            logging.error(f"Failed to insert batch of {len(batch)}: {str(e)}")
            invalid_records += len(batch)
            session.rollback()
        finally:
            batch.clear()

    logging.info("Starting record insertion")
    
    for record in records:
        current_batch.append(record)
        if len(current_batch) >= batch_size:
            commit_batch(current_batch)

    if current_batch:
        commit_batch(current_batch)
        valid_records += len(current_batch)
    
    session.close()
    logging.info("Finished record insertion\nFinal Summary:")
    logging.info(f"Total records processed: {len(records)}")
    logging.info(f"Valid records inserted: {valid_records}")
    logging.warning(f"Invalid records skipped: {invalid_records}")


