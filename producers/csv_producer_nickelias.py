#####################################
# Import Modules
#####################################

import os
import sys
import time
import pathlib
import json
import polars as pl  # Use polars for CSV handling
from dotenv import load_dotenv

from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    interval = int(os.getenv("SMOKER_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("smoker_temps.csv")

#####################################
# Message Generator using Polars
#####################################

def generate_messages(file_path: pathlib.Path):
    while True:
        try:
            logger.info(f"Reading data file: {file_path}")
            df = pl.read_csv(file_path)
            
            required_columns = ["timestamp", "temperature"]
            if not all(col in df.columns for col in required_columns):
                logger.error(f"Missing required columns in data file: {file_path}")
                sys.exit(1)
            
            for row in df.iter_rows(named=True):
                message = {
                    "timestamp": row["timestamp"],
                    "temperature": float(row["temperature"]),
                }
                logger.debug(f"Generated message: {message}")
                yield message
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)

#####################################
# Main Function
#####################################

def main():
    logger.info("START producer.")
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    producer = create_kafka_producer(value_serializer=lambda x: json.dumps(x).encode("utf-8"))
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for message in generate_messages(DATA_FILE):
            producer.send(topic, value=message)
            logger.info(f"Sent message to topic '{topic}': {message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END producer.")

if __name__ == "__main__":
    main()