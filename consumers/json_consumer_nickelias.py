"""
json_consumer_nickelias.py

Consume json messages from a Kafka topic and process them.

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting author occurrences
import time  # to add time-based alerts

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> int:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up Data Store to hold author counts
#####################################

# Initialize a dictionary to store author counts
# The defaultdict type initializes counts to 0
# pass in the int function as the default_factory
# to ensure counts are integers
author_counts = defaultdict(int)

# Set up a dictionary to track the time of the last message for each author
last_message_time = defaultdict(int)

# Define an alert threshold (for example, 100 messages from an author)
MESSAGE_THRESHOLD = 100

# Define a time-based alert threshold (e.g., more than 5 messages in 10 seconds)
FREQUENT_THRESHOLD = 5
FREQUENT_TIME_FRAME = 10  # seconds


#####################################
# Function to process a single message
#####################################

def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)

        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):
            # Extract the 'author' field from the Python dictionary
            author = message_dict.get("author", "unknown")
            message_content = message_dict.get("message", "")

            # Increment the count for the author
            author_counts[author] += 1

            # Log the updated counts
            logger.info(f"Updated author counts: {dict(author_counts)}")

            # Time-based alert (detect frequent messages)
            current_time = time.time()
            last_time = last_message_time.get(author, 0)
            if current_time - last_time <= FREQUENT_TIME_FRAME:
                logger.warning(f"Frequent message alert: {author} sent multiple messages in less than {FREQUENT_TIME_FRAME} seconds.")

            last_message_time[author] = current_time

            # Alert on specific author (e.g., important person like "manager" or "team lead")
            if author.lower() == "alice":
                logger.warning(f"ALERT: Message received from important author '{author}'!")

            # Message threshold alert (if an author exceeds a certain number of messages)
            if author_counts[author] >= MESSAGE_THRESHOLD:
                logger.warning(f"ALERT: {author} has sent {author_counts[author]} messages! Consider reviewing.")

            # Message content analysis alert (check for specific keywords like 'urgent', 'error')
            if "urgent" in message_content.lower() or "error" in message_content.lower():
                logger.warning(f"ALERT: Message contains an important keyword from {author}: {message_content}")

        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Performs analytics on messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
