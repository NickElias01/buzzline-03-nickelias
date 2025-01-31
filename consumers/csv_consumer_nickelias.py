import os
import json
import polars as pl
from collections import deque
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Load environment variables from .env
load_dotenv()

# Fetch .env variables
def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_stall_threshold() -> float:
    """Fetch message interval from environment or use default."""
    temp_variation = float(os.getenv("SMOKER_STALL_THRESHOLD_F", 0.2))
    logger.info(f"Max stall temperature range: {temp_variation} F")
    return temp_variation

def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

def check_temperature_alerts(df: pl.DataFrame) -> None:
    """Check if temperature has dropped or reached certain thresholds."""
    alerts = []
    
    # Check for temperature drops between consecutive rows
    for i in range(1, len(df)):
        if df[i, "temperature"] < df[i - 1, "temperature"]:
            alerts.append(f"Temperature dropped at timestamp {df[i, 'timestamp']}.")

    # Check for specific thresholds (Ready: 140°F, Overcooked: 150°F)
    for row in df.iter_rows():
        timestamp, temperature = row
        if temperature >= 150:
            alerts.append(f"Overcooked! Meat reached 150°F at timestamp {timestamp}.")
        elif temperature >= 140:
            alerts.append(f"Ready! Meat reached 140°F at timestamp {timestamp}.")

    # Print alerts if any
    for alert in alerts:
        logger.info(alert)

def detect_stall(rolling_window: deque) -> bool:
    """Detect a temperature stall based on the rolling window."""
    window_size = get_rolling_window_size()
    if len(rolling_window) < window_size:
        # Don't have a full window yet
        logger.debug(f"Rolling window size: {len(rolling_window)}. Waiting for {window_size}.")
        return False

    # Calculate the temperature range
    temp_range = max(rolling_window) - min(rolling_window)
    is_stalled = temp_range <= get_stall_threshold()
    logger.debug(f"Temperature range: {temp_range}°F. Stalled: {is_stalled}")
    return is_stalled

def process_message(message: str, rolling_window: deque, window_size: int, df: pl.DataFrame) -> None:
    """Process a single message and check for stalls or temperature alerts."""
    try:
        logger.debug(f"Raw message: {message}")
        data = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Add the new temperature reading to the rolling window
        rolling_window.append(temperature)

        # Append the new reading to the Polars DataFrame
        new_row = pl.DataFrame({"timestamp": [timestamp], "temperature": [temperature]})
        df = df.vstack(new_row)

        # Check for a stall
        if detect_stall(rolling_window):
            logger.info(f"STALL DETECTED at {timestamp}: Temp stable at {temperature}°F over last {window_size} readings.")

        # Check for temperature alerts (e.g., drop, ready, overcooked)
        check_temperature_alerts(df)

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

def main() -> None:
    """Main entry point for the consumer."""
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)
    df = pl.DataFrame({"timestamp": [], "temperature": []})

    # Create the Kafka consumer using the utility function
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size, df)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

if __name__ == "__main__":
    main()