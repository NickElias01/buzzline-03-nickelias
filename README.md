# buzzline-03-nickelias

Streaming data does not have to be simple text.
Many of us are familiar with streaming video content and audio (e.g. music) files. 

Streaming data can be structured (e.g. csv files) or
semi-structured (e.g. json data). 

We'll work with two different types of data, and so we'll use two different Kafka topic names. 
See [.env](.env). 


## Starting Zookeeper and Kafka:
### Zookeeper - Terminal 1
```shell
wsl
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Kafka - Terminal 2
```shell
wsl
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

# Kafka JSON Message Processor

## Introduction

This project implements a Kafka-based messaging pipeline to efficiently produce, consume, and analyze JSON messages. It consists of:

- **JSON Producer**: Reads structured JSON data from a file using `Polars` and streams it to a Kafka topic.
- **JSON Consumer**: Listens for messages on the Kafka topic, processes them, and applies real-time analytics, including author tracking, frequency monitoring, and alerting based on message content.

### Key Features
- **Efficient Data Handling**: Uses `Polars` for fast JSON parsing.
- **Real-time Monitoring**: Detects frequent messages and tracks author activity.
- **Custom Alerts**: Flags urgent messages and high-volume senders.

This setup is ideal for workplace message tracking, monitoring system logs, or processing structured Kafka events.

---


# CSV Message Processor  

## Overview  
The **CSV Message Processor** is a Kafka consumer script designed to ingest and analyze temperature data from a Kafka topic. The script monitors incoming messages, processes the temperature readings, and detects potential stalls based on a rolling window of recent values.  

## Features  
- Consumes JSON-encoded temperature messages from a Kafka topic.  
- Maintains a rolling window of recent temperature readings using a deque.  
- Detects stalls when temperature variation falls below a configurable threshold.  
- Logs key events, including message consumption, processing, and stall detection.  
- Configurable through environment variables for flexibility in different deployments.  



## About the Smart Smoker (CSV Example)

A food stall occurs when the internal temperature of food plateaus or 
stops rising during slow cooking, typically between 150°F and 170°F. 
This happens due to evaporative cooling as moisture escapes from the 
surface of the food. The plateau can last for hours, requiring 
adjustments like wrapping the food or raising the cooking temperature to 
overcome it. Cooking should continue until the food reaches the 
appropriate internal temperature for safe and proper doneness.

The producer simulates a smart food thermometer, sending a temperature 
reading every 15 seconds. The consumer monitors these messages and 
maintains a time window of the last 5 readings. 
If the temperature varies by less than 2 degrees, the consumer alerts 
the BBQ master that a stall has been detected. This time window helps 
capture recent trends while filtering out minor fluctuations.

## Later Work Sessions
When resuming work on this project:
1. Open the folder in VS Code. 
2. Start the Zookeeper service.
3. Start the Kafka service.
4. Activate your local project virtual environment (.env).

## Save Space
To save disk space, you can delete the .venv folder when not actively working on this project.
You can always recreate it, activate it, and reinstall the necessary packages later. 
Managing Python virtual environments is a valuable skill. 

## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.
