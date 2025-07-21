# Kafka Demo

## Installation

To run kafka locally, follow the following steps:

NOTE: Requires Java 11 or higher

1. Expand the tarball:

```bash
tar -xzf kafka_2.13-3.9.0.tgz
cd kafka_2.13-3.9.0
```

2. Create KRaft store:

```bash
export CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format \
    -t $CLUSTER_ID \
    -c config/kraft/server.properties
```

3. Startup the broker:

```bash
bin/kafka-server-start.sh config/kraft/server.properties
```

4. Check for it to be running:

[Kafka Web](http://localhost:9092)

It should return an error at this point.

## Create the topic for the examples:

Do this in a separate terminal:

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
                    --create --topic temperatura --partitions 1 --replication-factor 1
```

NOTE: You can also remove the topic using the following command, and then recreate it:

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic temperatura --delete
```

## Validate topic:

Open a terminal for each the producer and the consumer.

### Producer:

```bash
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic temperatura
```
Then enter:
```
{"sensor":"s1","value":25}
```

### Consumer:

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic temperatura --from-beginning
```

## Kafka Demos:

### Prerequisites

Make sure you have kafka-python installer:

```bash
pip install kafka-python
```

### Demo 1: Simple Producer / Consumer

Run the producer script, which will generate a "temperature reading" every second until stopped:

```bash
python producer.py
```

Then we have the notebook `22 Kafka Consumer.ipynb` which will consume from that queue, execute the notebook and it should show the messages being received.

### Demo 2: Kafka + Spark Structured Streaming

NOTE: Assumes you have Spark installed locally and can execute it. Spark steup is beyond the scope of this document.

1. Run the notebook `23 Kafka Structured Streaming.ipynb`
2. Keep sending message from the `producer.py` program
3. Should be displayed in the notebook.


