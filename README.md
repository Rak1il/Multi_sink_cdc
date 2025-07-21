# Multi_sink_cdc
CDC Pipeline: PostgreSQL → Kafka (Debezium) → Spark → Cassandra + Redis + REST API

CDC Pipeline: PostgreSQL → Kafka (Debezium) → Spark → Cassandra + Redis + REST API
This project implements a full Change Data Capture (CDC) pipeline with the following stack:

🔷 PostgreSQL: source database

🔷 Debezium: logical decoding + Kafka Connect

🔷 Apache Kafka: event streaming

🔷 Apache Spark Structured Streaming: processing

🔷 Cassandra: sink #1

🔷 Redis: sink #2

🔷 REST API: sink #3
------------------------------------------------------------------------------------------------------------
Steps to set up
 1. Install Debezium PostgreSQL Kafka Connector

cd ~
wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.5.0.Final/debezium-connector-postgres-2.5.0.Final-plugin.tar.gz
mkdir -p ~/kafka/connectors
tar -xvzf debezium-connector-postgres-2.5.0.Final-plugin.tar.gz -C ~/kafka/connectors

sudo mkdir -p /opt/kafka/plugins/debezium-postgres
sudo cp ~/kafka/connectors/debezium-connector-postgres/*.jar /opt/kafka/plugins/debezium-postgres/

----------------------------------------------------------------------------------------------------------------
🔷 2. PostgreSQL setup
Edit config and enable logical replication:


sudo nano /etc/postgresql/16/main/postgresql.conf
# set: wal_level = logical

sudo systemctl restart postgresql
Create database and tables:

sudo -u postgres psql

CREATE DATABASE engagement_db;
\c engagement_db

CREATE TABLE content (
    id UUID PRIMARY KEY,
    slug TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    content_type TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')),
    length_seconds INTEGER,
    publish_ts TIMESTAMPTZ NOT NULL
);

CREATE TABLE engagement_events (
    id BIGSERIAL PRIMARY KEY,
    content_id UUID REFERENCES content(id),
    user_id UUID,
    event_type TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')),
    event_ts TIMESTAMPTZ NOT NULL,
    duration_ms INTEGER,
    device TEXT,
    raw_payload JSONB
);

INSERT INTO engagement_events
(content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
VALUES
('4bb775ea-96f5-4ffa-80be-22e3b1c94570', gen_random_uuid(), 'play', now(), 7000, 'android', '{}');

CREATE PUBLICATION engagement_pub FOR TABLE engagement_events;

SELECT * FROM pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
🔷 3. Start Kafka & Zookeeper

sudo /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
sudo /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
Create Kafka topic:


sudo /opt/kafka/bin/kafka-topics.sh --create --topic engagement_project.public.engagement_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
🔷 4. Start Kafka Connect
bash
Copy
Edit
sudo /opt/kafka/bin/connect-distributed.sh /opt/kafka/config/connect-distributed.properties
Register Debezium CDC connector:


curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "engagement-cdc",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "plugin.name": "pgoutput",
    "database.hostname": "localhost",
    "database.port": "5432",
    "database.user": "mohammed",
    "database.password": "master",
    "database.dbname": "engagement_db",
    "database.server.name": "engagement_project",
    "topic.prefix": "engagement_project",
    "table.include.list": "public.engagement_events",
    "publication.name": "engagement_pub",
    "slot.name": "cdc_slot",
    "database.allowPublicKeyRetrieval": "true",
    "database.sslmode": "disable",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
  }
}'
Check status:


curl http://localhost:8083/connectors/engagement-cdc/status
🔷 5. Cassandra
Start Cassandra (docker or local):


docker start cassandra-hadoop
docker exec -it cassandra-hadoop cqlsh
Create keyspace and table:


CREATE KEYSPACE engagement WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE engagement;

CREATE TABLE engagement.engagement_events (
    id bigint PRIMARY KEY,
    content_id uuid,
    user_id uuid,
    event_type text,
    event_ts timestamp,
    duration_ms int,
    device text
);
🔷 6. Redis

sudo apt update
sudo apt install redis-server -y
redis-cli ping
🔷 7. REST API receiver
Install Flask:


pip install flask
Save to rest_api.py:

from flask import Flask, request

app = Flask(__name__)

@app.route('/api', methods=['POST'])
def receive():
    print(request.json)
    return '', 200

app.run(port=5000)
Run:


python3 rest_api.py
🔷 8. Spark job
Install Python dependencies

pip install redis cassandra-driver requests
Save as: spark_cdc_multi_sink.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from cassandra.cluster import Cluster
import redis
import requests
import time
import uuid

# Cassandra Sink
class CassandraSink:
    def __init__(self):
        cluster = Cluster(['127.0.0.1'])
        self.session = cluster.connect('engagement')

    def write_data(self, id, content_id, user_id, event_type, event_ts, duration_ms, device):
        query = """
        INSERT INTO engagement_events
        (id, content_id, user_id, event_type, event_ts, duration_ms, device)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        content_uuid = uuid.UUID(content_id) if content_id else None
        user_uuid = uuid.UUID(user_id) if user_id else None
        self.session.execute(query, (
            id, content_uuid, user_uuid, event_type, event_ts, duration_ms, device
        ))

r = redis.Redis(host='localhost', port=6379, db=0)
cass_sink = CassandraSink()

def send_to_rest(row):
    try:
        resp = requests.post("http://localhost:5000/api", json=row.asDict())
        if resp.status_code != 200:
            print(f"⚠️ REST API warning: {resp.status_code}")
    except Exception as e:
        print(f"❌ REST API Error: {e}")

spark = SparkSession.builder \
    .appName("CDC-MultiSink") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("payload", StructType([
        StructField("id", LongType()),
        StructField("content_id", StringType()),
        StructField("user_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_ts", StringType()),
        StructField("duration_ms", IntegerType()),
        StructField("device", StringType()),
        StructField("raw_payload", StringType())
    ]))
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "engagement_project.public.engagement_events") \
    .load()

value_df = df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = value_df.select(from_json(col("json_str"), schema).alias("data")).select("data.payload.*")

def process_batch(batch_df, batch_id):
    rows = batch_df.collect()
    print(f"📦 Batch {batch_id} - {len(rows)} rows")
    for row in rows:
        cass_id = row.id if row.id else int(time.time() * 1000)
        try:
            cass_sink.write_data(
                cass_id,
                row.content_id,
                row.user_id,
                row.event_type,
                row.event_ts,
                row.duration_ms,
                row.device
            )
            r.zincrby("engagement_10m", 1, row.content_id)
            send_to_rest(row)
        except Exception as e:
            print(f"❌ Error: {e}")

query = parsed_df.writeStream.foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/cdc_checkpoints") \
    .start()

query.awaitTermination()
Run it:


spark-submit --master local[*] spark_cdc_multi_sink.py
🧪 Validation
✅ Check Cassandra:

cqlsh> SELECT * FROM engagement.engagement_events;
✅ Check Redis:


redis-cli ZREVRANGE engagement_10m 0 5 WITHSCORES
✅ Test REST API:


curl -X POST http://localhost:5000/api -H "Content-Type: application/json" -d '{"message": "hello"}'
