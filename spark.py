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
        # ✅ convert to UUID if not None
        content_uuid = uuid.UUID(content_id) if content_id else None
        user_uuid = uuid.UUID(user_id) if user_id else None

        self.session.execute(query, (
            id, content_uuid, user_uuid, event_type, event_ts, duration_ms, device
        ))

# Redis
r = redis.Redis(host='localhost', port=6379, db=0)

cass_sink = CassandraSink()

# REST API sender
def send_to_rest(row):
    try:
        resp = requests.post("http://localhost:5000/api", json=row.asDict())
        if resp.status_code != 200:
            print(f"⚠️ REST API warning: status_code={resp.status_code} body={resp.text}")
    except Exception as e:
        print(f" REST API Error: {e}")

# Initialize Spark
spark = SparkSession.builder \
    .appName("CDC-MultiSink") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka schema
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

# Read from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.39:9092") \
    .option("subscribe", "engagement_project.public.engagement_events") \
    .load()

value_df = df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = value_df.select(from_json(col("json_str"), schema).alias("data")).select("data.payload.*")

# Batch processor
def process_batch(batch_df, batch_id):
    rows = batch_df.collect()
    print(f"📦 Processing batch {batch_id} with {len(rows)} rows")

    for row in rows:
        cass_id = row.id if row.id is not None else int(time.time() * 1000)

        print(f"➡️ Row: {row.asDict()}")

        # Write to Cassandra
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
            print(f"✅ Cassandra: inserted row id={cass_id}")
        except Exception as e:
            print(f" Cassandra Error (row id={cass_id}): {e}")

        # Write to Redis
        try:
            if row.content_id:
                r.zincrby("engagement_10m", 1, row.content_id)
                print(f"✅ Redis: updated content_id={row.content_id}")
            else:
                print(f"⚠️ Skipping Redis: content_id is None for row id={cass_id}")
        except Exception as e:
            print(f" Redis Error (row id={cass_id}): {e}")

        # Send to REST API
        send_to_rest(row)

# Start stream
query = parsed_df.writeStream.foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/cdc_checkpoints") \
    .start()

query.awaitTermination()


