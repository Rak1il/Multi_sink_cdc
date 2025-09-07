# 📊 Multi Sink CDC Pipeline

**CDC Pipeline:** PostgreSQL → Kafka (Debezium) → Spark → Cassandra + Redis + REST API

This project implements a full **Change Data Capture (CDC)** pipeline designed to capture and stream real-time changes from a PostgreSQL database through Kafka, process them with Spark, and sink the results into Cassandra, Redis, and a REST API.

---

## ⚙️ Tech Stack

- **PostgreSQL** – source database  
- **Debezium** – CDC via logical decoding + Kafka Connect  
- **Apache Kafka** – event streaming platform  
- **Apache Spark Structured Streaming** – stream processing engine  
- **Cassandra** – sink #1 for persistent, scalable storage  
- **Redis** – sink #2 for fast, in-memory counters  
- **REST API** – sink #3 for external integrations

---

## 🛠 Setup Instructions

### 1. Install Debezium PostgreSQL Kafka Connector

```bash
cd ~
wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.5.0.Final/debezium-connector-postgres-2.5.0.Final-plugin.tar.gz
mkdir -p ~/kafka/connectors
tar -xvzf debezium-connector-postgres-2.5.0.Final-plugin.tar.gz -C ~/kafka/connectors

sudo mkdir -p /opt/kafka/plugins/debezium-postgres
sudo cp ~/kafka/connectors/debezium-connector-postgres/*.jar /opt/kafka/plugins/debezium-postgres/
```

---

### 2. PostgreSQL Setup

Enable logical replication:

```bash
sudo nano /etc/postgresql/16/main/postgresql.conf
# Set:
wal_level = logical
sudo systemctl restart postgresql
```

Create database and tables:

```sql
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

CREATE PUBLICATION engagement_pub FOR TABLE engagement_events;
SELECT * FROM pg_create_logical_replication_slot('cdc_slot', 'pgoutput');
```

---

### 3. Start Kafka & Zookeeper

```bash
sudo /opt/kafka/bin/zookeeper-server-start.sh -daemon /opt/kafka/config/zookeeper.properties
sudo /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
```

Create the Kafka topic:

```bash
sudo /opt/kafka/bin/kafka-topics.sh --create   --topic engagement_project.public.engagement_events   --bootstrap-server localhost:9092   --partitions 1 --replication-factor 1
```

---

### 4. Start Kafka Connect & Register CDC Connector

```bash
sudo /opt/kafka/bin/connect-distributed.sh /opt/kafka/config/connect-distributed.properties
```

Register the Debezium connector:

```bash
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
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"
  }
}'
```

Check status:

```bash
curl http://localhost:8083/connectors/engagement-cdc/status
```

---

### 5. Cassandra Setup

Start Cassandra:

```bash
docker start cassandra-hadoop
docker exec -it cassandra-hadoop cqlsh
```

Create keyspace and table:

```sql
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
```

#### Why Cassandra?

I used **Apache Cassandra** as one of the sinks due to its **high write throughput** and **low latency**, which are ideal for real-time CDC workloads. Its distributed architecture provides horizontal scalability and fault tolerance, making it a great fit for streaming applications where performance and reliability are critical.

---

### 6. Redis Setup

```bash
sudo apt update
sudo apt install redis-server -y
redis-cli ping
```

---

### 7. REST API Sink

Install Flask:

```bash
pip install flask
```

Create `rest_api.py`:

```python
from flask import Flask, request
app = Flask(__name__)

@app.route('/api', methods=['POST'])
def receive():
    print(request.json)
    return '', 200

app.run(port=5000)
```

Run it:

```bash
python3 rest_api.py
```

---

### 8. Spark Streaming Job

Install dependencies:

```bash
pip install redis cassandra-driver requests
```

Create `spark_cdc_multi_sink.py` with logic to process Kafka messages and write to Cassandra, Redis, and REST API.

Run the job:

```bash
spark-submit --master local[*] spark_cdc_multi_sink.py
```

---

## ✅ Validation

- **Check Cassandra:**

```sql
SELECT * FROM engagement.engagement_events;
```

- **Check Redis:**

```bash
redis-cli ZREVRANGE engagement_10m 0 5 WITHSCORES
```

- **Test REST API:**

```bash
curl -X POST http://localhost:5000/api -H "Content-Type: application/json" -d '{"message": "hello"}'
```

---

## 🔮 Future Improvements

- ⏱ **Monitoring**: Integrate Prometheus + Grafana for observability and lag metrics  
- 📦 **DLQ Support**: Add a Dead Letter Queue for error-handling  
- 🧪 **Testing**: Add unit & integration tests for Spark and REST API  
- ☁️ **Cloud Deployment**: Provide Docker Compose or Helm charts for production deployment

---
