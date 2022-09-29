![](https://i.imgur.com/j0DltNW.gif)

# **INNOVATION AND PROBLEM SOLVING WITH DATA.**

> Storyboard

#### 1. Describe the core theme and use case

#### 2. Describe desired outcomes

#### 3. Framework

#### 4. Solution (demo).

---

</br>

# WHAT IS THE USE CASE

> ### Core Theme
>
> Improve customer experience and retention.

> ### Specifics

1. Marketing team has measured a decline of LTR (likelihood to recommend) score YOY .
2. Ratings and feedback are bad. Alarming trends around new user acq & low retention.
3. Customer loyalty programs have not helped.
4. Data for ratings and feedback is available but has 2 limitations.

- - Exists in several places (marketing, sales) with ambiguous quality.
- - Analysis is slow (or not possible due to volume/infra) & has low shelf life.

5. Customer information is available on-prem (SQL/Oracle database).

> ### How do we win? (Describe the outcomes)

1. Root cause / trend analysis. How do we wi> n?
2. Use data to drive more real-time decisions to retain customers.
3. Solution which can scale to similar/other challenges (roadmap).

# THE SETUP

> ### Containers

- Confluent KAFKA Single Broker Cluster
- Confluent KAFKA Connect
- KSQLDB
- Zookeeper
- Confluent Schema Registry
- Elastic Search
- Kibana
- MSSQL (or MySQL) Marketing database
- MSSQL server Finance Database

All Containers are ran as part of a single _docker-compose.yml_ file and has been tested on ECS and Azure Event hubs.

# DEMO

We have a marketing database which hosts all the ratings. Its a simulated stream of events purporting to show the ratings left by users on a website, with data elements including the device type that they used, the star rating, and a message associated with the rating.

> Note that we don't need to know the format of the data; KsqlDB introspects the data and understands how to deserialise it.

```bash
docker compose up -d
# for reruns, docker compose up -d --build --force-recreate

docker exec -it ksqldb ksql "http://localhost:8088"

show topics;

print 'ratings';
```

Lets sink this data into elastic search so we can have a look at it in Kibana.

```sql
-- Create sink connector
CREATE SINK CONNECTOR SINK_ES_RATINGS WITH (
    'connector.class' = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
    'topics'          = 'ratings',
    'connection.url'  = 'http://elasticsearch:9200',
    'type.name'       = '_doc',
    'key.ignore'      = 'false',
    'schema.ignore'   = 'true',
    'transforms'= 'ExtractTimestamp',
    'transforms.ExtractTimestamp.type'= 'org.apache.kafka.connect.transforms.InsertField$Value',
    'transforms.ExtractTimestamp.timestamp.field' = 'RATING_TS'
);
```

> ## 2.1 Creating Streams

Now let's _register_ this topic. Registering it will automatically create a schema for this topic in AVRO . Note that I didnt have to specify what column names etc. That’s because the data is in Avro format, and the Confluent Schema Registry supplies the actual schema details.
Let's create a _stream_ from this data set and deserialize the value part of the message (message has a key:pair) into AVRO. You can use DESCRIBE to examine an object’s columns.

```sql
CREATE STREAM RATINGS WITH (KAFKA_TOPIC='ratings',VALUE_FORMAT='AVRO');

describe ratings;
```

Let's clean up this data as there's some sources which are marked as test. But instead of it being a SELECT statement, lets just create a new topic (ie. stream) which specifically has this dataset. Its cleaner and has higher likelihood of being useful to an ai worker.

> ## 2.2 Continuous Query

```sql
-- select users
SELECT USER_ID, STARS, CHANNEL, MESSAGE FROM RATINGS EMIT CHANGES;

-- remove test users
SELECT USER_ID, STARS, CHANNEL, MESSAGE FROM RATINGS WHERE LCASE(CHANNEL) NOT LIKE '%test%' EMIT CHANGES;
```

You’ll notice that the data keeps on coming. That is because ksqlDB is fundamentally a streaming engine, and the queries that you run are continuous queries. We specified EMIT CHANGES which tells ksqlDB to output the changes to the stream, which is everytime a new event arrives.

> ## 2.3 Filtering data

```sql
-- Create stream of live users
CREATE STREAM RATINGS_LIVE AS
SELECT * FROM RATINGS WHERE LCASE(CHANNEL) NOT LIKE '%test%' EMIT CHANGES;
-- and test users
CREATE STREAM RATINGS_TEST AS
SELECT * FROM RATINGS WHERE LCASE(CHANNEL) LIKE '%test%' EMIT CHANGES;
-- Test the streams
SELECT * FROM RATINGS_LIVE EMIT CHANGES LIMIT 5;
SELECT * FROM RATINGS_TEST EMIT CHANGES LIMIT 5;
-- extended description
DESCRIBE EXTENDED RATINGS_LIVE
```

> ## 2.4 KAFKA connect - Connecting to a DB

Now lets ingest the customer table we have available from our internal systems of record. This could be an Oracle SQL server, of MS SQL Server - I'm using a mysql server here for simplicity sake but KAFKA can literllay connect to almost any CDC type.

But before we source this data, let's check it out.

```sh
docker exec -it mysql bash -c 'mysql -u $MYSQL_USER -p$MYSQL_PASSWORD demo'

```

```sql
SHOW TABLES;

SELECT ID, FIRST_NAME, LAST_NAME, EMAIL, CLUB_STATUS FROM CUSTOMERS LIMIT 5;
```

As you can see we have a customer id field which we can use to enrich the ratings data along with some form of club status which looks interesting.

Let's source this data as CDC into a KAFKA topic.

```sh
CREATE SOURCE CONNECTOR SOURCE_MYSQL_01 WITH (
    'connector.class' = 'io.debezium.connector.mysql.MySqlConnector',
    'database.hostname' = 'mysql',
    'database.port' = '3306',
    'database.user' = 'debezium',
    'database.password' = 'dbz',
    'database.server.id' = '42',
    'database.server.name' = 'asgard',
    'table.whitelist' = 'demo.customers',
    'database.history.kafka.bootstrap.servers' = 'kafka:29092',
    'database.history.kafka.topic' = 'dbhistory.demo' ,
    'include.schema.changes' = 'false',
    'transforms'= 'unwrap,extractkey',
    'transforms.unwrap.type'= 'io.debezium.transforms.ExtractNewRecordState',
    'transforms.extractkey.type'= 'org.apache.kafka.connect.transforms.ExtractField$Key',
    'transforms.extractkey.field'= 'id',
    'key.converter'= 'org.apache.kafka.connect.storage.StringConverter',
    'value.converter'= 'io.confluent.connect.avro.AvroConverter',
    'value.converter.schema.registry.url'= 'http://schema-registry:8081'
    );
```

Make sure all connectors are running

```sql
SHOW CONNECTORS;

SHOW TOPICS;

PRINT 'asgard.demo.CUSTOMERS' FROM BEGINNING;
```

> ## 2.5 Joining data in realtime.

Let’s use the customer data (CUSTOMERS) and use it to enrich the inbound stream of ratings data (RATINGS) to show against each rating who the customer is, and their club status ('platinum','gold', etc).

```sql
CREATE TABLE CUSTOMERS (CUSTOMER_ID VARCHAR PRIMARY KEY)
  WITH (KAFKA_TOPIC='asgard.demo.CUSTOMERS', VALUE_FORMAT='AVRO');

-- query the table.
SET 'auto.offset.reset' = 'earliest';
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CLUB_STATUS FROM CUSTOMERS EMIT CHANGES LIMIT 5;
```

Now just to prove that this is real time. Let's update this customer's name and see what happens.

```sql
INSERT INTO CUSTOMERS (ID,FIRST_NAME,LAST_NAME) VALUES (42,'Rick','Astley');
UPDATE CUSTOMERS SET EMAIL = 'rick@example.com' where ID=42;
UPDATE CUSTOMERS SET CLUB_STATUS = 'bronze' where ID=42;
UPDATE CUSTOMERS SET CLUB_STATUS = 'platinum' where ID=42;
```

Observe in the continuous ksqlDB query that the customer name has now changed.

> ## 2.6 Create enriched streams from joined data.
>
> Let’s persist this as an enriched stream, including a few more columns (including concatenating the two components of the name (FIRST_NAME and LAST_NAME)), by using CREATE STREAM … AS:

Now let's create a topic(stream), which outer joins ratings data with customer data using customer_id as primary key.

```sql
SET 'auto.offset.reset' = 'earliest';
CREATE STREAM RATINGS_WITH_CUSTOMER_DATA
       WITH (KAFKA_TOPIC='ratings-enriched')
       AS
SELECT R.RATING_ID, R.MESSAGE, R.STARS, R.CHANNEL,
       C.CUSTOMER_ID, C.FIRST_NAME + ' ' + C.LAST_NAME AS FULL_NAME,
       C.CLUB_STATUS, C.EMAIL
FROM   RATINGS_LIVE R
       LEFT JOIN CUSTOMERS C
         ON CAST(R.USER_ID AS STRING) = C.CUSTOMER_ID
WHERE  C.FIRST_NAME IS NOT NULL
EMIT CHANGES;
```

Now, let's take a moment to appreciate what's happening here. We've just associated context to just a pile of data. What does that mean in practise.

So for e.g. Check out the ratings for customer id 2 only from the new stream that we’ve created - note the CLUB_STATUS is platinum:

```sql
SELECT TIMESTAMPTOSTRING(ROWTIME, 'HH:mm:ss') AS EVENT_TS,
        FULL_NAME, CLUB_STATUS, STARS, MESSAGE, CHANNEL
  FROM RATINGS_WITH_CUSTOMER_DATA
  WHERE CAST(CUSTOMER_ID AS INT)=2
  EMIT CHANGES;
```

> ## 2.7 Unhappy VIPs
>
> Having enriched the initial stream of ratings events with customer data, we can now persist a filtered version of that stream that includes a predicate to identify just those VIP customers who have left bad reviews:

```sql
-- reset
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM UNHAPPY_PLATINUM_CUSTOMERS AS
SELECT FULL_NAME, CLUB_STATUS, EMAIL, STARS, MESSAGE
FROM   RATINGS_WITH_CUSTOMER_DATA
WHERE  STARS < 3
  AND  CLUB_STATUS = 'platinum'
PARTITION BY FULL_NAME;
```

Now we can query the derived stream to easily identify important customers who are not happy.

```sql
SELECT STARS, MESSAGE, EMAIL FROM UNHAPPY_PLATINUM_CUSTOMERS EMIT CHANGES;
```

> ## 2.8 Visualize unhappy customers
>
> Since this is backed by a Kafka topic being continually popuated by ksqlDB we can also drive other applications with this data, as well as land it to datastores down-stream for visualisation. lets _sink_ this to elastic search/Kibana for rapid visualisation and analysis.

```sql
-- reset
SET 'auto.offset.reset' = 'earliest';

CREATE SINK CONNECTOR SINK_ELASTIC_01 WITH (
  'connector.class' = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
  'connection.url' = 'http://elasticsearch:9200',
  'type.name' = '',
  'behavior.on.malformed.documents' = 'warn',
  'errors.tolerance' = 'all',
  'errors.log.enable' = 'true',
  'errors.log.include.messages' = 'true',
  'topics' = 'ratings-enriched,UNHAPPY_PLATINUM_CUSTOMERS',
  'key.ignore' = 'true',
  'schema.ignore' = 'true',
  'key.converter' = 'org.apache.kafka.connect.storage.StringConverter',
  'transforms'= 'ExtractTimestamp',
  'transforms.ExtractTimestamp.type'= 'org.apache.kafka.connect.transforms.InsertField$Value',
  'transforms.ExtractTimestamp.timestamp.field' = 'EXTRACT_TS'
);
```

Let's make sure everything ran.

```sql
--recheck connectors
SHOW CONNECTORS;
```

Check data in elastic search

```sh
docker exec elasticsearch curl -s "http://localhost:9200/_cat/indices/*?h=idx,docsCount"
```

> ## 2.9 Launch Kibana

```
...url/app/kibana#/dashboard/mysql-ksql-kafka-es

Navigate in Kibana > Dashboards > Ratings

```

Note that this is being fed with live events—if you click the Refresh button you’ll see it updates with up-to-the-second data. By default the dashboard shows the last 15 minutes of events.

> ## 2.10 Bonus! (Little something for the Data scientists)
>
> Lastly, let's build an aggregated _topic_ for my data-scientist as with this setup we can create aggregates over all events to date or based on a time window.

Supported types are:

- Tumbling (e.g. every 5 minutes : 00:00, 00:05, 00:10)
- Hopping (e.g. every 5 minutes, advancing 1 minute: 00:00-00:05, 00:01-00:06)
- Session (Sets a timeout for the given key, after which any new data is treated as a new session)

```sql

-- reset
SET 'auto.offset.reset' = 'earliest';

-- check the query
SELECT TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss') AS WINDOW_START_TS,
            CLUB_STATUS, COUNT(*) AS RATING_COUNT
        FROM RATINGS_WITH_CUSTOMER_DATA
            WINDOW TUMBLING (SIZE 1 MINUTES)
        GROUP BY CLUB_STATUS
        EMIT CHANGES;


CREATE TABLE RATINGS_PER_CUSTOMER_PER_15MINUTE AS
SELECT FULL_NAME,COUNT(*) AS RATINGS_COUNT, COLLECT_LIST(STARS) AS RATINGS
FROM RATINGS_WITH_CUSTOMER_DATA
  WINDOW TUMBLING (SIZE 15 MINUTE)
  GROUP BY FULL_NAME
  EMIT CHANGES;

CREATE TABLE RATINGS_BY_CLUB_STATUS AS
SELECT CLUB_STATUS, COUNT(*) AS RATING_COUNT
FROM RATINGS_WITH_CUSTOMER_DATA
  WINDOW TUMBLING (SIZE 1 MINUTES)
  GROUP BY CLUB_STATUS
  EMIT CHANGES;
```

And, let's _sink_ this into a mongo database.

```json
{
  "name": "mongo-analytics-sink-00",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "connection.uri": "mongodb://mongo:mongo@mongo:27017/",
    "database": "analytics",
    "collection": "ratings",
    "topics": "UNHAPPY_PLATINUM_CUSTOMERS",
    "change.data.capture.handler": "com.mongodb.kafka.connect.sink.cdc.mongodb.ChangeStreamHandler"
  }
}
```

![](https://i.imgur.com/YbGoCEq.gif)
