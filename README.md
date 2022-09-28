<div>
<img class='banner' src="https://img.rawpixel.com/private/static/images/website/2022-05/pdmonet-waterlilyrob-job582-2.jpg?w=800&dpr=1&fit=default&crop=default&q=65&vib=3&con=3&usm=15&bg=F4F4F3&s=b6b1c2f88fc8b1cf87439ffa079d030a" style=" aspect-ratio: 1:3; border: solid 2.5em antiquewhite;"></img>
<hr>
</div>

#### DEMO STORYBOARD

# WHAT IS THE USE CASE

Marketing team is getting very low LTR scores and beleives its primarily due to certain trends which should be evident from all the ratings and feedback we are getting but that data is currently sitting in an sql database in the marketing department.

# THE SETUP

We have a marketing database which hosts all the ratings. It can be seen here.

```bash
docker exec -it ksqldb ksql "http://localhost:8088"

show topics;

describe ratings;

print ratings;
```

Lets sink this data into elastic search so we can have a look at it in Kibana.

```sql
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

Let's create a _stream_ from this data set and deserialize the value part of the message (message has a key:pair) into AVRO

```sql
CREATE STREAM RATINGS WITH (KAFKA_TOPIC='ratings',VALUE_FORMAT='AVRO');

describe ratings;
```

Let's clean up this data as there's some sources which are marked as test. But instead of it being a SELECT statement, lets just create a new topic (ie. stream) which specifically has this dataset. Its cleaner and has higher likelihood of being useful to an ai worker.

```sql
-- select users
SELECT USER_ID, STARS, CHANNEL, MESSAGE FROM RATINGS EMIT CHANGES;

-- remove test users
SELECT USER_ID, STARS, CHANNEL, MESSAGE FROM RATINGS WHERE LCASE(CHANNEL) NOT LIKE '%test%' EMIT CHANGES;
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

We want to eventually join this data with ratings. So let's create a table from a select query just like we did last time.

```sql
CREATE TABLE CUSTOMERS (CUSTOMER_ID VARCHAR PRIMARY KEY)
  WITH (KAFKA_TOPIC='asgard.demo.CUSTOMERS', VALUE_FORMAT='AVRO');

-- query the table.
SET 'auto.offset.reset' = 'earliest';
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CLUB_STATUS FROM CUSTOMERS EMIT CHANGES LIMIT 5;

```

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

So for e.g. I can tell you in real time what a customer's rating is

```sql
SELECT TIMESTAMPTOSTRING(ROWTIME, 'HH:mm:ss') AS EVENT_TS,
        FULL_NAME, CLUB_STATUS, STARS, MESSAGE, CHANNEL
  FROM RATINGS_WITH_CUSTOMER_DATA
  WHERE CAST(CUSTOMER_ID AS INT)=2
  EMIT CHANGES;
```

Now just to prove that this is real time. Let's update this customer's name and see what happens.

```sql
UPDATE CUSTOMERS SET CLUB_STATUS = 'bronze' WHERE ID=2;
```

Observe in the continuous ksqlDB query that the customer name has now changed.

Now let's create a stream of unhappy VIP's.

```sql
CREATE STREAM UNHAPPY_PLATINUM_CUSTOMERS AS
SELECT FULL_NAME, CLUB_STATUS, EMAIL, STARS, MESSAGE
FROM   RATINGS_WITH_CUSTOMER_DATA
WHERE  STARS < 3
  AND  CLUB_STATUS = 'platinum'
PARTITION BY FULL_NAME;
```

lets _sink_ this to elastic search so we can have a better look at it in kibana.

```sql
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

Lastly, let's build an aggregated _topic_ for my data-scientist pal over in marketing campaigns for research purposes - data has bidirectional benefits.

```sql
CREATE TABLE RATINGS_PER_CUSTOMER_PER_15MINUTE AS
SELECT FULL_NAME,COUNT(*) AS RATINGS_COUNT, COLLECT_LIST(STARS) AS RATINGS
  FROM RATINGS_WITH_CUSTOMER_DATA
        WINDOW TUMBLING (SIZE 15 MINUTE)
  GROUP BY FULL_NAME
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
