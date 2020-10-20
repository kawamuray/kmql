kmql
====

Kafka Management with sQL

`kmql` is a command that allows you to query [Apache Kafka](https://kafka.apache.org/) cluster's metadata.

## The problem

Kafka provides a bunch of commands to inspect a cluster's state such as `kafka-topics.sh`, `kafka-configs.sh`, `kafka-acls.sh`, and more. These scripts is a sufficient option when a human operator inspects relatively simpler information.
However when we want to 1. parse result output by a program or 2. query relatively complex information as some are shown in [Examples](https://github.com/kawamuray/kmql#query-examples), it becomes a mess of scripts that deals with non-machine friendly output formats and involving to execute multiple commands aggregating their results.
Alternatively, you may use `AdminClient` but that always involves writing a Java program and compile it which is not swift enough to operate clusters reliably and efficiently.

# Install

1. Download and unzip latest binary from [Releases](https://github.com/kawamuray/kmql/releases)
2. Copy `kmql-$VERSION` to the place where you like

# Usage

Starting interactive console:
```sh
kmql --bootstrap-servers="YOUR CLUSTER's bootstrap.servers"
query> SELECT * FROM replicas LIMIT 3
╔═══════════╤═══════════╤═══════════╤═══════════╤═════════════════════╤════════════╗
║ TOPIC     │ PARTITION │ BROKER_ID │ IS_LEADER │ IS_PREFERRED_LEADER │ IS_IN_SYNC ║
╠═══════════╪═══════════╪═══════════╪═══════════╪═════════════════════╪════════════╣
║ topic-xyz │ 0         │ 1         │ true      │ true                │ true       ║
╟───────────┼───────────┼───────────┼───────────┼─────────────────────┼────────────╢
║ topic-xyz │ 0         │ 3         │ false     │ false               │ true       ║
╟───────────┼───────────┼───────────┼───────────┼─────────────────────┼────────────╢
║ topic-xyz │ 0         │ 2         │ false     │ false               │ true       ║
╚═══════════╧═══════════╧═══════════╧═══════════╧═════════════════════╧════════════╝
```

Execute single query and print output:
```sh
kmql --bootstrap-servers="YOUR CLUSTER's bootstrap.servers" -e "SELECT * FROM replicas LIMIT 3"
(same as the above)
```

Execute single query and get output as JSON:
```sh
kmql --bootstrap-servers="YOUR CLUSTER's bootstrap.servers" -e "SELECT * FROM replicas LIMIT 3" --format=json | jq .
[
  {
    "TOPIC": "topic-xyz",
    "PARTITION": 0,
    "BROKER_ID": 1,
    "IS_LEADER": true,
    "IS_PREFERRED_LEADER": true,
    "IS_IN_SYNC": true
  },
...
]
```

Execute single query and get output as SSV (space-separated values):
```sh
kmql --bootstrap-servers="YOUR CLUSTER's bootstrap.servers" -e "SELECT * FROM replicas LIMIT 3" --format=ssv
# TOPIC PARTITION BROKER_ID IS_LEADER IS_PREFERRED_LEADER IS_IN_SYNC
topic-xyz 0 1 true true true
topic-xyz 0 3 false false true
topic-xyz 0 2 false false true
```

To see all available tables and their schema:
```sh
kmql --bootstrap-servers="YOUR CLUSTER's bootstrap.servers" --init-all
query> SHOW TABLES;
query> SHOW COLUMNS FROM table_name;
```

# Query Examples

```sh
# Dump replica info for the specific topic
SELECT * FROM replicas WHERE topic = 'topic-name'

# Topics which has more than 100 partitions, sorted in descending order by number of partitions
SELECT topic, COUNT(DISTINCT partition) AS partitions FROM replicas GROUP BY topic HAVING partitions > 100 ORDER BY partitions DESC

# Topic/partitions which its leader is assigned to broker 1
SELECT topic, partition FROM replicas WHERE broker_id=1 AND is_leader

# Topic partitions that has an out-of-sync replica
SELECT DISTINCT topic, partition FROM replicas WHERE NOT is_in_sync

# Replicas that are on non-preferred leader broker
SELECT * FROM replicas WHERE is_leader AND NOT is_preferred_leader

# Topics that are configured to enable message down conversion
SELECT name FROM configs WHERE resource_type = 'topic' AND key = 'message.downconversion.enable' AND value = 'true'

# List broker hostnames that are consuming their log directories over 10TB
SELECT host FROM brokers RIGHT JOIN logdirs ON id = broker_id GROUP BY (id) HAVING SUM(size) > 10000000000000

# Partitions that has ISRs below the min.insync.replicas
SELECT topic, partition, isr, value AS minISR
FROM configs
RIGHT JOIN (
  SELECT topic, partition, COUNT(*) as isr
  FROM replicas
  WHERE is_in_sync
  GROUP BY (topic, partition)
) ON name = topic
WHERE key = 'min.insync.replicas' AND isr < CAST(value AS INT)
```

# How it works

It obtains information from Kafka cluster using `AdminClient` and feeds it into [H2](https://www.h2database.com) in-memory database.
This stupid approach works very well for:

* providing fully SQL compliant query support
* caching obtained metadata for arbitrary duration

The load time to initialize database is XX seconds even for large cluster that hosts over x partitions.

# License

Apache License Version 2.0.
See [LICENSE](LICENSE) for more detail.
