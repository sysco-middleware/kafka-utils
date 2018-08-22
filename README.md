# Kafka Utilities

Utility libraries around Kafka.

## Graphviz for Kafka Streams

To increase understanding about Kafka Streams 
topology, this utility print it on [Graphviz](https://www.graphviz.org/)
format, describing how processors are communicating between
each other, how topics are connected as inputs and outputs, 
and how stores are used.

### How to use it

Add dependency:

```xml
<dependency>
  <groupId>no.sysco.middleware.kafka</groupId>
  <artifactId>kafka-util-streams-graphviz</artifactId>
  <version>0.1.0</version>
</dependency>
```
