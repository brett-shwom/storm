A storm topology AND a trident topology, both of which count impressions grouped by campaign id

Storm Topology
==============

Inspired by: <https://github.com/apache/storm/tree/master/examples/storm-starter>

To run: `mvn compile exec:java -Dstorm.topology=storm.starter.ImpressionStormTopology`

Trident Topology
==============

Run `CREATE KEYSPACE test WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' } ;`

Why Trident? Trident provides exactly-once semantics. Storm itself does not provide exactly-once semantics.

Inspired by: <https://github.com/eshioji/trident-tutorial>

To run: ``

See also: 
- <https://github.com/hmsonline/storm-cassandra/issues/54#issuecomment-84002523>
- <https://storm.apache.org/documentation/Trident-tutorial.html>
- <https://storm.apache.org/documentation/Trident-state.html>
- <http://grokbase.com/t/gg/storm-user/132pg1fjnh/problem-chaining-trident-state-updaters>
- <https://github.com/hmsonline/storm-cassandra-cql/issues/23>
- <https://svendvanderveken.wordpress.com/2013/07/30/scalable-real-time-state-update-with-storm/>
- <https://github.com/svendx4f/stormRoomOccupancy/tree/v1.0.1>
- <https://storm.apache.org/documentation/Trident-API-Overview.html>
- <https://blog.twitter.com/2012/trident-a-high-level-abstraction-for-realtime-computation>
- <https://github.com/hmsonline/storm-cassandra/blob/master/src/test/java/com/hmsonline/storm/cassandra/bolt/CassandraMapStateTest.java>
- <https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/trident/TridentWordCount.java#L41>