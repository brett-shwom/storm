package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.spout.IBatchSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.task.TopologyContext;
import storm.trident.operation.TridentCollector;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import java.util.regex.Pattern;
import storm.trident.operation.BaseFunction;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Random;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;
// import com.hmsonline.storm.cassandra.StormCassandraConstants;
// import com.hmsonline.storm.cassandra.trident.CassandraMapState;
// import com.hmsonline.storm.cassandra.trident.CassandraMapState.Options;
import storm.trident.state.TransactionalValue;
import storm.trident.state.OpaqueValue;
import backtype.storm.LocalDRPC;

//import com.netflix.astyanax.AstyanaxContext;
import com.hmsonline.trident.cql.CassandraCqlMapState;
import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;

import java.io.Serializable;
import java.util.List;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.trident.cql.mappers.CqlRowMapper;

import storm.trident.operation.CombinerAggregator;



public class ImpressionTridentTopology {
    private static final Logger log = LoggerFactory.getLogger(ImpressionTridentTopology.class);
    private static String KEYSPACE = "test";
    private static String CASSANDRA_HOST = "localhost";

    public static void main(String[] args) throws Exception {

        Config config = new Config();
        config.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, CASSANDRA_HOST);
        config.setMaxSpoutPending(25);
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("impressionCounting", config, getTopology());
        Thread.sleep(10000);
        cluster.killTopology("impressionCounting");
        cluster.shutdown();
    }

    public static StormTopology getTopology() throws IOException {

        TridentTopology topology = new TridentTopology();

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("cid"), 3,
               new Values("1"),
               new Values("2"),
               new Values("3"),
               new Values("4"),
               new Values("5")
        );
        spout.setCycle(true);




        // Options options = new Options<OpaqueValue>();
        // options.columnFamily = "opaque";
        // StateFactory cassandraStateFactory = CassandraMapState.opaque(options);  

        
        //LocalDRPC client = new LocalDRPC();

        //TridentState impressionCounts = 
        topology
                .newStream("impressions", spout)
                .groupBy(new Fields("cid"))
                // .each(new Fields("cid", "count"), new Print(), new Fields("cid", "count"))
                //.persistentAggregate(cassandraStateFactory, new Count(), new Fields("count"))
                .persistentAggregate(CassandraCqlMapState.nonTransactional(new ImpressionCountMapper()),new LongCount(), new Fields("count") )
                .parallelismHint(1)
                .newValuesStream() //beware of putting stuff between this statement and the next: http://grokbase.com/t/gg/storm-user/132pg1fjnh/problem-chaining-trident-state-updaters
                .each(new Fields("cid", "count"), new Print())

        ;

        // topology.newDRPCStream("impressionCountsPerCid")
        //         .each(new Fields("args"), new Split)

        // You can prune unnecessary fields using "project"
        // topology
        //         .newStream("projection", spout)
        //         .each(new Fields("text"), new ToUpperCase(), new Fields("uppercased_text"))
        //         .project(new Fields("uppercased_text"))
        //         .each(new Fields("uppercased_text"), new Print());

        // Stream can be parallelized with "parallelismHint"
        // Parallelism hint is applied downwards until a partitioning operation (we will see this later).
        // This topology creates 5 spouts and 5 bolts:
        // Let's debug that with TridentOperationContext.partitionIndex !
        // topology
        //         .newStream("parallel", spout)
        //         .each(new Fields("actor"), new RegexFilter("pere"))
        //         .parallelismHint(5)
        //         .each(new Fields("text", "actor"), new Print());

        // You can perform aggregations by grouping the stream and then applying an aggregation
        // Note how each actor appears more than once. We are aggregating inside small batches (aka micro batches)
        // This is useful for pre-processing before storing the result to databases
        // topology
        //         .newStream("aggregation", spout)
        //         .groupBy(new Fields("actor"))
        //         .aggregate(new Count(),new Fields("count"))
        //         .each(new Fields("actor", "count"),new Print())
        // ;

        // In order ot aggregate across batches, we need persistentAggregate.
        // This example is incrementing a count in the DB, using the result of these micro batch aggregations
        // (here we are simply using a hash map for the "database")
        // topology
        //         .newStream("aggregation", spout)
        //         .groupBy(new Fields("actor"))
        //         .persistentAggregate(new MemoryMapState.Factory(),new Count(),new Fields("count"))
        // ;

        return topology.build();
    }

}


/**
 * @author Enno Shioji (enno.shioji@peerindex.com)
 */
class Print extends BaseFilter {
    private int partitionIndex;
    private int numPartitions;
    private final String name;

    public Print(){
        name = "";
    }
    public Print(String name){
        this.name = name;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.partitionIndex = context.getPartitionIndex();
        this.numPartitions = context.numPartitions();
    }


    @Override
    public boolean isKeep(TridentTuple tuple) {
        System.err.println(String.format("%s::Partition idx: %s out of %s partitions got %s", name, partitionIndex, numPartitions, tuple.toString()));
        return true;
    }
}



//https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/test/java/com/hmsonline/trident/cql/example/wordcount/WordCountAndSourceMapper.java
class ImpressionCountMapper implements CqlRowMapper<List<String>, Number>, Serializable {
    private static final long serialVersionUID = 1L;
    //private static final Logger LOG = LoggerFactory.getLogger(WordCountAndSourceMapper.class);

    public static final String KEYSPACE_NAME = "mykeyspace";
    public static final String TABLE_NAME = "impressioncounttable";
    public static final String CID_KEY_NAME = "cid";
    public static final String VALUE_NAME = "count";

    @Override
    public Statement map(List<String> keys, Number value) {
        Insert statement = QueryBuilder.insertInto(KEYSPACE_NAME, TABLE_NAME);
        statement.value(CID_KEY_NAME, keys.get(0));
        statement.value(VALUE_NAME, value);
        return statement;
    }

    @Override
    public Statement retrieve(List<String> keys) {
        // Retrieve all the columns associated with the keys
        Select statement = QueryBuilder
                .select()
                .column(CID_KEY_NAME)
                .column(VALUE_NAME)
                .from(KEYSPACE_NAME, TABLE_NAME);
        statement.where(QueryBuilder.eq(CID_KEY_NAME, keys.get(0)));
        return statement;
    }

    @Override
    public Number getValue(Row row) {
        return (Number) row.getInt(VALUE_NAME);
    }

    @Override
    public Statement map(TridentTuple tuple) {
        return null;
    }


}





class LongCount implements CombinerAggregator<Long> {
    private static final long serialVersionUID = 1L;

    @Override
    public Long init(TridentTuple tuple) {
        return 1L;
    }

    @Override
    public Long combine(Long val1, Long val2) {
        return val1 + val2;
    }

    @Override
    public Long zero() {
        return 0L;
    }
}

