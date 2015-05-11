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
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.trident.CassandraMapState;
import com.hmsonline.storm.cassandra.trident.CassandraMapState.Options;
import storm.trident.state.TransactionalValue;
import storm.trident.state.OpaqueValue;
import backtype.storm.LocalDRPC;

import com.netflix.astyanax.AstyanaxContext;



public class ImpressionTridentTopology {
    private static final Logger log = LoggerFactory.getLogger(ImpressionTridentTopology.class);
    private static String KEYSPACE = "test";
    private static String CASSANDRA_HOST_AND_PORT = "localhost:9160";

    // public static void setupCassandra() throws Exception {
    //     try {

    //         AstyanaxContext<Cluster> clusterContext = newClusterContext(CASSANDRA_HOST_AND_PORT);

    //         //createColumnFamily(clusterContext, KEYSPACE, "transactional", "UTF8Type", "UTF8Type", "UTF8Type");
    //         //createColumnFamily(clusterContext, KEYSPACE, "nontransactional", "UTF8Type", "UTF8Type", "UTF8Type");
    //         createColumnFamily(clusterContext, KEYSPACE, "opaque", "UTF8Type", "UTF8Type", "UTF8Type");

    //     } catch (Exception e) {
    //         LOG.warn("Couldn't setup cassandra.", e);
    //         throw e;
    //     }
    // }

    public static void main(String[] args) throws Exception {


        HashMap<String, Object> clientConfig = new HashMap<String, Object>();
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, CASSANDRA_HOST_AND_PORT);
        clientConfig.put(StormCassandraConstants.CASSANDRA_STATE_KEYSPACE, KEYSPACE);
        Config config = new Config();
        config.setMaxSpoutPending(25);
        config.put("cassandra.config", clientConfig);
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




        Options options = new Options<OpaqueValue>();
        options.columnFamily = "opaque";
        StateFactory cassandraStateFactory = CassandraMapState.opaque(options);  

        
        //LocalDRPC client = new LocalDRPC();

        //TridentState impressionCounts = 
        topology
                .newStream("impressions", spout)
                .groupBy(new Fields("cid"))
                // .each(new Fields("cid", "count"), new Print(), new Fields("cid", "count"))
                .persistentAggregate(cassandraStateFactory, new Count(), new Fields("count"))
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

